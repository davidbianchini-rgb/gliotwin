#!/usr/bin/env python3
"""
Importa i dataset LUMIERE, MU-Glioma-Post e UCSD-PTGBM in GlioTwin.

Utilizzo:
  python pipelines/run_import.py                     # tutti i dataset
  python pipelines/run_import.py lumiere             # solo LUMIERE
  python pipelines/run_import.py mu ucsd             # MU + UCSD
  python pipelines/run_import.py --reset-db all      # ricrea DB e importa tutto
  python pipelines/run_import.py --verbose lumiere   # output dettagliato

L'import è IDEMPOTENTE: rieseguirlo non crea duplicati.
I file .partial o incompleti vengono saltati automaticamente.
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path

# Aggiunge la root del progetto al path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import DB_PATH, init_db, get_conn
from pipelines import import_lumiere, import_mu, import_ucsd

RUNNERS = {
    'lumiere': import_lumiere.run,
    'mu':      import_mu.run,
    'ucsd':    import_ucsd.run,
}

DB_DATASET_NAMES = {
    'lumiere': 'lumiere',
    'mu': 'mu_glioma_post',
    'ucsd': 'ucsd_ptgbm',
}


def _counts(conn) -> dict:
    tables = ['subjects', 'sessions', 'sequences',
              'computed_structures', 'radiological_structures', 'clinical_events']
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}


def _counts_by_dataset(conn) -> list[tuple[str, int, int, int]]:
    rows = conn.execute(
        """
        SELECT
            s.dataset,
            COUNT(DISTINCT s.id) AS n_subjects,
            COUNT(DISTINCT ses.id) AS n_sessions,
            COUNT(DISTINCT seq.id) AS n_sequences
        FROM subjects s
        LEFT JOIN sessions ses ON ses.subject_id = s.id
        LEFT JOIN sequences seq ON seq.session_id = ses.id
        GROUP BY s.dataset
        ORDER BY s.dataset
        """
    ).fetchall()
    return [(row[0], row[1], row[2], row[3]) for row in rows]


def _purge_datasets(conn, datasets: list[str]) -> None:
    if not datasets:
        return
    db_names = [DB_DATASET_NAMES[d] for d in datasets]
    placeholders = ','.join('?' for _ in db_names)
    conn.execute(f"DELETE FROM subjects WHERE dataset IN ({placeholders})", db_names)
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'datasets', nargs='*',
        default=['lumiere', 'mu', 'ucsd'],
        help="Dataset da importare: lumiere | mu | ucsd (default: tutti)"
    )
    parser.add_argument(
        '--reset-db', action='store_true',
        help="Cancella e ricrea il database prima di importare (dati esistenti persi)"
    )
    parser.add_argument(
        '--purge-selected', action='store_true',
        help="Cancella dal DB solo i dataset selezionati prima di reimportarli"
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help="Importa solo i primi N soggetti del dataset selezionato"
    )
    parser.add_argument(
        '--subject', action='append', default=[],
        help="Importa solo uno specifico subject_id/patient_id (ripetibile)"
    )
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Stampa dettagli sui file saltati")
    args = parser.parse_args()

    # Normalizza 'all' → tutti e tre
    datasets = []
    for d in args.datasets:
        if d == 'all':
            datasets = list(RUNNERS.keys())
            break
        if d not in RUNNERS:
            parser.error(f"Dataset sconosciuto: {d!r}. Scegli tra: {list(RUNNERS)}")
        datasets.append(d)

    # ── Database setup ───────────────────────────────────────────────────
    if args.reset_db:
        if DB_PATH.exists():
            DB_PATH.unlink()
            print(f"[reset] DB eliminato: {DB_PATH}")
        init_db()
        print(f"[init]  DB ricreato con schema aggiornato.")
    elif not DB_PATH.exists():
        init_db()
        print(f"[init]  DB creato: {DB_PATH}")
    elif args.purge_selected:
        with get_conn() as conn:
            _purge_datasets(conn, datasets)
        print(f"[purge] Dataset rimossi dal DB: {', '.join(datasets)}")

    with get_conn() as conn:
        before = _counts(conn)

    # ── Import ───────────────────────────────────────────────────────────
    for ds in datasets:
        print(f"\n{'='*55}")
        print(f"  Importo: {ds.upper()}")
        print('='*55)
        try:
            with get_conn() as conn:
                RUNNERS[ds](
                    conn,
                    verbose=args.verbose,
                    limit=args.limit,
                    subjects=set(args.subject) if args.subject else None,
                )
        except Exception as exc:
            print(f"  [ERRORE] {ds}: {exc}")
            if args.verbose:
                import traceback
                traceback.print_exc()

    # ── Riepilogo ────────────────────────────────────────────────────────
    with get_conn() as conn:
        after = _counts(conn)
        per_dataset = _counts_by_dataset(conn)

    print(f"\n{'='*55}")
    print("  RIEPILOGO IMPORT")
    print('='*55)
    labels = {
        'subjects':              'Soggetti',
        'sessions':              'Sessioni',
        'sequences':             'Sequenze MRI',
        'computed_structures':   'Strutture computed',
        'radiological_structures': 'Strutture radiologiche',
        'clinical_events':       'Clinical events',
    }
    for key, label in labels.items():
        tot = after[key]
        new = tot - before[key]
        sign = f'+{new}' if new >= 0 else str(new)
        print(f"  {label:<30} {tot:>6}  ({sign})")
    print("\n  PER DATASET")
    for dataset, n_subjects, n_sessions, n_sequences in per_dataset:
        print(
            f"  {dataset:<20} "
            f"subjects={n_subjects:<4} sessions={n_sessions:<4} sequences={n_sequences:<4}"
        )
    print()


if __name__ == '__main__':
    main()
