# GlioTwin — Panoramica del progetto

## Cos'è GlioTwin

GlioTwin è una piattaforma clinica per la gestione, il preprocessing e l'analisi longitudinale di immagini RM cerebrali di pazienti con glioblastoma. L'obiettivo è costruire un percorso dati riproducibile che vada dall'acquisizione DICOM grezza fino alle strutture tumorali segmentate e alle metriche quantitative, integrando in un unico sistema i dati di imaging, le strutture radiologiche e i dati clinici.

Il sistema è pensato per lavorare su più dataset contemporaneamente — sia dati interni IRST che dataset pubblici — mantenendo una rappresentazione comune che permetta confronti longitudinali per singolo paziente e analisi di coorte.


## Architettura generale

Il backend è scritto in Python con FastAPI e usa SQLite come database locale. Il frontend è una Single Page Application in JavaScript puro, organizzata in viste separate per ogni fase del flusso di lavoro. Le operazioni pesanti (preprocessing, segmentazione) vengono eseguite come processi separati avviati in background dal server, con logging e stato persistenti su DB.

I file di imaging non vengono mai copiati inutilmente: il DB conserva i percorsi (`raw_path`, `processed_path`) e le operazioni lavorano direttamente sui file originali o sui NIfTI prodotti.


## Modello dei dati

Il modello centrale ruota attorno a una gerarchia semplice:

**Soggetto → Sessione → Sequenze / Strutture**

Un *soggetto* rappresenta un paziente in un dato dataset. Una *sessione* corrisponde a un timepoint (un esame RM), identificato da una data e da una label come `timepoint_001`. Ogni sessione ha associato un insieme di *sequenze* (le singole serie RM: T1, T1ce, T2, FLAIR, APT, DWI, ecc.) e, dopo la segmentazione, un insieme di *strutture calcolate* (ET, TC, SNFH, RC) o *strutture importate* dai dataset pubblici.

A livello di soggetto vengono gestiti anche i dati clinici: eventi clinici (diagnosi, chemioterapia, radioterapia, progressione), dati di radioterapia importati da MOSAIQ e informazioni biologiche come IDH, MGMT, sopravvivenza.


## Dataset supportati

Il sistema gestisce dataset eterogenei con pipeline di import dedicate. Ogni dataset ha le proprie regole di lettura e di correzione delle anomalie.

- **IRST (interno):** dati DICOM acquisiti presso l'istituto, con dati RT da MOSAIQ.
- **MU-Glioma-Post:** dataset pubblico con MRI post-operatoria, strutture radiologiche e dati clinici in formato NIfTI.
- **LUMIERE:** dataset longitudinale con MRI multi-contrasto settimanali e segmentazioni HD-GLIO-AUTO.
- **UCSD-PTGBM, RHUH-GBM, QIN-GBM, GLIS-RT:** dataset pubblici con pipeline in sviluppo.


## Flusso dei dati — fase per fase

### Import

Il processo di import parte dalla scansione di una cartella DICOM. Il sistema legge le serie presenti, classifica ciascuna applicando le regole testuali definite in `series_rules.csv` (matching normalizzato sulla `SeriesDescription` DICOM), e raggruppa le serie per paziente e per esame. Ogni esame viene classificato come *ready* (tutte e quattro le sequenze core trovate univocamente), *review* (core trovate ma con candidati multipli per almeno una classe) o *incomplete* (almeno una core mancante).

L'operatore può quindi revisionare la classificazione, scegliere manualmente il candidato preferito per i casi ambigui, e confermare l'import. Il sistema scrive su DB i soggetti, le sessioni e le sequenze, salvando il percorso della cartella DICOM originale. I file non vengono copiati.

Per i dataset pubblici (MU, LUMIERE) la pipeline legge direttamente i NIfTI pre-esistenti e le strutture già disponibili, popolando direttamente anche i percorsi processati e le strutture radiologiche.

Per i dati di radioterapia IRST viene letto un file Excel esportato da MOSAIQ. Il sistema fa il match nominativo tra i pazienti del file RT e quelli già presenti nel DB DICOM, segnala i casi ambigui e importa i dati clinici solo per i match univoci.

### Preprocessing

Il preprocessing converte le serie DICOM in NIfTI in uno spazio canonico uniforme. Lo scopo è avere tutti i volumi allineati, con risoluzione standardizzata, pronti per la segmentazione e per il confronto quantitativo.

Il sistema gestisce due percorsi:

**Percorso FeTS** — quando sono disponibili tutte e quattro le sequenze core (T1 nativa, T1ce, T2, FLAIR). Le cartelle DICOM vengono copiate in uno staging area, viene eseguito lo script FeTS che internamente fa la conversione NIfTI, il brain extraction con HD-BET e l'allineamento nello spazio canonico FeTS (basato su T1ce). I file NIfTI risultanti vengono copiati nella directory finale e i percorsi aggiornati su DB.

**Percorso SimpleITK** — quando mancano T1 nativa e/o T2 (tipico per alcuni pazienti IRST con solo T1ce e FLAIR, o con sequenze funzionali). In questo caso T1ce viene convertita da DICOM a NIfTI con SimpleITK e usata come riferimento. Tutte le altre sequenze disponibili (FLAIR, DWI, APT, perfusione, ktrans) vengono convertite e ricampionate sulla griglia di T1ce con un resample identità (le serie sono già co-registrate nativamente dallo scanner). Non viene eseguito il brain extraction.

Lo stato di ogni step (validazione input, conversione NIfTI, brain extraction) è serializzato per ogni soggetto/sessione e consultabile in tempo reale nella vista Preprocessing.

I file NIfTI finali vengono salvati in:
`/mnt/dati/irst_data/irst_preprocessed_final/{subject_id}/{session_label}/`

### Segmentazione

La segmentazione viene eseguita su i NIfTI preprocessati e produce strutture tumorali. Il segmentatore attualmente integrato è **FeTS** (Federated Tumor Segmentation), che produce quattro regioni:

- **ET** — Enhancing Tumor (tumore captante)
- **TC** — Tumor Core (nucleo tumorale)
- **SNFH** — Signal Abnormality / edema peritumorale
- **RC** — Resection Cavity (cavità di resezione)

Le strutture prodotte vengono scritte nel DB in `computed_structures` con volume in ml, percorso della maschera NIfTI, spazio di riferimento e origine (engine + versione). Queste strutture coesistono con quelle importate dai dataset pubblici e con eventuali strutture manuali future, classificate per origine.

È prevista anche l'integrazione con **RH-GlioSeg** come engine alternativo.

### Analisi

L'area di analisi aggrega le informazioni prodotte nelle fasi precedenti per rispondere a domande cliniche e di ricerca.

A livello di singolo paziente è disponibile una **timeline clinica** che mostra in ordine cronologico gli esami RM con le relative strutture, gli eventi clinici (diagnosi, inizio chemio, RT, progressione), e i volumi tumorali per timepoint.

A livello di coorte la dashboard mostra distribuzioni per dataset (IDH, MGMT, sopravvivenza, numero di sessioni) e permette di filtrare i soggetti per caratteristiche biologiche o per stato nella pipeline.

Sono in sviluppo i filtri per sottopopolazione e l'analisi longitudinale sistematica dei volumi tumorali.

### Export

L'export finale converte le strutture calcolate in **DICOM RTSTRUCT** associato alla serie nativa del paziente. Il percorso prevede di applicare la trasformazione inversa dallo spazio canonico verso lo spazio DICOM originale (trasformazione rigida), e di generare il file RTSTRUCT con `pydicom`. Questa fase è attualmente in sviluppo e la reversibilità delle trasformazioni verso lo spazio nativo non è ancora verificata end-to-end.


## Stato operativo di una sessione

Ogni sessione ha uno stato calcolato a runtime che determina cosa è possibile fare:

- **incomplete** — nessuna sequenza di riferimento disponibile (né T1ce né T1 nativa)
- **ready** — almeno T1ce o T1 presenti, il preprocessing può essere avviato
- **queued / running** — job di preprocessing in coda o in esecuzione
- **failed** — errore in uno degli step, con messaggio esplicito
- **completed** — preprocessing terminato con strutture disponibili

La distinzione tra *ready con FeTS* e *ready con SimpleITK* è visibile nella checklist della sessione, che mostra quale modalità verrà usata in base alle sequenze disponibili.


## Percorsi principali su filesystem

| Cosa | Percorso |
|---|---|
| DICOM IRST raw | `/mnt/dati/irst_data/irst_dicom_raw/DICOM GBM/` |
| DICOM pazienti di test (ID1, ID2, ID3) | `/home/irst/gliotwin/raw/` |
| NIfTI preprocessati finali | `/mnt/dati/irst_data/irst_preprocessed_final/` |
| Staging job di preprocessing | `/mnt/dati/irst_data/processing_jobs/` |
| Database SQLite | `/home/irst/gliotwin/db/gliotwin.db` |
| Regole classificazione serie DICOM | `/mnt/dati/irst/fets/data/series_rules.csv` |


## Cosa è ancora in sviluppo

- Pipeline di import per UCSD-PTGBM, RHUH-GBM, QIN-GBM, GLIS-RT
- Brain extraction nel percorso SimpleITK (attualmente saltato)
- Analisi longitudinale sistematica dei volumi
- Export DICOM RTSTRUCT verso spazio nativo verificato end-to-end
- Modello completo di checklist per avanzamento batch per stato
