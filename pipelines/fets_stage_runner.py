#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path


PROJECT_ROOT = Path("/project")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import prepare  # type: ignore
from stages.pipeline import Pipeline  # type: ignore
from stages.pipeline import write_report  # type: ignore
from stages.row_stage import RowStage  # type: ignore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("FeTS staged runner")
    parser.add_argument("--mode", choices=["brain", "tumor", "full"], required=True)
    parser.add_argument("--data_path", dest="data", required=True)
    parser.add_argument("--labels_path", dest="labels", required=True)
    parser.add_argument("--models_path", dest="models", required=True)
    parser.add_argument("--data_out", dest="data_out", required=True)
    parser.add_argument("--labels_out", dest="labels_out", required=True)
    parser.add_argument("--report", dest="report", required=True)
    parser.add_argument("--parameters", dest="parameters", required=True)
    parser.add_argument("--metadata_path", dest="metadata_path", required=True)
    return parser


def configure_env(args: argparse.Namespace) -> Path:
    output_path = args.data_out
    models_path = args.models

    tmpfolder = Path(output_path) / ".tmp"
    cbica_tmpfolder = tmpfolder / ".cbicaTemp"
    os.environ["TMPDIR"] = str(tmpfolder)
    os.environ["CBICA_TEMP_DIR"] = str(cbica_tmpfolder)
    os.environ["RESULTS_FOLDER"] = os.path.join(models_path, "nnUNet_trained_models")
    os.environ["nnUNet_raw_data_base"] = os.path.join(str(tmpfolder), "nnUNet_raw_data_base")
    os.environ["nnUNet_preprocessed"] = os.path.join(str(tmpfolder), "nnUNet_preprocessed")
    tmpfolder.mkdir(parents=True, exist_ok=True)
    cbica_tmpfolder.mkdir(parents=True, exist_ok=True)
    return tmpfolder


def select_stages(mode: str, pipeline: Pipeline):
    if mode == "brain":
        return pipeline.stages[:3]
    if mode == "tumor":
        return pipeline.stages[3:4]
    return pipeline.stages


def _run_selected_stages(report, report_path: str, pipeline: Pipeline, selected_stages) -> None:
    # The official FeTS pipeline keeps advancing to downstream review stages.
    # Gliotwin only needs the requested linear subset, so stop once those stages are exhausted.
    report, _ = pipeline.init_stage.execute(report)
    write_report(report, report_path)

    while True:
        progress_made = False
        for subject in list(report.index):
            while True:
                next_stage = None
                for stage in selected_stages:
                    if isinstance(stage, RowStage):
                        if stage.could_run(subject, report):
                            next_stage = stage
                            break
                    elif stage.could_run(report):
                        next_stage = stage
                        break

                if next_stage is None:
                    break

                if isinstance(next_stage, RowStage):
                    report, successful = next_stage.execute(subject, report)
                else:
                    report, successful = next_stage.execute(report)
                write_report(report, report_path)
                progress_made = True

                if not successful:
                    raise RuntimeError(f"FeTS stage failed: {next_stage.name} for {subject}")

        if not progress_made:
            break


def _restore_hidden_timepoints(stage_root: Path) -> None:
    for branch in ("DataForFeTS", "DataForQC"):
        root = stage_root / branch
        if not root.exists():
            continue
        hidden_dirs = sorted(
            [path for path in root.rglob(".*") if path.is_dir()],
            key=lambda path: len(path.parts),
            reverse=True,
        )
        for hidden_dir in hidden_dirs:
            restored = hidden_dir.with_name(hidden_dir.name.lstrip("."))
            if restored.exists():
                continue
            hidden_dir.rename(restored)


def _pick_mask(mask_dir: Path, subject_id: str, timepoint: str) -> Path:
    preferred = [
        mask_dir / f"{subject_id}_{timepoint}_tumorMask.nii.gz",
        mask_dir / f"{subject_id}_{timepoint}_tumorMask_model_0.nii.gz",
    ]
    for candidate in preferred:
        if candidate.exists():
            return candidate

    matches = sorted(mask_dir.glob("*tumorMask*.nii.gz"))
    if matches:
        return matches[0]
    raise RuntimeError(f"No tumor mask found in {mask_dir}")


def _materialize_output_labels(data_out: Path, labels_out: Path) -> None:
    tumor_qc_root = data_out / "tumor_extracted" / "DataForQC"
    if not tumor_qc_root.exists():
        raise RuntimeError(f"Tumor QC output not found in {tumor_qc_root}")

    labels_out.mkdir(parents=True, exist_ok=True)
    for subject_dir in sorted(path for path in tumor_qc_root.iterdir() if path.is_dir()):
        for timepoint_dir in sorted(path for path in subject_dir.iterdir() if path.is_dir()):
            mask_dir = timepoint_dir / "TumorMasksForQC"
            if not mask_dir.exists():
                raise RuntimeError(f"Tumor mask directory not found in {mask_dir}")
            mask_src = _pick_mask(mask_dir, subject_dir.name, timepoint_dir.name)
            case_id = f"{subject_dir.name}-{timepoint_dir.name}"
            shutil.copy2(mask_src, labels_out / f"{case_id}.nii.gz")
            shutil.copy2(mask_src, labels_out / f"{case_id}_tumorMask.nii.gz")


def normalize_staged_outputs(args: argparse.Namespace) -> None:
    data_out = Path(args.data_out)
    labels_out = Path(args.labels_out)

    # The legacy FeTS stages temporarily hide previous-stage directories with a leading dot.
    # Restore them so downstream finalization sees stable, human-readable paths.
    _restore_hidden_timepoints(data_out / "brain_extracted")

    if args.mode in {"tumor", "full"}:
        _materialize_output_labels(data_out, labels_out)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    tmpfolder = configure_env(args)
    try:
        report = prepare.init_report(args)
        base_pipeline = prepare.init_pipeline(args)
        selected_stages = select_stages(args.mode, base_pipeline)
        _run_selected_stages(report, args.report, base_pipeline, selected_stages)
        normalize_staged_outputs(args)
        return 0
    finally:
        shutil.rmtree(tmpfolder, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
