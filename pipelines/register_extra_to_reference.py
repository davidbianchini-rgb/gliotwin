#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import ants
import SimpleITK as sitk


def _read_dicom_as_nifti(dicom_dir: str, out_path: str) -> None:
    reader = sitk.ImageSeriesReader()
    files = list(reader.GetGDCMSeriesFileNames(dicom_dir))
    if not files:
        raise RuntimeError(f"No DICOM series found in {dicom_dir}")
    reader.SetFileNames(files)
    sitk.WriteImage(reader.Execute(), out_path)


def compute_native_to_canonical_transform(
    t1ce_dicom_dir: str,
    t1ce_prepared_path: str,
    transform_out_path: str,
) -> str:
    with tempfile.TemporaryDirectory(prefix="gliotwin_reg_") as tmp:
        t1ce_native_nii = str(Path(tmp) / "t1ce_native.nii.gz")
        _read_dicom_as_nifti(t1ce_dicom_dir, t1ce_native_nii)

        fixed = ants.image_read(t1ce_prepared_path)
        moving = ants.image_read(t1ce_native_nii)

        reg = ants.registration(
            fixed=fixed,
            moving=moving,
            type_of_transform="Affine",
            aff_metric="mattes",
            aff_sampling=32,
            grad_step=0.2,
            verbose=False,
        )

        tmp_mat = reg["fwdtransforms"][0]
        out = Path(transform_out_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(tmp_mat, out)
        return str(out)


def apply_transform_to_sequence(
    moving_nifti_path: str,
    reference_path: str,
    transform_path: str,
    output_path: str,
) -> None:
    fixed = ants.image_read(reference_path)
    moving = ants.image_read(moving_nifti_path)
    warped = ants.apply_transforms(
        fixed=fixed,
        moving=moving,
        transformlist=[transform_path],
        interpolator="linear",
    )
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    warped.to_file(str(out))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--t1ce-dicom", required=True)
    parser.add_argument("--t1ce-prepared", required=True)
    parser.add_argument("--moving-nifti", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--save-transform", default=None)
    parser.add_argument("--transform-in", default=None)
    args = parser.parse_args()

    transform_path = args.transform_in
    if not transform_path:
        if not args.save_transform:
            raise RuntimeError("Either --transform-in or --save-transform is required")
        transform_path = compute_native_to_canonical_transform(
            args.t1ce_dicom,
            args.t1ce_prepared,
            args.save_transform,
        )

    apply_transform_to_sequence(
        args.moving_nifti,
        args.t1ce_prepared,
        transform_path,
        args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
