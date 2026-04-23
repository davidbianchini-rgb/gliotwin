#!/usr/bin/env python3
"""Register APT from native DICOM space into FeTS canonical (SRI24) space.

Strategy
--------
FeTS reorients every sequence from native patient-LPS (oblique DICOM acquisition)
to the SRI24 atlas space (1 mm isotropic, identity direction, origin [0,-239,0]).
The atlas-registration transform is NOT saved by FeTS.

We recover it in two steps:

  1. Register T1ce_native (DICOM, with skull) to T1ce_prepared (FeTS canonical,
     with skull) using ANTs affine + MI.  Both images are the same modality and
     share skull content → the registration is robust and fast (~15 s).

  2. Apply the resulting affine transform to APT_native, resampling it into the
     same canonical space.

Usage
-----
    python register_apt_to_reference.py \\
        --t1ce-dicom    /path/to/t1ce_dicom_dir   \\
        --t1ce-prepared /path/to/t1c_prepared.nii.gz \\
        --apt-nifti     /path/to/apt_native.nii.gz  \\
        --output        /path/to/apt_canonical.nii.gz \\
        [--save-transform /path/to/transform.mat]
"""
from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import ants
import SimpleITK as sitk


def _read_dicom_as_nifti(dicom_dir: str, out_path: str) -> None:
    """Read a DICOM series with SimpleITK and write to NIfTI (for ANTs)."""
    reader = sitk.ImageSeriesReader()
    files  = list(reader.GetGDCMSeriesFileNames(dicom_dir))
    if not files:
        raise RuntimeError(f"No DICOM series found in {dicom_dir}")
    reader.SetFileNames(files)
    sitk.WriteImage(reader.Execute(), out_path)


def compute_native_to_canonical_transform(
    t1ce_dicom_dir: str,
    t1ce_prepared_path: str,
    transform_out_path: str | None = None,
) -> str:
    """Register T1ce_native (DICOM) → T1ce_prepared (SRI24) with ANTs affine.

    Returns path to the saved .mat transform file (in a temp dir if
    transform_out_path is None).
    """
    with tempfile.TemporaryDirectory(prefix="gliotwin_reg_") as tmp:
        t1ce_native_nii = str(Path(tmp) / "t1ce_native.nii.gz")
        _read_dicom_as_nifti(t1ce_dicom_dir, t1ce_native_nii)

        fixed  = ants.image_read(t1ce_prepared_path)
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

        if transform_out_path:
            Path(transform_out_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(tmp_mat, transform_out_path)
            return transform_out_path

        # Caller did not request a persistent copy; keep it in tmp — but tmp is
        # about to be deleted.  Materialize a sibling temp file instead.
        persistent = tempfile.NamedTemporaryFile(
            suffix=".mat", prefix="gliotwin_reg_", delete=False
        )
        persistent.close()
        shutil.copy2(tmp_mat, persistent.name)
        return persistent.name


def apply_transform_to_apt(
    apt_nifti_path: str,
    t1ce_prepared_path: str,
    transform_path: str,
    output_path: Path,
) -> None:
    fixed  = ants.image_read(t1ce_prepared_path)
    moving = ants.image_read(apt_nifti_path)
    warped = ants.apply_transforms(
        fixed=fixed,
        moving=moving,
        transformlist=[transform_path],
        interpolator="linear",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    warped.to_file(str(output_path))


def register_apt(
    t1ce_dicom_dir: str,
    t1ce_prepared_path: str,
    apt_nifti_path: str,
    output_path: Path,
    save_transform_path: Path | None = None,
) -> None:
    transform_path = compute_native_to_canonical_transform(
        t1ce_dicom_dir,
        t1ce_prepared_path,
        str(save_transform_path) if save_transform_path else None,
    )
    apply_transform_to_apt(apt_nifti_path, t1ce_prepared_path, transform_path, output_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--t1ce-dicom",     required=True,
                        help="T1ce DICOM series directory (native space, with skull).")
    parser.add_argument("--t1ce-prepared",  required=True,
                        help="T1ce FeTS prepared NIfTI (canonical SRI24 space, with skull).")
    parser.add_argument("--apt-nifti",      required=True,
                        help="APT NIfTI in native DICOM space.")
    parser.add_argument("--output",         required=True,
                        help="Output APT NIfTI in canonical SRI24 space.")
    parser.add_argument("--save-transform", default=None,
                        help="Optional: save the computed ANTs affine transform (.mat).")
    args = parser.parse_args()

    register_apt(
        t1ce_dicom_dir      = args.t1ce_dicom,
        t1ce_prepared_path  = args.t1ce_prepared,
        apt_nifti_path      = args.apt_nifti,
        output_path         = Path(args.output),
        save_transform_path = Path(args.save_transform) if args.save_transform else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
