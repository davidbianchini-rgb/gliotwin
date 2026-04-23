#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import SimpleITK as sitk


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert filtered APT DICOM slice set to NIfTI.")
    parser.add_argument("--input-dir", required=True, help="Directory containing only the APTW DICOM instances.")
    parser.add_argument("--output-img", required=True, help="Output NIfTI path.")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_img = Path(args.output_img)
    output_img.parent.mkdir(parents=True, exist_ok=True)

    reader = sitk.ImageSeriesReader()
    files = list(reader.GetGDCMSeriesFileNames(str(input_dir)))
    if not files:
        raise RuntimeError(f"No readable DICOM series found in {input_dir}")
    reader.SetFileNames(files)
    image = reader.Execute()
    sitk.WriteImage(image, str(output_img))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
