#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import SimpleITK as sitk


def apply_mask(image_path: str, mask_path: str, output_path: str) -> None:
    image = sitk.ReadImage(image_path)
    mask = sitk.ReadImage(mask_path)

    if image.GetSize() != mask.GetSize():
        raise RuntimeError(
            f"Mask/image size mismatch: image={image.GetSize()} mask={mask.GetSize()}"
        )

    mask = sitk.Cast(mask > 0, sitk.sitkFloat32)
    masked = sitk.Cast(image, sitk.sitkFloat32) * mask

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    sitk.WriteImage(masked, str(out))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--mask", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    apply_mask(args.image, args.mask, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
