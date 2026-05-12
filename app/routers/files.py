import tempfile
from pathlib import Path

import nibabel as nib
import numpy as np
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response

router = APIRouter(tags=["files"])

PROJECT_ROOT  = Path(__file__).resolve().parents[2]
ALLOWED_ROOTS = [PROJECT_ROOT, Path("/mnt/dati")]

ALLOWED_SUFFIXES = {".nii", ".gz", ".json", ".mat"}


def _resolve_safe(rel_path: str) -> Path:
    # Absolute paths (e.g. /mnt/dati/...) are preserved by Path division
    resolved = (PROJECT_ROOT / rel_path).resolve()
    if not any(str(resolved).startswith(str(root)) for root in ALLOWED_ROOTS):
        raise HTTPException(403, "Path outside allowed roots")
    return resolved


def _process_nifti(
    resolved: Path,
    file_path: str,
    label_code: int | None,
    outline: bool,
) -> bytes | None:
    """
    Load a NIfTI, apply any requested transformations in order, and return
    gzip-compressed NIfTI bytes.  Returns None if no transformation is needed
    (caller falls through to a plain FileResponse).

    Transformations (composed in this order):
      1. label_code — extract voxels equal to this integer as a binary mask
      2. outline    — keep only border voxels (3D surface shell via erosion)
      3. HD-GLIO-AUTO fix — copy qform→sform so NiiVue uses world coordinates
                            that align with native-space sequences (sform_code=1)
    """
    is_hd_glio = (
        "HD-GLIO-AUTO" in file_path
        and file_path.endswith("segmentation.nii.gz")
    )
    needs_processing = is_hd_glio or label_code is not None or outline

    if not needs_processing:
        return None

    try:
        img    = nib.load(str(resolved))
        data   = img.get_fdata(dtype=np.float32)
        affine = img.affine.copy()

        # 1. Extract one label from a multi-label map
        if label_code is not None:
            data = (data == label_code).astype(np.float32)

        # 2. Compute 2D per-slice outline (border voxels in each axial slice).
        #    3D erosion doesn't work on thin volumes (e.g. 24 z-slices) because
        #    it eats the entire mask and the "border" becomes solid planes.
        if outline and data.any():
            from scipy.ndimage import binary_erosion
            struct2d = np.ones((3, 3), dtype=bool)
            binary   = data > 0
            border   = np.zeros_like(binary)
            for k in range(binary.shape[2]):
                slc = binary[:, :, k]
                if slc.any():
                    border[:, :, k] = slc & ~binary_erosion(slc, structure=struct2d)
            data = border.astype(np.float32)

        # 3. HD-GLIO-AUTO: sform_code=0 (invalid), qform_code=1 (valid).
        #    NiiVue uses sform when sform_code>0, qform otherwise.
        #    Fix: copy qform affine to sform so the overlay aligns with the
        #    native sequences (which have sform_code=1).  No data manipulation.
        if is_hd_glio:
            qform = img.get_qform()
            affine = qform

        new_img = nib.Nifti1Image(data.astype(np.uint8), affine=affine)

        with tempfile.NamedTemporaryFile(suffix=".nii.gz", delete=True) as tmp:
            tmp_path = tmp.name
        nib.save(new_img, tmp_path)
        out = Path(tmp_path).read_bytes()
        Path(tmp_path).unlink(missing_ok=True)
        return out

    except Exception:
        return None


@router.api_route("/files/{file_path:path}", methods=["GET", "HEAD"])
async def serve_file(
    file_path: str,
    request: Request,
    label_code: int | None = Query(default=None),
    outline: bool = Query(default=False),
):
    """
    Serve project files by relative path.

    Optional query params:
      label_code=N  — extract a single integer label as a binary mask
      outline=true  — return only the 3D surface border voxels
    Special: HD-GLIO-AUTO segmentation masks are axis-flipped and have their
             affine corrected in memory so they align with the native T1.
    """
    resolved = _resolve_safe(file_path)

    if not resolved.exists():
        raise HTTPException(404, f"File not found: {file_path}")

    if resolved.suffix not in ALLOWED_SUFFIXES:
        raise HTTPException(403, f"File type not allowed: {resolved.suffix}")

    processed = _process_nifti(resolved, file_path, label_code, outline)

    _CACHE = "max-age=86400"

    if processed is not None:
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(processed)),
            "Cache-Control": _CACHE,
        }
        if request.method == "HEAD":
            return Response(status_code=200, headers=headers)
        return Response(
            content=processed,
            media_type="application/octet-stream",
            headers={"Accept-Ranges": "bytes", "Cache-Control": _CACHE},
        )

    # ── Plain serve ───────────────────────────────────────────────
    if request.method == "HEAD":
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(resolved.stat().st_size),
                "Content-Type": "application/octet-stream",
                "Cache-Control": _CACHE,
            },
        )

    return FileResponse(
        path=resolved,
        media_type="application/octet-stream",
        headers={"Accept-Ranges": "bytes", "Cache-Control": _CACHE},
    )
