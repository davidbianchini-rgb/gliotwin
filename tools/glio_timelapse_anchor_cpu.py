#!/usr/bin/env python3
"""
Anchor-driven MRI timelapse for subject 170202961.

This deliberately avoids hallucinating neural interpolation. It tracks stable
high-contrast anatomy with Lucas-Kanade, converts those sparse tracks into a
smooth elastic RBF field, and streams frames directly to ffmpeg.
"""

from __future__ import annotations

from pathlib import Path
import subprocess

import cv2
import nibabel as nib
import numpy as np
from scipy.interpolate import RBFInterpolator


SUBJECT_ID = "170202961"
OUT = Path("/mnt/dati/irst_data/tmp/170202961_t1c_timelapse.mp4")

FPS_OUT = 60
SECONDS_PER_WEEK = 0.7
DEFAULT_WEEKS_BETWEEN = 8.0
SEG_SECONDS = SECONDS_PER_WEEK * DEFAULT_WEEKS_BETWEEN

IMG_SIZE = 1080
FIELD_SIZE = 360
HEADER_H = 110
FOOTER_H = 18
CANVAS_W = IMG_SIZE
CANVAS_H = HEADER_H + IMG_SIZE + FOOTER_H

T1C_PATHS = [
    "/mnt/dati/irst_data/processing_jobs/job_00115/runs/170202961_20260416_114047/output/tumor_extracted/DataForFeTS/170202961/timepoint_001/170202961_timepoint_001_brain_t1c.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00116/runs/170202961_20260416_120647/output/tumor_extracted/DataForFeTS/170202961/timepoint_002/170202961_timepoint_002_brain_t1c.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00117/runs/170202961_20260416_123209/output/tumor_extracted/DataForFeTS/170202961/timepoint_003/170202961_timepoint_003_brain_t1c.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00118/runs/170202961_20260416_125707/output/tumor_extracted/DataForFeTS/170202961/timepoint_004/170202961_timepoint_004_brain_t1c.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00119/runs/170202961_20260416_132156/output/tumor_extracted/DataForFeTS/170202961/timepoint_005/170202961_timepoint_005_brain_t1c.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00120/runs/170202961_20260416_134653/output/tumor_extracted/DataForFeTS/170202961/timepoint_006/170202961_timepoint_006_brain_t1c.nii.gz",
]

MASK_PATHS = [
    "/mnt/dati/irst_data/processing_jobs/job_00115/runs/170202961_20260416_114047/output_labels/170202961-timepoint_001_tumorMask.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00116/runs/170202961_20260416_120647/output_labels/170202961-timepoint_002_tumorMask.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00117/runs/170202961_20260416_123209/output_labels/170202961-timepoint_003_tumorMask.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00118/runs/170202961_20260416_125707/output_labels/170202961-timepoint_004_tumorMask.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00119/runs/170202961_20260416_132156/output_labels/170202961-timepoint_005_tumorMask.nii.gz",
    "/mnt/dati/irst_data/processing_jobs/job_00120/runs/170202961_20260416_134653/output_labels/170202961-timepoint_006_tumorMask.nii.gz",
]


def normalize_uint8(vol: np.ndarray) -> np.ndarray:
    brain = vol[vol > 0]
    lo, hi = np.percentile(brain, [1, 99])
    return np.clip((vol - lo) / (hi - lo + 1e-8) * 255, 0, 255).astype(np.uint8)


def choose_z() -> int:
    combined = None
    for path in MASK_PATHS:
        mask = nib.as_closest_canonical(nib.load(path)).get_fdata(dtype=np.float32)
        combined = mask if combined is None else combined + mask
    zc = np.where(combined > 0)[2]
    return int((zc.min() + zc.max()) // 2)


def prep(vol: np.ndarray, z: int) -> tuple[np.ndarray, np.ndarray]:
    sl = np.rot90(vol[:, :, z], k=1).copy()
    display = cv2.resize(sl, (IMG_SIZE, IMG_SIZE), interpolation=cv2.INTER_LANCZOS4)
    blur = cv2.GaussianBlur(display, (0, 0), 0.8)
    display = cv2.addWeighted(display, 1.32, blur, -0.32, 0)

    guide = cv2.resize(sl, (FIELD_SIZE, FIELD_SIZE), interpolation=cv2.INTER_AREA)
    guide = cv2.createCLAHE(clipLimit=1.45, tileGridSize=(8, 8)).apply(guide)
    return display, guide


def regularize_field(flow: np.ndarray) -> np.ndarray:
    mag = np.sqrt(flow[..., 0] ** 2 + flow[..., 1] ** 2)
    cap = max(float(np.percentile(mag, 99.0)), 1.0)
    flow = flow * np.minimum(1.0, cap / (mag + 1e-6))[..., None]
    out = np.empty_like(flow)
    out[..., 0] = cv2.GaussianBlur(flow[..., 0], (0, 0), 1.5)
    out[..., 1] = cv2.GaussianBlur(flow[..., 1], (0, 0), 1.5)
    return out.astype(np.float32)


def sparse_tracks(a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    p68, p93 = np.percentile(a[a > 0], [68, 93])
    bright = cv2.inRange(a, int(p68), 255)
    edge = cv2.Canny(a, 25, 85)
    mask = cv2.bitwise_or(bright, cv2.dilate(edge, np.ones((3, 3), np.uint8)))

    pts = cv2.goodFeaturesToTrack(
        a,
        maxCorners=900,
        qualityLevel=0.0045,
        minDistance=5,
        blockSize=7,
        mask=mask,
    )
    if pts is None:
        return np.empty((0, 2), np.float32), np.empty((0, 2), np.float32)

    lk = dict(
        winSize=(35, 35),
        maxLevel=4,
        criteria=(cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 60, 0.006),
    )
    p1, st1, _ = cv2.calcOpticalFlowPyrLK(a, b, pts, None, **lk)
    p0r, st2, _ = cv2.calcOpticalFlowPyrLK(b, a, p1, None, **lk)
    p0 = pts.reshape(-1, 2)
    p1 = p1.reshape(-1, 2)
    p0r = p0r.reshape(-1, 2)
    disp = p1 - p0
    fb = np.linalg.norm(p0 - p0r, axis=1)
    mag = np.linalg.norm(disp, axis=1)
    good = (st1.reshape(-1) == 1) & (st2.reshape(-1) == 1) & (fb < 1.5) & (mag < np.percentile(mag, 96))
    p0, disp = p0[good], disp[good]
    if len(p0) < 8:
        return p0, disp

    med = np.median(disp, axis=0)
    mad = np.median(np.abs(disp - med), axis=0) + 1e-4
    keep = np.all(np.abs(disp - med) < 5.0 * mad + 1.5, axis=1)
    return p0[keep].astype(np.float32), disp[keep].astype(np.float32)


def anchor_field(a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, int]:
    pts, disp = sparse_tracks(a, b)
    n = len(pts)
    if n < 8:
        return np.zeros((FIELD_SIZE, FIELD_SIZE, 2), np.float32), n

    lin = np.linspace(0, FIELD_SIZE - 1, 14, dtype=np.float32)
    border = np.vstack([
        np.column_stack([lin, np.zeros_like(lin)]),
        np.column_stack([lin, np.full_like(lin, FIELD_SIZE - 1)]),
        np.column_stack([np.zeros_like(lin), lin]),
        np.column_stack([np.full_like(lin, FIELD_SIZE - 1), lin]),
    ])
    pts_all = np.vstack([pts, border])
    disp_all = np.vstack([disp, np.zeros_like(border)])

    yy, xx = np.mgrid[0:FIELD_SIZE, 0:FIELD_SIZE]
    grid = np.column_stack([xx.ravel(), yy.ravel()]).astype(np.float32)
    rbf = RBFInterpolator(
        pts_all,
        disp_all,
        kernel="thin_plate_spline",
        smoothing=3.5,
    )
    dense = rbf(grid).reshape(FIELD_SIZE, FIELD_SIZE, 2)
    return regularize_field(dense.astype(np.float32)), n


def warp(img: np.ndarray, field: np.ndarray, amount: float) -> np.ndarray:
    h, w = img.shape
    flow = cv2.resize(field, (w, h), interpolation=cv2.INTER_CUBIC)
    flow[..., 0] *= w / FIELD_SIZE
    flow[..., 1] *= h / FIELD_SIZE
    gx, gy = np.meshgrid(np.arange(w, dtype=np.float32), np.arange(h, dtype=np.float32))
    # field is forward A->B; backward sampling requires subtracting it.
    mx = (gx - flow[..., 0] * amount).astype(np.float32)
    my = (gy - flow[..., 1] * amount).astype(np.float32)
    return cv2.remap(img.astype(np.float32), mx, my, cv2.INTER_LINEAR, borderMode=cv2.BORDER_REPLICATE)


def smootherstep(x: float) -> float:
    x = float(np.clip(x, 0.0, 1.0))
    return x * x * x * (x * (x * 6 - 15) + 10)


def interpolate(a: np.ndarray, b: np.ndarray, field_ab: np.ndarray, field_ba: np.ndarray, alpha: float) -> np.ndarray:
    t = smootherstep(alpha)
    wa = warp(a, field_ab, t)
    wb = warp(b, field_ba, 1.0 - t)

    # Continuous anti-ghosting: where A and B disagree after warping, favor a
    # smooth source handoff over literal double exposure.
    blend = (1.0 - t) * wa + t * wb
    handoff = smootherstep((t - 0.28) / 0.44)
    source = (1.0 - handoff) * wa + handoff * wb
    diff = np.abs(wa - wb)
    conf = np.exp(-diff / 22.0)
    out = conf * blend + (1.0 - conf) * source
    return np.clip(out, 0, 255).astype(np.uint8)


def make_canvas(gray: np.ndarray, idx: int, total: int) -> np.ndarray:
    week = idx / FPS_OUT / SECONDS_PER_WEEK
    tp_est = min(int((idx / FPS_OUT) // SEG_SECONDS) + 1, len(T1C_PATHS))
    canvas = np.zeros((CANVAS_H, CANVAS_W, 3), dtype=np.uint8)
    canvas[HEADER_H:HEADER_H + IMG_SIZE] = cv2.applyColorMap(gray, cv2.COLORMAP_BONE)
    cv2.rectangle(canvas, (0, 0), (CANVAS_W, HEADER_H), (20, 20, 40), -1)
    cv2.putText(canvas, f"Soggetto  {SUBJECT_ID}", (22, 42),
                cv2.FONT_HERSHEY_SIMPLEX, 1.1, (160, 210, 255), 2)
    cv2.putText(canvas, f"Settimana {int(round(week)):>3}", (22, 92),
                cv2.FONT_HERSHEY_SIMPLEX, 1.3, (60, 220, 110), 2)
    cv2.putText(canvas, f"timepoint_{tp_est:03d}", (440, 92),
                cv2.FONT_HERSHEY_SIMPLEX, 0.85, (160, 160, 160), 1)
    y = HEADER_H + IMG_SIZE
    cv2.rectangle(canvas, (0, y), (CANVAS_W, CANVAS_H), (20, 20, 20), -1)
    cv2.rectangle(canvas, (0, y), (int(idx / max(total - 1, 1) * CANVAS_W), CANVAS_H),
                  (50, 200, 100), -1)
    return canvas


def ffmpeg_writer() -> subprocess.Popen:
    cmd = [
        "ffmpeg", "-y",
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "-s", f"{CANVAS_W}x{CANVAS_H}",
        "-r", str(FPS_OUT),
        "-i", "-",
        "-c:v", "h264_nvenc",
        "-preset", "p7",
        "-tune", "hq",
        "-rc", "vbr",
        "-cq", "15",
        "-b:v", "0",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(OUT),
    ]
    return subprocess.Popen(cmd, stdin=subprocess.PIPE)


def main() -> None:
    z = choose_z()
    print(f"Using z={z}")
    displays, guides = [], []
    for path in T1C_PATHS:
        vol = normalize_uint8(nib.as_closest_canonical(nib.load(path)).get_fdata(dtype=np.float32))
        display, guide = prep(vol, z)
        displays.append(display)
        guides.append(guide)
        print(f"  loaded {Path(path).name}")

    frames_per_segment = int(round(SEG_SECONDS * FPS_OUT))
    n_segments = len(displays) - 1
    total = frames_per_segment * n_segments - (n_segments - 1) + FPS_OUT
    frame_idx = 0

    proc = ffmpeg_writer()
    try:
        for i in range(n_segments):
            print(f"Anchors TP{i+1}->TP{i+2}...", flush=True)
            field_ab, n_ab = anchor_field(guides[i], guides[i + 1])
            field_ba, n_ba = anchor_field(guides[i + 1], guides[i])
            print(f"  tracks: A->B {n_ab}, B->A {n_ba}; frames={frames_per_segment}", flush=True)

            k_start = 0 if i == 0 else 1
            for k in range(k_start, frames_per_segment):
                alpha = k / (frames_per_segment - 1)
                gray = interpolate(displays[i], displays[i + 1], field_ab, field_ba, alpha)
                canvas = make_canvas(gray, frame_idx, total)
                proc.stdin.write(canvas.tobytes())
                frame_idx += 1

        for _ in range(FPS_OUT):
            canvas = make_canvas(displays[-1], frame_idx, total)
            proc.stdin.write(canvas.tobytes())
            frame_idx += 1
    finally:
        if proc.stdin:
            proc.stdin.close()
        ret = proc.wait()
        if ret != 0:
            raise RuntimeError(f"ffmpeg failed with exit code {ret}")

    print(f"Saved: {OUT} ({frame_idx} frames, {FPS_OUT} fps)")


if __name__ == "__main__":
    main()
