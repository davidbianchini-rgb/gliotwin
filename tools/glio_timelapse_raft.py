#!/usr/bin/env python3
"""
GPU RAFT timelapse for subject 170202961.

This version does not use masks or overlays. RAFT estimates optical flow from
contrast-enhanced MRI guide frames, then the motion field is used to deform the
actual grayscale MRI frames. A confidence blend suppresses ghosting where the
two warped frames disagree.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import cv2
import nibabel as nib
import numpy as np
from scipy.interpolate import RBFInterpolator
import torch
from torchvision.models.optical_flow import Raft_Large_Weights, raft_large


SUBJECT_ID = "170202961"
OUT = Path("/mnt/dati/irst_data/tmp/170202961_t1c_timelapse.mp4")
WORK = Path("/mnt/dati/irst_data/tmp/raft_work")

FPS_OUT = 60
SECONDS_PER_WEEK = 0.7
DEFAULT_WEEKS_BETWEEN = 8.0
SEG_SECONDS = SECONDS_PER_WEEK * DEFAULT_WEEKS_BETWEEN
IMG_SIZE = 1080
FLOW_SIZE = 960
HEADER_H = 110
FOOTER_H = 18
CANVAS_W = IMG_SIZE
CANVAS_H = HEADER_H + IMG_SIZE + FOOTER_H

# These values are intentionally conservative. RAFT already estimates dense
# motion; excessive amplification is what caused the previous "hallucinated"
# motion fields.
MOTION_GAIN = 1.0
CONFIDENCE_SIGMA = 28.0
FLOW_SMOOTH_SIGMA = 1.15
FLOW_MAX_PERCENTILE = 99.2
ANCHOR_FIELD_SIZE = 480
ANCHOR_BLEND_MAX = 0.72

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


def prep_slice(vol: np.ndarray, z: int) -> tuple[np.ndarray, np.ndarray]:
    """Return display gray frame and RGB RAFT guide frame."""
    sl = np.rot90(vol[:, :, z], k=1).copy()

    display = cv2.resize(sl, (IMG_SIZE, IMG_SIZE), interpolation=cv2.INTER_LANCZOS4)
    blur = cv2.GaussianBlur(display, (0, 0), 0.8)
    display = cv2.addWeighted(display, 1.35, blur, -0.35, 0)

    guide = cv2.resize(sl, (FLOW_SIZE, FLOW_SIZE), interpolation=cv2.INTER_AREA)
    clahe = cv2.createCLAHE(clipLimit=1.5, tileGridSize=(8, 8)).apply(guide)
    gx = cv2.Sobel(clahe, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(clahe, cv2.CV_32F, 0, 1, ksize=3)
    edge = cv2.normalize(cv2.magnitude(gx, gy), None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)

    # White/high-contrast anatomy is the anchor. Encode it as a separate RAFT
    # channel instead of over-sharpening the whole image, which creates jitter.
    p85, p995 = np.percentile(guide[guide > 0], [85, 99.5])
    bright = np.clip((guide.astype(np.float32) - p85) / (p995 - p85 + 1e-8) * 255, 0, 255).astype(np.uint8)
    bright = cv2.GaussianBlur(bright, (0, 0), 0.7)

    guide_rgb = np.stack([
        clahe,
        cv2.addWeighted(clahe, 0.55, bright, 0.45, 0),
        cv2.addWeighted(clahe, 0.70, edge, 0.30, 0),
    ], axis=2)
    return display, guide_rgb


def tensor_from_rgb(rgb: np.ndarray, device: torch.device) -> torch.Tensor:
    ten = torch.from_numpy(rgb).permute(2, 0, 1).float()[None] / 255.0
    return (ten * 2.0 - 1.0).to(device)


@torch.inference_mode()
def raft_flow(model: torch.nn.Module, a: np.ndarray, b: np.ndarray, device: torch.device) -> np.ndarray:
    ta = tensor_from_rgb(a, device)
    tb = tensor_from_rgb(b, device)
    pred = model(ta, tb)[-1][0].detach().float().cpu().numpy()
    flow = regularize_flow(np.moveaxis(pred, 0, -1).astype(np.float32))
    del ta, tb, pred
    if device.type == "cuda":
        torch.cuda.empty_cache()
    return flow


def regularize_flow(flow: np.ndarray) -> np.ndarray:
    """Suppress isolated RAFT spikes while preserving coherent anatomical motion."""
    mag = np.sqrt(flow[..., 0] ** 2 + flow[..., 1] ** 2)
    cap = max(float(np.percentile(mag, FLOW_MAX_PERCENTILE)), 1.0)
    scale = np.minimum(1.0, cap / (mag + 1e-6))
    clipped = flow * scale[..., None]

    smooth = np.empty_like(clipped)
    smooth[..., 0] = cv2.GaussianBlur(clipped[..., 0], (0, 0), FLOW_SMOOTH_SIGMA)
    smooth[..., 1] = cv2.GaussianBlur(clipped[..., 1], (0, 0), FLOW_SMOOTH_SIGMA)
    return 0.75 * smooth + 0.25 * clipped


def anchor_flow(a_rgb: np.ndarray, b_rgb: np.ndarray) -> tuple[np.ndarray, np.ndarray, int]:
    """
    Track stable high-contrast/white structures with Lucas-Kanade and convert
    sparse displacements into a smooth dense elastic field.

    Returns:
        flow:  HxWx2 forward displacement A->B in FLOW_SIZE coordinates.
        conf:  HxW confidence map, high near tracked anchors.
        n:     number of retained bidirectional tracks.
    """
    a = cv2.cvtColor(a_rgb, cv2.COLOR_RGB2GRAY)
    b = cv2.cvtColor(b_rgb, cv2.COLOR_RGB2GRAY)

    # Prefer the white/enhancing structures and their borders.
    p70 = np.percentile(a[a > 0], 70)
    p92 = np.percentile(a[a > 0], 92)
    bright = cv2.inRange(a, int(p70), 255)
    edge = cv2.Canny(a, 30, 95)
    mask = cv2.bitwise_or(bright, cv2.dilate(edge, np.ones((3, 3), np.uint8)))

    pts = cv2.goodFeaturesToTrack(
        a,
        maxCorners=750,
        qualityLevel=0.006,
        minDistance=7,
        blockSize=7,
        mask=mask,
        useHarrisDetector=False,
    )
    if pts is None or len(pts) < 12:
        return np.zeros((FLOW_SIZE, FLOW_SIZE, 2), np.float32), np.zeros((FLOW_SIZE, FLOW_SIZE), np.float32), 0

    lk_params = dict(
        winSize=(31, 31),
        maxLevel=4,
        criteria=(cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 50, 0.01),
    )
    p1, st1, _ = cv2.calcOpticalFlowPyrLK(a, b, pts, None, **lk_params)
    p0r, st2, _ = cv2.calcOpticalFlowPyrLK(b, a, p1, None, **lk_params)

    p0 = pts.reshape(-1, 2)
    p1 = p1.reshape(-1, 2)
    p0r = p0r.reshape(-1, 2)
    fb_err = np.linalg.norm(p0 - p0r, axis=1)
    disp = p1 - p0
    mag = np.linalg.norm(disp, axis=1)

    good = (
        (st1.reshape(-1) == 1)
        & (st2.reshape(-1) == 1)
        & (fb_err < 1.8)
        & (mag < np.percentile(mag, 97))
    )
    p0 = p0[good]
    disp = disp[good]
    if len(p0) < 12:
        return np.zeros((FLOW_SIZE, FLOW_SIZE, 2), np.float32), np.zeros((FLOW_SIZE, FLOW_SIZE), np.float32), int(len(p0))

    # Robustly remove residual displacement outliers.
    med = np.median(disp, axis=0)
    mad = np.median(np.abs(disp - med), axis=0) + 1e-4
    keep = np.all(np.abs(disp - med) < 5.5 * mad + 2.0, axis=1)
    p0 = p0[keep]
    disp = disp[keep]

    # Add weak zero-motion boundary constraints. This prevents RBF edge folding
    # without dominating the actual anatomical anchors.
    n_edge = 12
    lin = np.linspace(0, FLOW_SIZE - 1, n_edge, dtype=np.float32)
    border = np.vstack([
        np.column_stack([lin, np.zeros_like(lin)]),
        np.column_stack([lin, np.full_like(lin, FLOW_SIZE - 1)]),
        np.column_stack([np.zeros_like(lin), lin]),
        np.column_stack([np.full_like(lin, FLOW_SIZE - 1), lin]),
    ])
    border_disp = np.zeros_like(border)
    pts_all = np.vstack([p0, border])
    disp_all = np.vstack([disp, border_disp])

    scale = ANCHOR_FIELD_SIZE / FLOW_SIZE
    pts_small = pts_all * scale
    grid_y, grid_x = np.mgrid[0:ANCHOR_FIELD_SIZE, 0:ANCHOR_FIELD_SIZE]
    grid = np.column_stack([grid_x.ravel(), grid_y.ravel()]).astype(np.float32)

    # Thin-plate spline style RBF: smooth, global, elastic.
    rbf = RBFInterpolator(
        pts_small.astype(np.float32),
        (disp_all * scale).astype(np.float32),
        kernel="thin_plate_spline",
        smoothing=4.0,
    )
    dense_small = rbf(grid).reshape(ANCHOR_FIELD_SIZE, ANCHOR_FIELD_SIZE, 2)
    dense = cv2.resize(dense_small, (FLOW_SIZE, FLOW_SIZE), interpolation=cv2.INTER_CUBIC)
    dense[..., 0] /= scale
    dense[..., 1] /= scale
    dense = regularize_flow(dense.astype(np.float32))

    markers = np.zeros((ANCHOR_FIELD_SIZE, ANCHOR_FIELD_SIZE), np.float32)
    for x, y in (p0 * scale).astype(np.int32):
        if 0 <= x < ANCHOR_FIELD_SIZE and 0 <= y < ANCHOR_FIELD_SIZE:
            markers[y, x] = 1.0
    conf_small = cv2.GaussianBlur(markers, (0, 0), sigmaX=18.0)
    conf_small = conf_small / (conf_small.max() + 1e-8)
    conf = cv2.resize(conf_small, (FLOW_SIZE, FLOW_SIZE), interpolation=cv2.INTER_LINEAR)

    # Increase confidence on bright structures, where anchors matter most.
    bright_weight = np.clip((a.astype(np.float32) - p70) / (p92 - p70 + 1e-8), 0, 1)
    conf = np.clip((0.55 * conf + 0.45 * conf * bright_weight) * ANCHOR_BLEND_MAX, 0, ANCHOR_BLEND_MAX)
    return dense.astype(np.float32), conf.astype(np.float32), int(len(p0))


def fuse_raft_with_anchors(raft: np.ndarray, anchors: np.ndarray, conf: np.ndarray) -> np.ndarray:
    flow = raft * (1.0 - conf[..., None]) + anchors * conf[..., None]
    return regularize_flow(flow.astype(np.float32))


def warp_gray(src: np.ndarray, flow: np.ndarray, amount: float, sign: float) -> np.ndarray:
    h, w = src.shape[:2]
    fh, fw = flow.shape[:2]
    flow_up = cv2.resize(flow, (w, h), interpolation=cv2.INTER_LINEAR)
    flow_up[..., 0] *= w / fw
    flow_up[..., 1] *= h / fh

    gx, gy = np.meshgrid(np.arange(w, dtype=np.float32), np.arange(h, dtype=np.float32))
    mx = (gx + sign * flow_up[..., 0] * amount * MOTION_GAIN).astype(np.float32)
    my = (gy + sign * flow_up[..., 1] * amount * MOTION_GAIN).astype(np.float32)
    return cv2.remap(src.astype(np.float32), mx, my, cv2.INTER_LINEAR, borderMode=cv2.BORDER_REPLICATE)


def choose_flow_sign(a: np.ndarray, b: np.ndarray, flow_ab: np.ndarray) -> float:
    a_gray = cv2.cvtColor(a, cv2.COLOR_RGB2GRAY) if a.ndim == 3 else a
    b_gray = cv2.cvtColor(b, cv2.COLOR_RGB2GRAY) if b.ndim == 3 else b
    wa_plus = warp_gray(a_gray, flow_ab, 1.0, +1.0)
    wa_minus = warp_gray(a_gray, flow_ab, 1.0, -1.0)
    mse_plus = float(np.mean((wa_plus - b_gray.astype(np.float32)) ** 2))
    mse_minus = float(np.mean((wa_minus - b_gray.astype(np.float32)) ** 2))
    return +1.0 if mse_plus < mse_minus else -1.0


def smootherstep(x: float) -> float:
    x = float(np.clip(x, 0.0, 1.0))
    return x * x * x * (x * (x * 6 - 15) + 10)


def blend_motion(a: np.ndarray, b: np.ndarray, flow_ab: np.ndarray, flow_ba: np.ndarray,
                 sign_ab: float, sign_ba: float, alpha: float) -> np.ndarray:
    t = smootherstep(alpha)
    wa = warp_gray(a, flow_ab, t, sign_ab)
    wb = warp_gray(b, flow_ba, 1.0 - t, sign_ba)

    blend = (1.0 - t) * wa + t * wb

    # Where the two motion-compensated frames disagree, hard blending creates
    # visible double contrast. Prefer the nearest real timepoint in those areas.
    diff = np.abs(wa - wb)
    conf = np.exp(-diff / CONFIDENCE_SIGMA)
    near_t = smootherstep((t - 0.34) / 0.32)
    nearest = (1.0 - near_t) * wa + near_t * wb
    out = conf * blend + (1.0 - conf) * nearest
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


def encode_nvenc(frames_dir: Path) -> str:
    cmd = [
        "ffmpeg", "-y", "-framerate", str(FPS_OUT),
        "-i", str(frames_dir / "frame_%05d.jpg"),
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
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return "h264_nvenc"
    except subprocess.CalledProcessError:
        cmd = [
            "ffmpeg", "-y", "-framerate", str(FPS_OUT),
            "-i", str(frames_dir / "frame_%05d.jpg"),
            "-c:v", "libx264", "-preset", "slow", "-crf", "14",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(OUT),
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return "libx264"


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
        "-cq", "14",
        "-b:v", "0",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        str(OUT),
    ]
    return subprocess.Popen(cmd, stdin=subprocess.PIPE)


def main() -> None:
    if WORK.exists():
        shutil.rmtree(WORK)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")
    print("Loading RAFT large weights...")
    weights = Raft_Large_Weights.C_T_SKHT_V2
    model = raft_large(weights=weights, progress=False).to(device).eval()

    z = choose_z()
    print(f"Using z={z}")
    displays: list[np.ndarray] = []
    guides: list[np.ndarray] = []
    for path in T1C_PATHS:
        vol = normalize_uint8(nib.as_closest_canonical(nib.load(path)).get_fdata(dtype=np.float32))
        display, guide = prep_slice(vol, z)
        displays.append(display)
        guides.append(guide)
        print(f"  loaded {Path(path).name}")

    frames_per_segment = int(round(SEG_SECONDS * FPS_OUT))
    n_segments = len(displays) - 1
    total = frames_per_segment * n_segments - (n_segments - 1) + FPS_OUT
    frame_idx = 0
    proc = ffmpeg_writer()

    try:
        for i in range(len(displays) - 1):
            print(f"RAFT flow TP{i+1}->TP{i+2}...", flush=True)
            flow_ab_raft = raft_flow(model, guides[i], guides[i + 1], device)
            flow_ba_raft = raft_flow(model, guides[i + 1], guides[i], device)
            flow_ab_anchor, conf_ab, n_ab = anchor_flow(guides[i], guides[i + 1])
            flow_ba_anchor, conf_ba, n_ba = anchor_flow(guides[i + 1], guides[i])
            flow_ab = fuse_raft_with_anchors(flow_ab_raft, flow_ab_anchor, conf_ab)
            flow_ba = fuse_raft_with_anchors(flow_ba_raft, flow_ba_anchor, conf_ba)
            sign_ab = choose_flow_sign(guides[i], guides[i + 1], flow_ab)
            sign_ba = choose_flow_sign(guides[i + 1], guides[i], flow_ba)
            print(
                f"  anchors: A->B {n_ab}, B->A {n_ba}; "
                f"signs: A->B {sign_ab:+.0f}, B->A {sign_ba:+.0f}; "
                f"frames={frames_per_segment}",
                flush=True,
            )

            # Include alpha=1.0 so the segment lands exactly on the next real
            # timepoint. Then skip alpha=0.0 on following segments to avoid a
            # duplicate boundary frame.
            k_start = 0 if i == 0 else 1
            for k in range(k_start, frames_per_segment):
                alpha = k / (frames_per_segment - 1)
                gray = blend_motion(displays[i], displays[i + 1], flow_ab, flow_ba, sign_ab, sign_ba, alpha)
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

    print(f"Saved: {OUT} (h264_nvenc, {frame_idx} frames, {FPS_OUT} fps)")


if __name__ == "__main__":
    main()
