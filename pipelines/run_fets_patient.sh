#!/bin/bash
# =============================================================================
# run_fets_patient.sh
# Pipeline FeTS standalone - preprocessing + segmentazione tumorale
# Bypassa completamente MedPerf, niente login, niente dataset registration
#
# Uso:
#   ./run_fets_patient.sh --paziente PAZ_ID --input /path/to/input --output /path/to/output
#
# La cartella input deve avere struttura:
#   <input>/
#   └── PAZ_ID/
#       └── timepoint_001/   (o qualsiasi nome timepoint, deve essere ordinabile)
#           ├── t1c/         ← DICOM T1 con contrasto
#           ├── t1n/         ← DICOM T1 nativo
#           ├── t2f/         ← DICOM FLAIR
#           └── t2w/         ← DICOM T2
#
# Output (per ogni run):
#   <output>/PAZ_ID_TIMESTAMP/
#   ├── output/              ← NIfTI preparati (skull-stripped, registrati SRI)
#   ├── output_labels/       ← Segmentazione tumorale (tumorMask_model_0.nii.gz)
#   ├── metadata/
#   └── report.yaml
# =============================================================================

set -euo pipefail

SIF="/mnt/dati/irst/medperf_storage/rano-data-prep-mlcube_1.0.11.sif"
MODELS="/mnt/dati/irst/medperf_storage/.medperf/cubes/api_medperf_org/99/additional_files/models"
PARAMS="/mnt/dati/irst/medperf_storage/.medperf/cubes/api_medperf_org/99/parameters.yaml"
HOST_PIPELINES_DIR="/home/irst/gliotwin/pipelines"
CONTAINER_PIPELINES_DIR="/gliotwin_pipelines"

usage() {
  echo "Uso: $0 --paziente PAZ_ID --input /path/input --output /path/output [--gpu 0] [--no-gpu]"
  echo ""
  echo "  --paziente   ID paziente (deve corrispondere alla cartella in --input)"
  echo "  --input      Cartella radice contenente la cartella paziente"
  echo "  --output     Cartella dove salvare i risultati"
  echo "  --gpu N      Numero GPU da usare per la fase tumorale (default: 0)"
  echo "  --no-gpu     Esegui anche la fase tumorale su CPU"
  exit 1
}

PAZ_ID=""
INPUT_BASE=""
OUTPUT_BASE=""
GPU_ID="0"
USE_GPU=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --paziente) PAZ_ID="$2"; shift 2 ;;
    --input) INPUT_BASE="$2"; shift 2 ;;
    --output) OUTPUT_BASE="$2"; shift 2 ;;
    --gpu) GPU_ID="$2"; shift 2 ;;
    --no-gpu) USE_GPU=false; shift ;;
    *) echo "Argomento sconosciuto: $1"; usage ;;
  esac
done

[[ -z "$PAZ_ID" || -z "$INPUT_BASE" || -z "$OUTPUT_BASE" ]] && usage

if [[ ! -f "$SIF" ]]; then
  echo "ERRORE: Container non trovato: $SIF"
  exit 1
fi

if [[ ! -f "$HOST_PIPELINES_DIR/fets_stage_runner.py" ]]; then
  echo "ERRORE: Stage runner non trovato: $HOST_PIPELINES_DIR/fets_stage_runner.py"
  exit 1
fi

if [[ ! -d "$INPUT_BASE/$PAZ_ID" ]]; then
  echo "ERRORE: Cartella paziente non trovata: $INPUT_BASE/$PAZ_ID"
  echo "Struttura attesa: $INPUT_BASE/$PAZ_ID/timepoint_001/{t1c,t1n,t2f,t2w}/"
  exit 1
fi

MISSING=0
for TP in "$INPUT_BASE/$PAZ_ID"/*/; do
  for SEQ in t1c t1n t2f t2w; do
    if [[ ! -d "$TP/$SEQ" ]]; then
      echo "ATTENZIONE: Manca la cartella $TP/$SEQ"
      MISSING=1
    fi
  done
done
if [[ $MISSING -eq 1 ]]; then
  echo "ERRORE: Struttura input incompleta. Controlla le cartelle."
  exit 1
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR="${OUTPUT_BASE}/${PAZ_ID}_${TIMESTAMP}"
OUT_PREPARED="${OUT_DIR}/output"
OUT_LABELS="${OUT_DIR}/output_labels"
OUT_METADATA="${OUT_DIR}/metadata"
TMP_PARAMS="${OUT_DIR}/parameters.yaml"

mkdir -p "$OUT_PREPARED" "$OUT_LABELS" "$OUT_METADATA"
cp "$PARAMS" "$TMP_PARAMS"

echo "============================================================"
echo "  FeTS Pipeline Standalone"
echo "  Paziente   : $PAZ_ID"
echo "  Input      : $INPUT_BASE"
echo "  Output     : $OUT_DIR"
echo "  Container  : $SIF"
echo "  GPU        : $([ "$USE_GPU" = true ] && echo "GPU $GPU_ID" || echo "CPU")"
echo "  Avvio      : $(date)"
echo "============================================================"

BIND_ARGS=(
  "--bind" "${INPUT_BASE}:/mlcube_io0"
  "--bind" "${INPUT_BASE}:/mlcube_io1"
  "--bind" "${OUT_DIR}:/mlcube_io2"
  "--bind" "${MODELS}:/mlcube_io3/models"
  "--bind" "${OUT_PREPARED}:/mlcube_io4"
  "--bind" "${OUT_LABELS}:/mlcube_io5"
  "--bind" "${OUT_DIR}:/mlcube_io6"
  "--bind" "${OUT_METADATA}:/mlcube_io7"
  "--bind" "${HOST_PIPELINES_DIR}:${CONTAINER_PIPELINES_DIR}"
)

BASE_CMD=(
  "python" "${CONTAINER_PIPELINES_DIR}/fets_stage_runner.py"
  "--data_path=/mlcube_io0/"
  "--labels_path=/mlcube_io1/"
  "--models_path=/mlcube_io3/models"
  "--data_out=/mlcube_io4/"
  "--labels_out=/mlcube_io5/"
  "--report=/mlcube_io6/report.yaml"
  "--parameters=/mlcube_io2/parameters.yaml"
  "--metadata_path=/mlcube_io7/"
)

LOG_FILE="${OUT_DIR}/pipeline.log"
echo "Log: $LOG_FILE"
echo ""

set +e
apptainer exec \
  "${BIND_ARGS[@]}" \
  "$SIF" \
  "${BASE_CMD[@]}" \
  "--mode=brain" \
  2>&1 | tee "$LOG_FILE"
PHASE1_EXIT=${PIPESTATUS[0]}

if [[ $PHASE1_EXIT -eq 0 ]]; then
  if [[ "$USE_GPU" = true ]]; then
    env "APPTAINERENV_CUDA_VISIBLE_DEVICES=${GPU_ID}" apptainer exec \
      --nv \
      "${BIND_ARGS[@]}" \
      "$SIF" \
      "${BASE_CMD[@]}" \
      "--mode=tumor" \
      2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
  else
    apptainer exec \
      "${BIND_ARGS[@]}" \
      "$SIF" \
      "${BASE_CMD[@]}" \
      "--mode=tumor" \
      2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
  fi
else
  EXIT_CODE=$PHASE1_EXIT
fi
set -e

echo ""
echo "============================================================"
if [[ $EXIT_CODE -eq 0 ]]; then
  echo "  COMPLETATO con successo: $(date)"
  echo ""
  echo "  Output NIfTI preparati:"
  mapfile -t prepared_preview < <(find "$OUT_PREPARED" -name "*.nii.gz" 2>/dev/null)
  printf '%s\n' "${prepared_preview[@]:0:10}"
  echo ""
  echo "  Segmentazione tumorale:"
  mapfile -t label_preview < <(find "$OUT_LABELS" \( -name "*tumorMask*" -o -name "*seg*" \) 2>/dev/null)
  printf '%s\n' "${label_preview[@]:0:5}"
  echo ""
  echo "  Report: ${OUT_DIR}/report.yaml"
else
  echo "  ERRORE (exit code: $EXIT_CODE): $(date)"
  echo "  Controlla il log: $LOG_FILE"
fi
echo "============================================================"

exit $EXIT_CODE
