/* ============================================================
   Main view — browser + lightweight viewer
   Layout:
     LEFT   : filtri, pazienti, sessioni, strutture
     RIGHT  : viewer 2D singolo con controller perimetrale di blending

   Obiettivo:
     Mantenere un solo canvas NiiVue, ma consentire una navigazione
     continua tra serie registrate senza saltare tra tab discreti.
   ============================================================ */

// ── Orientamenti 2D ──────────────────────────────────────────
const VIEW_MODES = [
  { label: 'Axial',    sliceType: 0 },
  { label: 'Sagittal', sliceType: 2 },
  { label: 'Coronal',  sliceType: 1 },
];

const SERIES_ORDER = ['T1ce', 'CT1', 'T1', 'T2', 'FLAIR', 'APT', 'DWI', 'Ktrans', 'nrCBV'];
let _seqRailOrder = ['T1', 'T1ce', 'CT1', 'T2', 'FLAIR', 'APT', 'DWI', 'Ktrans', 'nrCBV'];
let _seqRailDragType = null;
const SERIES_ALIASES = {
  T1ce: 'T1+C',
  Ktrans: 'Ktrans',
  nrCBV: 'nrCBV',
  CT1: 'T1+C',
  APT: 'APT',
};

const SIGNAL_SEQUENCE_COLORMAP = {
  APT: 'apt_heat',
};

// ── Colori swatch ────────────────────────────────────────────
const _SC = {
  et:'#ef4444', enhancing_tumor:'#ef4444',
  netc:'#3b7ef8', necrosis:'#3b7ef8', necrotic_core:'#3b7ef8',
  snfh:'#facc15', edema:'#facc15',
  whole_tumor:'#22c55e', wt:'#22c55e',
  tc:'#3b7ef8', tumor_core:'#3b7ef8', tumor_mask:'#ef4444',
  rc:'#22c55e', resection_cavity:'#22c55e',
  brain_mask:'#22c97a',
};
const _sc = l => _SC[(l||'').toLowerCase()] || '#94a3b8';

function _safeUpper(value, fallback = '') {
  const text = String(value ?? '').trim();
  return text ? text.toUpperCase() : fallback;
}

function _friendlyStructureLabel(label) {
  const key = String(label || '').trim().toLowerCase();
  const map = {
    enhancing_tumor: 'Enhancing Tumor',
    edema: 'Edema',
    necrosis: 'Necrotic Core',
    necrotic_core: 'Necrotic Core',
    resection_cavity: 'Resection Cavity',
    tumor_core: 'Tumor Core',
    whole_tumor: 'Whole Tumor',
    tumor_mask: 'Tumor Mask',
    brain_mask: 'Brain Mask',
    et: 'Enhancing Tumor',
    snfh: 'Edema',
    netc: 'Necrotic Core',
    rc: 'Resection Cavity',
    wt: 'Whole Tumor',
    tc: 'Tumor Core',
    malato: 'Malato',
    sano: 'Sano',
  };
  if (map[key]) return map[key];
  return String(label || 'Structure')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function _structUrl(maskPath, labelCode) {
  const encoded = maskPath.split('/').map(encodeURIComponent).join('/');
  const params  = ['outline=1'];
  if (labelCode != null) params.push(`label_code=${labelCode}`);
  return `/api/files/${encoded}?${params.join('&')}`;
}

// ── Stato modulo ─────────────────────────────────────────────
let _S = {
  patients:      null,
  sessions:      [],
  overlayItems:  [],
  allStructsRaw: [],
  overlaySpace:  'native',
  selPid:        null,
  selSid:        null,
  // viewer
  nv2D:          null,
  nvGrid:        new Map(),
  viewMode:      'grid',   // 'grid' | 'focus'
  focusedSlotKey:  null,
  focusedSliceType: null,
  _focusNvKey:         null,  // slot::sliceType caricato in nv2D
  _sharedCrosshairPos: null,  // [x,y,z] 0-1 condiviso tra tutti i viewer
  _sharedCrosshairMM:  null,  // [x,y,z] mm, usato per allineare serie con affine/dimensioni diverse
  _crosshairLocked: false,   // se true, i click non spostano la crosshair
  _gridWW:   null,           // window width corrente in griglia (null = auto)
  _gridWL:   null,           // window level corrente in griglia
  _gridZoom: 1.0,            // zoom condiviso tra tutte le celle della griglia
  lastSeqs:      [],
  activeSlots:   [],
  _scrubberPreferredSeqType: null,
  _tlBuilding: false,
  activeSeriesKey: null,
  targetSeriesKey: null,
  activeConnectionKey: null,
  blendFactor: 0,
  activeOrientation: VIEW_MODES[0].sliceType,
  viewerNotice:  '',
  patientDetail: null,
  signalTimeline: null,
  signalTimelineLoading: false,
  signalMetricStatus: null,
  signalOptions: {
    labels: [],
    sequenceTypes: [],
    sources: ['preferred', 'radiological', 'computed'],
  },
  selectedSignalLabel: '',
  selectedSignalSequence: 'APT',
  selectedSignalSource: 'preferred',
  activeRightTab: 'viewer',
};
let _signalChartInstances = [];

function _currentPatientListItem() {
  return (_S.patients || []).find((item) => item.id === _S.selPid) || null;
}

function _currentPatientData() {
  return _S.patientDetail || _currentPatientListItem();
}

function _parseIsoDate(value) {
  if (!value) return null;
  const text = String(value).trim();
  // YYYY-MM-DD  oppure  YYYYMMDD (formato DICOM / SQLite)
  const m = text.match(/^(\d{4})-(\d{2})-(\d{2})$/) || text.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!m) return null;
  const dt = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function _pointTemporalDate(point) {
  return _parseIsoDate(point?.study_date) || null;
}

function _eventTemporalDate(event) {
  return _parseIsoDate(event?.event_date) || null;
}

// ── Crosshair condivisa ───────────────────────────────────────
let _gridCrosshairSyncing = false;
// True quando il prossimo onLocationChange viene da un click (non da scroll/frecce)
let _crosshairFromClick   = false;

// ── Debounce reload overlay ──────────────────────────────────
let _loadTimer = null;
let _loadSeq = 0;
let _blendDrag = null;
let _signalMetricPollTimer = null;
let _signalTimelineRequestSeq = 0;
function _scheduleLoad() {
  clearTimeout(_loadTimer);
  _loadTimer = setTimeout(_reloadOverlays, 220);
}

// ── Overlay helpers (global, read from _S.allStructsRaw) ─────────────────
// Paired colors: each structure has a "primary" (FeTS) and "alt" (rh-GlioSeg) shade
const _OV_COLORS = {
  enhancing_tumor:  { primary: '#f97316', alt: '#fbbf24' },  // orange / amber
  edema:            { primary: '#3b7ef8', alt: '#38bdf8' },  // blue / sky
  necrotic_core:    { primary: '#ef4444', alt: '#f472b6' },  // red / pink
  resection_cavity: { primary: '#a78bfa', alt: '#c084fc' },  // violet / lavender
};
const _OV_MODEL_SHORT = {
  'fets_postop':   'FeTS',
  'fets_official': 'FeTS',
  'rh-glioseg-v3': 'rh-GlioSeg',
};
function _ovModelShort(name) {
  if (!name) return '';
  if (_OV_MODEL_SHORT[name]) return _OV_MODEL_SHORT[name];
  if (name.startsWith('fets') || name.startsWith('hd_glio')) return 'FeTS';
  return name;
}
function _ovIsAlt(modelName) {
  return modelName === 'rh-glioseg-v3';
}
function _ovColor(label, modelName) {
  const pair = _OV_COLORS[label];
  if (!pair) return _ovIsAlt(modelName) ? '#94a3b8' : '#8395b0';
  return _ovIsAlt(modelName) ? pair.alt : pair.primary;
}
// Palette per strutture radiologiche: [label_key → [{color, colormap}, ...]]
// OP1 usa la prima entry, OP2 la seconda, ecc.
const _RADIO_PALETTE = {
  malato: [
    { color: '#ef4444', colormap: 'glio_red'       },
    { color: '#b91c1c', colormap: 'glio_red_dark'  },
    { color: '#fca5a5', colormap: 'glio_red'       },
  ],
  sano: [
    { color: '#22c55e', colormap: 'glio_green'      },
    { color: '#15803d', colormap: 'glio_green_dark' },
    { color: '#86efac', colormap: 'glio_green'      },
  ],
  _other: [
    { color: '#a78bfa', colormap: 'glio_violet'      },
    { color: '#7c3aed', colormap: 'glio_violet_dark' },
    { color: '#c4b5fd', colormap: 'glio_violet'      },
  ],
};
function _radioPalette(label, annotatorIdx) {
  const key = (label || '').toLowerCase();
  const palette = _RADIO_PALETTE[key] || _RADIO_PALETTE._other;
  return palette[Math.min(annotatorIdx, palette.length - 1)];
}
function _ovBuildItems(structs) {
  const annotators = [...new Set(structs.filter(s => s.annotator).map(s => s.annotator))].sort();
  const annotatorIdx = Object.fromEntries(annotators.map((a, i) => [a, i]));
  return structs.map(s => {
    const isRadio = !!s.annotator;
    const entry = isRadio ? _radioPalette(s.label, annotatorIdx[s.annotator] ?? 0) : null;
    const color    = entry ? entry.color    : _ovColor(s.label, s.model_name);
    const colormap = entry ? entry.colormap : null;
    const suffix = isRadio ? s.annotator : _ovModelShort(s.model_name);
    return {
      url:          _structUrl(s.mask_path, s.label_code),
      name:         s.mask_path.split('/').pop(),
      label:        s.label,
      displayLabel: suffix ? `${_friendlyStructureLabel(s.label)} · ${suffix}` : _friendlyStructureLabel(s.label),
      color,
      colormap,
      volume_ml:    s.volume_ml,
      model_name:   s.model_name,
      annotator:    s.annotator || null,
      reference_space: s.reference_space || 'native',
    };
  });
}
function _ovRenderList() {
  _S.overlayItems = _ovBuildItems(_S.allStructsRaw);
  const ovList = document.getElementById('ov-list');
  const ovSec  = document.getElementById('ov-sec');
  if (!ovList) return;
  if (_S.overlayItems.length) {
    // Group by label for visual separation
    const byLabel = {};
    _S.overlayItems.forEach((item, i) => {
      if (!byLabel[item.label]) byLabel[item.label] = [];
      byLabel[item.label].push({ item, i });
    });
    const labelOrder = ['enhancing_tumor', 'edema', 'necrotic_core', 'resection_cavity'];
    const groups = [...labelOrder, ...Object.keys(byLabel).filter(l => !labelOrder.includes(l))];
    ovList.innerHTML = groups.filter(l => byLabel[l]).map(label => {
      return byLabel[label].map(({ item, i }) => {
        const vol = item.volume_ml != null ? Number(item.volume_ml).toFixed(1) + ' mL' : '';
        return `<label class="ov-item">
          <input type="checkbox" class="ov-cb" value="${i}" checked onchange="_scheduleLoad()">
          <span class="ov-swatch" style="border:1.5px solid ${item.color};background:${item.color}22"></span>
          <span class="ov-label">${item.displayLabel}</span>
          ${vol ? `<span class="ov-vol">${vol}</span>` : ''}
        </label>`;
      }).join('');
    }).join('');
    ovSec.style.display = '';
  } else {
    ovList.innerHTML = '<div style="color:#8395b0;font-size:11px;padding:4px 0">Nessuna struttura disponibile.</div>';
    ovSec.style.display = '';
  }
}

// ── Status badge ─────────────────────────────────────────────
function _setStatus(msg, cls = '') {
  const el = document.getElementById('nv-status');
  if (!el) return;
  el.textContent = msg;
  el.className   = 'nv-status' + (cls ? ' visible ' + cls : '');
  if (cls === 'nv-ok') setTimeout(() => {
    el.classList.contains('nv-ok') && (el.className = 'nv-status');
  }, 2000);
}

let _tlStatusTimer = null;
function _setTlStatus(loaded, total) {
  const el = document.getElementById('tl-status');
  if (!el) return;
  clearTimeout(_tlStatusTimer);
  if (total === 0) { el.className = 'tl-status'; return; }
  const done = loaded >= total;
  const pct  = Math.round((loaded / total) * 100);
  el.innerHTML = done
    ? `<span class="tl-st-label">Pronto ✓</span>`
    : `<span class="tl-st-label">Caricamento ${loaded}/${total}</span><span class="tl-st-bar"><span style="width:${pct}%"></span></span>`;
  el.className = 'tl-status visible' + (done ? ' tl-st-ok' : ' tl-st-loading');
  if (done) _tlStatusTimer = setTimeout(() => {
    el.classList.contains('tl-st-ok') && (el.className = 'tl-status');
  }, 3000);
}

function _isViewablePath(path) {
  if (!path) return false;
  return path.endsWith('.nii') || path.endsWith('.nii.gz');
}

function _clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function _friendlySeriesLabel(seq) {
  const label = SERIES_ALIASES[seq.sequence_type] || seq.sequence_type || 'Series';
  return String(label).trim();
}

function _seriesColormap(sequenceType) {
  return SIGNAL_SEQUENCE_COLORMAP[sequenceType] || 'gray';
}

function _seriesSortValue(seq) {
  const idx = SERIES_ORDER.indexOf(seq.sequence_type);
  return idx >= 0 ? idx : 100 + idx;
}

function _sortSequenceTypes(values) {
  return [...(values || [])].sort((a, b) => {
    const ai = SERIES_ORDER.indexOf(a);
    const bi = SERIES_ORDER.indexOf(b);
    const av = ai >= 0 ? ai : 100;
    const bv = bi >= 0 ? bi : 100;
    if (av !== bv) return av - bv;
    return String(a || '').localeCompare(String(b || ''));
  });
}

function _sequenceViewPath(seq, { preferNative = false } = {}) {
  const raw = seq?.raw_path || '';
  const processed = seq?.processed_path || '';
  if (preferNative && _isViewablePath(raw)) return raw;
  if (_isViewablePath(processed)) return processed;
  if (_isViewablePath(raw)) return raw;
  return '';
}

function _buildViewableSlots(seqs, options = {}) {
  return (Array.isArray(seqs) ? seqs : [])
    .filter(seq => _isViewablePath(_sequenceViewPath(seq, options)))
    .sort((a, b) => {
      const diff = _seriesSortValue(a) - _seriesSortValue(b);
      if (diff !== 0) return diff;
      return _friendlySeriesLabel(a).localeCompare(_friendlySeriesLabel(b));
    })
    .map((seq, index) => ({
      key: `seq-${seq.id || `${seq.sequence_type || 'series'}-${index}`}`,
      label: _friendlySeriesLabel(seq),
      shortLabel: _friendlySeriesLabel(seq),
      sequenceType: seq.sequence_type || '',
      seqPath: _sequenceViewPath(seq, options),
      colormap: _seriesColormap(seq.sequence_type || ''),
      sequenceId: seq.id || null,
    }));
}

function _preferredSeriesKey(slots) {
  for (const sequenceType of SERIES_ORDER) {
    const match = slots.find(slot => slot.sequenceType === sequenceType);
    if (match) return match.key;
  }
  return slots[0]?.key || null;
}

function _currentSlot() {
  return _S.activeSlots.find(slot => slot.key === _S.activeSeriesKey) || null;
}

function _currentTargetSlot() {
  if (!_S.targetSeriesKey) return null;
  return _S.activeSlots.find(slot => slot.key === _S.targetSeriesKey) || null;
}

function _blendSummary() {
  const source = _currentSlot();
  const target = _currentTargetSlot();
  const factor = _clamp01(_S.blendFactor);
  if (!source) return 'No active series';
  if (!target || !_S.activeConnectionKey) return `${source.label} 100%`;
  return `${source.label} ${Math.round((1 - factor) * 100)}% · ${target.label} ${Math.round(factor * 100)}%`;
}

function _connectionKey(sourceKey, targetKey) {
  return sourceKey && targetKey ? `${sourceKey}->${targetKey}` : null;
}

function _clearBlendTransition() {
  _S.targetSeriesKey = null;
  _S.activeConnectionKey = null;
  _S.blendFactor = 0;
}

function _selectPureSeries(seriesKey) {
  _S.activeSeriesKey = seriesKey;
  _clearBlendTransition();
  _refreshBlendUi();
  _reloadOverlays();
}

function _controllerNodes(slots) {
  const total = slots.length || 1;
  const cx = 500;
  const cy = 500;
  const radius = total > 8 ? 360 : total > 5 ? 335 : 310;
  return slots.map((slot, index) => {
    const angle = (-Math.PI / 2) + ((Math.PI * 2 * index) / total);
    return {
      ...slot,
      x: cx + Math.cos(angle) * radius,
      y: cy + Math.sin(angle) * radius,
      angle,
    };
  });
}

function _linePoint(sourceNode, targetNode, factor) {
  const t = _clamp01(factor);
  return {
    x: sourceNode.x + ((targetNode.x - sourceNode.x) * t),
    y: sourceNode.y + ((targetNode.y - sourceNode.y) * t),
  };
}

function _projectionFactor(point, sourceNode, targetNode) {
  const vx = targetNode.x - sourceNode.x;
  const vy = targetNode.y - sourceNode.y;
  const lenSq = (vx * vx) + (vy * vy) || 1;
  const dot = ((point.x - sourceNode.x) * vx) + ((point.y - sourceNode.y) * vy);
  return _clamp01(dot / lenSq);
}

function _svgPointFromEvent(svg, event) {
  const rect = svg.getBoundingClientRect();
  if (!rect.width || !rect.height) return { x: 0, y: 0 };
  return {
    x: ((event.clientX - rect.left) / rect.width) * 1000,
    y: ((event.clientY - rect.top) / rect.height) * 1000,
  };
}

function _renderBlendController() {
  const nodes = _controllerNodes(_S.activeSlots);
  const sourceNode = nodes.find(node => node.key === _S.activeSeriesKey) || null;
  const targetNode = nodes.find(node => node.key === _S.targetSeriesKey) || null;
  const activeFactor = _clamp01(_S.blendFactor);

  const lines = sourceNode
    ? nodes
        .filter(node => node.key !== sourceNode.key)
        .map(node => {
          const isActive = node.key === _S.targetSeriesKey;
          return `
            <g class="blend-link-group ${isActive ? 'is-active' : ''}" data-target-key="${node.key}">
              <line class="blend-link-hit"
                    x1="${sourceNode.x}" y1="${sourceNode.y}"
                    x2="${node.x}" y2="${node.y}" />
              <line class="blend-link ${isActive ? 'is-active' : ''}"
                    x1="${sourceNode.x}" y1="${sourceNode.y}"
                    x2="${node.x}" y2="${node.y}" />
            </g>`;
        })
        .join('')
    : '';

  const handle = sourceNode && targetNode
    ? (() => {
        const point = _linePoint(sourceNode, targetNode, activeFactor);
        return `
          <g class="blend-handle-wrap" data-target-key="${targetNode.key}">
            <circle class="blend-handle-ring" cx="${point.x}" cy="${point.y}" r="18" />
            <circle class="blend-handle-core" cx="${point.x}" cy="${point.y}" r="8" />
          </g>`;
      })()
    : '';

  const nodeMarkup = nodes.map(node => {
    const isActive = node.key === _S.activeSeriesKey && !_S.activeConnectionKey;
    const isSource = node.key === _S.activeSeriesKey;
    const isTarget = node.key === _S.targetSeriesKey;
    return `
      <g class="blend-node-group ${isSource ? 'is-source' : ''} ${isTarget ? 'is-target' : ''} ${isActive ? 'is-active' : ''}"
         data-node-key="${node.key}">
        <circle class="blend-node"
                cx="${node.x}" cy="${node.y}" r="84" />
        <text class="blend-node-label" x="${node.x}" y="${node.y}">
          ${node.shortLabel}
        </text>
      </g>`;
  }).join('');

  return `
    <svg class="blend-graph" viewBox="0 0 1000 1000" preserveAspectRatio="xMidYMid meet">
      ${lines}
      ${handle}
      ${nodeMarkup}
    </svg>`;
}

function _refreshBlendUi() {
  const host = document.getElementById('blend-controller-host');
  const state = document.getElementById('blend-state');
  if (state) state.textContent = _blendSummary();
  if (host) { host.innerHTML = _renderBlendController(); _bindBlendController(); }
  _refreshSeqRail();
}

function _stopBlendDrag() {
  if (!_blendDrag) return;
  window.removeEventListener('pointermove', _blendDrag.onMove);
  window.removeEventListener('pointerup', _blendDrag.onUp);
  _blendDrag = null;
}

function _finishBlendDrag() {
  if (!_blendDrag) return;
  const targetKey = _blendDrag.targetKey;
  _stopBlendDrag();
  if (_S.targetSeriesKey === targetKey && _S.blendFactor >= 0.995) {
    _S.activeSeriesKey = targetKey;
    _clearBlendTransition();
  }
  _refreshBlendUi();
  _reloadOverlays();
}

function _updateBlendFromPointer(event) {
  if (!_blendDrag) return;
  const svg = _blendDrag.svg;
  const sourceNode = _blendDrag.sourceNode;
  const targetNode = _blendDrag.targetNode;
  if (!svg || !sourceNode || !targetNode) return;
  const point = _svgPointFromEvent(svg, event);
  const factor = _projectionFactor(point, sourceNode, targetNode);
  if (Math.abs(factor - _S.blendFactor) < 0.003) return;
  _S.blendFactor = factor;
  _refreshBlendUi();
  _blendDrag.svg = document.querySelector('#blend-controller-host .blend-graph');
  _reloadOverlays();
}

function _beginBlendDrag(event, targetKey) {
  const svg = document.querySelector('#blend-controller-host .blend-graph');
  const nodes = _controllerNodes(_S.activeSlots);
  const sourceNode = nodes.find(node => node.key === _S.activeSeriesKey) || null;
  const targetNode = nodes.find(node => node.key === targetKey) || null;
  if (!svg || !sourceNode || !targetNode) return;

  _S.targetSeriesKey = targetKey;
  _S.activeConnectionKey = _connectionKey(_S.activeSeriesKey, targetKey);

  const onMove = (moveEvent) => {
    moveEvent.preventDefault();
    _updateBlendFromPointer(moveEvent);
  };
  const onUp = (upEvent) => {
    upEvent.preventDefault();
    _finishBlendDrag();
  };

  _blendDrag = { svg, sourceNode, targetNode, targetKey, onMove, onUp };
  window.addEventListener('pointermove', onMove);
  window.addEventListener('pointerup', onUp, { once: true });
  _updateBlendFromPointer(event);
}

function _bindBlendController() {
  document.querySelectorAll('[data-node-key]').forEach(el => {
    el.addEventListener('click', (event) => {
      event.preventDefault();
      _selectPureSeries(el.dataset.nodeKey);
    });
  });

  document.querySelectorAll('[data-target-key]').forEach(el => {
    el.addEventListener('pointerdown', (event) => {
      event.preventDefault();
      event.stopPropagation();
      _beginBlendDrag(event, el.dataset.targetKey);
    });
  });
}

// ── Sequence rail verticale ───────────────────────────────────
function _renderSeqRail() {
  const activeSlots = _S.activeSlots || [];
  if (!activeSlots.length) return '<div class="seq-rail-empty">—</div>';
  const slotByType = new Map(activeSlots.map(s => [s.sequenceType, s]));
  const seen = new Set();
  const ordered = [];
  for (const type of _seqRailOrder) {
    if (slotByType.has(type) && !seen.has(type)) { ordered.push(type); seen.add(type); }
  }
  for (const slot of activeSlots) {
    if (!seen.has(slot.sequenceType)) { ordered.push(slot.sequenceType); seen.add(slot.sequenceType); }
  }
  return ordered.map(type => {
    const slot = slotByType.get(type);
    const isActive = slot.key === _S.activeSeriesKey;
    const label = slot.shortLabel || type;
    return `<div class="seq-node${isActive ? ' is-active' : ''}" data-seq-key="${slot.key}" data-seq-type="${type}" draggable="true" title="${slot.label || label}"><div class="seq-dot"></div><span class="seq-label">${label}</span></div>`;
  }).join('');
}

function _refreshSeqRail() {
  const host = document.getElementById('seq-rail');
  if (!host) return;
  host.innerHTML = _renderSeqRail();
  _attachSeqRailEvents();
}

function _attachSeqRailEvents() {
  const rail = document.getElementById('seq-rail');
  if (!rail) return;

  rail.querySelectorAll('.seq-node').forEach(node => {
    node.addEventListener('click', () => {
      _selectPureSeries(node.dataset.seqKey);
    });

    node.addEventListener('dragstart', (e) => {
      _seqRailDragType = node.dataset.seqType;
      e.dataTransfer.effectAllowed = 'move';
      setTimeout(() => node.classList.add('dragging-ghost'), 0);
    });

    node.addEventListener('dragend', () => {
      node.classList.remove('dragging-ghost');
      rail.querySelectorAll('.seq-rail-drop-indicator').forEach(el => el.remove());
      _seqRailDragType = null;
    });

    node.addEventListener('dragover', (e) => {
      if (!_seqRailDragType || _seqRailDragType === node.dataset.seqType) return;
      e.preventDefault();
      rail.querySelectorAll('.seq-rail-drop-indicator').forEach(el => el.remove());
      const rect = node.getBoundingClientRect();
      const indicator = document.createElement('div');
      indicator.className = 'seq-rail-drop-indicator';
      node.insertAdjacentElement(e.clientY < rect.top + rect.height / 2 ? 'beforebegin' : 'afterend', indicator);
    });

    node.addEventListener('drop', (e) => {
      if (!_seqRailDragType || _seqRailDragType === node.dataset.seqType) return;
      e.preventDefault();
      const fromType = _seqRailDragType;
      const toType   = node.dataset.seqType;
      const rect     = node.getBoundingClientRect();
      const before   = e.clientY < rect.top + rect.height / 2;

      const work = [..._seqRailOrder];
      const fromIdx = work.indexOf(fromType);
      if (fromIdx >= 0) work.splice(fromIdx, 1); else work.push(fromType);
      const toIdx = work.indexOf(toType);
      work.splice(toIdx >= 0 ? (before ? toIdx : toIdx + 1) : work.length, 0, fromType);
      _seqRailOrder = work;

      _refreshSeqRail();
    });
  });

  rail.addEventListener('dragleave', (e) => {
    if (!rail.contains(e.relatedTarget)) {
      rail.querySelectorAll('.seq-rail-drop-indicator').forEach(el => el.remove());
    }
  });
}

// ── Ricarica overlay su tutti i pannelli ─────────────────────
async function _reloadOverlays() {
  const requestId = ++_loadSeq;
  const overlays = _checkedOverlays();

  if (_S.viewMode === 'grid') {
    if (!_S.nvGrid.size) return;
    _setStatus('Aggiornamento…', 'nv-loading');
    try {
      for (const [key, nv] of _S.nvGrid) {
        const slotKey = key.split('::')[0];
        const slot = _S.activeSlots.find(s => s.key === slotKey);
        if (!slot?.seqPath) continue;
        await GlioViewer.loadInto(nv, slot.seqPath, overlays);
        if (requestId !== _loadSeq) return;
      }
      _restoreGridCrosshair();
      _setStatus('Aggiornato ✓', 'nv-ok');
    } catch(e) {
      if (requestId !== _loadSeq) return;
      _setStatus(e.message, 'nv-err');
    }
    return;
  }

  const current = _currentSlot();
  if (!_S.nv2D || !current?.seqPath) return;
  _setStatus('Loading…', 'nv-loading');
  try {
    _S.nv2D.setSliceType(_S.activeOrientation);
    await GlioViewer.loadInto(_S.nv2D, current.seqPath, overlays);
    if (requestId !== _loadSeq) return;
    _syncWLFromVolume();
    _setStatus('Loaded ✓', 'nv-ok');
    _prefetchAdjacent();
    if ((_S.sessions?.length ?? 0) > 1) _buildTimeline(current.sequenceType);
  } catch(e) {
    if (requestId !== _loadSeq) return;
    _setStatus(e.message, 'nv-err');
  }
}

function _checkedOverlays() {
  return [...document.querySelectorAll('.ov-cb:checked')]
    .map(el => {
      const item = _S.overlayItems[parseInt(el.value)];
      return item ? { url: item.url, label: item.label, name: item.name, colormap: item.colormap || null } : null;
    }).filter(Boolean);
}

function _baselineDate() {
  const patient = _currentPatientData();
  const diagDate = _parseIsoDate(
    patient?.diagnosis_date ||
    patient?.latest_radiotherapy_course?.diagnosis_date
  );
  if (diagDate) return diagDate;
  const sorted = [...(_S.sessions || [])].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));
  return _parseIsoDate(sorted[0]?.study_date) || null;
}

// Calcola posizioni % dei timepoint (stessa logica del render) — riusata dal drag handler
function _scrubberPositions() {
  const sessions = _S.sessions || [];
  if (sessions.length < 2) return [];
  const sorted = [...sessions].sort((a, b) => {
    const ad = a.study_date || '', bd = b.study_date || '';
    if (ad && bd) return ad.localeCompare(bd);
    return (a.session_label || '').localeCompare(b.session_label || '');
  });
  const MIN_POS = 5, MAX_POS = 95, RANGE = MAX_POS - MIN_POS;
  const timestamps = sorted.map(s => { const d = _parseIsoDate(s.study_date); return d ? d.getTime() : null; });
  const validTs = timestamps.filter(t => t != null);
  let positions;
  if (validTs.length === sorted.length && sorted.length > 1) {
    const minT = Math.min(...validTs), maxT = Math.max(...validTs), span = maxT - minT || 1;
    positions = timestamps.map(t => MIN_POS + ((t - minT) / span) * RANGE);
  } else {
    positions = sorted.map((_, i) => MIN_POS + (i / (sorted.length - 1)) * RANGE);
  }
  const baseline = _baselineDate();
  return sorted.map((s, i) => {
    const sDate = _parseIsoDate(s.study_date);
    const days = baseline && sDate ? Math.round((sDate.getTime() - baseline.getTime()) / 86400000) : null;
    return { sid: s.id, pct: positions[i], days };
  });
}

function _renderTimelineScrubberContent() {
  const sessions = _S.sessions || [];
  if (sessions.length < 2) return '';
  const positions = _scrubberPositions();
  const sorted = [...sessions].sort((a, b) => {
    const ad = a.study_date || '', bd = b.study_date || '';
    if (ad && bd) return ad.localeCompare(bd);
    return (a.session_label || '').localeCompare(b.session_label || '');
  });
  const ticks = sorted.map((s, i) => {
    const pos = positions[i];
    const pct = pos.pct.toFixed(2);
    const isActive = s.id === _S.selSid;
    const label = s.study_date ? GlioTwin.fmtDate(s.study_date) : (s.session_label || `T${i}`);
    const title = `${s.session_label}${s.study_date ? ' · ' + GlioTwin.fmtDate(s.study_date) : ''}`;
    const daysLabel = pos.days != null ? `D+${pos.days}` : '';
    return `<div class="tl-tick${isActive ? ' active' : ''}" style="left:${pct}%" data-sid="${s.id}" onclick="_tlScrubNav(${s.id})" title="${title}"><span class="tl-days">${daysLabel}</span><div class="tl-dot"></div><span class="tl-date">${label}</span></div>`;
  }).join('');
  return `<div class="tl-line"></div><div class="tl-cursor" id="tl-cursor"></div><div class="tl-marker-today" id="tl-today" style="display:none"></div><div class="tl-marker-synthetic" id="tl-synthetic" style="display:none"></div><button class="tl-play-btn" id="tl-play-btn" title="Play / Pausa" onclick="_tlPlayToggle()">▶</button>${ticks}`;
}

async function _prefetchAdjacent() {
  const seqType = _currentSlot()?.sequenceType;
  if (!seqType || !_S.selSid || !_S.sessions?.length) return;
  const sorted = [..._S.sessions].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));
  const idx = sorted.findIndex(s => s.id === _S.selSid);
  if (idx < 0) return;
  for (const offset of [-1, 1]) {
    const neighbor = sorted[idx + offset];
    if (!neighbor) continue;
    try {
      const detail = await GlioTwin.fetch(`/api/sessions/${neighbor.id}`);
      const seq = (detail.sequences || []).find(s => s.sequence_type === seqType);
      const path = seq?.processed_path || seq?.raw_path;
      if (path) GlioViewer.prefetch(path);
    } catch(_) {}
  }
}

async function _buildTimeline(seqType) {
  if (_S._tlBuilding || !seqType || !_S.sessions?.length || !_S.nv2D) return;
  const startNv = _S.nv2D;
  _S._tlBuilding = true;
  try {
    const sessions = [..._S.sessions].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));

    // Recupera dettagli sequenze E strutture di tutti i timepoint in parallelo
    const [allDetails, allStructResp] = await Promise.all([
      Promise.all(sessions.map(s => GlioTwin.fetch(`/api/sessions/${s.id}`).catch(() => null))),
      Promise.all(sessions.map(s => GlioTwin.fetch(`/api/sessions/${s.id}/structures`).catch(() => null))),
    ]);
    if (_S.nv2D !== startNv) return;

    // Overlay attualmente selezionati (dalla sessione corrente)
    const checkedItems = [...document.querySelectorAll('.ov-cb:checked')]
      .map(el => _S.overlayItems[parseInt(el.value)]).filter(Boolean);

    const activeSid = _S.selSid;
    const entries = [];
    const overlaysBySid = {};

    for (let i = 0; i < sessions.length; i++) {
      const s = sessions[i];
      const detail    = allDetails[i];
      const structRes = allStructResp[i];

      const seq  = (detail?.sequences || []).find(q => q.sequence_type === seqType);
      const path = seq?.processed_path || seq?.raw_path;
      entries.push({ sessionId: s.id, path, colormap: 'gray', isActive: s.id === activeSid });

      // Strutture: stesso filtro reference_space di mainSelectSes
      const rawStructs = [
        ...(structRes?.computed     || []),
        ...(structRes?.radiological || []),
      ].filter(st => st.mask_path);
      const hasReg  = rawStructs.some(st => st.reference_space === 'registered');
      const space   = hasReg ? 'registered' : 'native';
      const structs = rawStructs.filter(st => {
        const sp = st.reference_space || 'native';
        return space === 'native' ? (sp === 'native' || sp === 'canonical_1mm') : sp === space;
      });

      // Match per ogni overlay attivo: label+model, label+annotator, poi solo label
      overlaysBySid[s.id] = checkedItems.map(ci => {
        const match =
          (ci.model_name && structs.find(st => st.label === ci.label && st.model_name === ci.model_name)) ||
          (ci.annotator  && structs.find(st => st.label === ci.label && st.annotator  === ci.annotator))  ||
          structs.find(st => st.label === ci.label) || null;
        if (!match) return null;
        return { url: _structUrl(match.mask_path, match.label_code), colormap: ci.colormap, label: ci.label };
      }).filter(Boolean);
    }

    // Prefetch blob con indicatore di progresso
    const toPrefetch = entries.filter(e => e.path);
    let loaded = 0;
    _setTlStatus(0, toPrefetch.length);
    await Promise.all(toPrefetch.map(async e => {
      await GlioViewer.prefetch(e.path);
      _setTlStatus(++loaded, toPrefetch.length);
    }));
    if (_S.nv2D !== startNv) return;

    await GlioViewer.buildTimeline(startNv, entries, overlaysBySid);
    _setTlStatus(toPrefetch.length, toPrefetch.length); // conferma "cache ✓"
  } catch(_) { _setTlStatus(0, 0); }
  finally { _S._tlBuilding = false; }
}

function _updateScrubberTick(sid) {
  document.querySelectorAll('#tl-scrubber .tl-tick').forEach(el => {
    el.classList.toggle('active', parseInt(el.dataset.sid) === sid);
  });
  _updateSyntheticMarker(sid);
}

// ── Marker "Oggi" (verde) e "Sintetico" (rosso) ──────────────
function _updateTodayMarker() {
  const el = document.getElementById('tl-today');
  if (!el) return;
  const baseline = _baselineDate();
  const sessions = _S.sessions || [];
  if (!baseline || sessions.length < 2) { el.style.display = 'none'; return; }
  const sorted = [...sessions].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));
  const timestamps = sorted.map(s => _parseIsoDate(s.study_date)?.getTime() ?? null).filter(t => t != null);
  if (timestamps.length < 2) { el.style.display = 'none'; return; }
  const minT = Math.min(...timestamps), maxT = Math.max(...timestamps);
  const todayMs = Date.now();
  const pct = 5 + ((todayMs - minT) / (maxT - minT)) * 90;
  if (pct < -5 || pct > 105) { el.style.display = 'none'; return; }
  const todayDays = Math.round((todayMs - baseline.getTime()) / 86400000);
  el.style.left = Math.max(0, Math.min(100, pct)).toFixed(2) + '%';
  el.style.display = '';
  el.title = `Oggi · D+${todayDays}`;
  el.dataset.label = `D+${todayDays}`;
}

function _updateSyntheticMarker(sid) {
  const el = document.getElementById('tl-synthetic');
  if (!el) return;
  const pos = _scrubberPositions();
  const entry = pos.find(p => p.sid === sid);
  if (!entry) { el.style.display = 'none'; return; }
  el.style.left = entry.pct.toFixed(2) + '%';
  el.style.display = '';
  el.title = `Sintetico · ${entry.days != null ? 'D+' + entry.days : ''}`;
}

// ── Play / Pausa ──────────────────────────────────────────────
// Velocità: giorni simulati al secondo. Le immagini intermedie
// future (da codici di simulazione) avranno la stessa granularità.
const TL_DAYS_PER_SEC = 10;          // 10 giorni/s → 0.1 s/giorno
const TL_MIN_BLEND_MS = 500;         // minimo per gap brevissimi
const TL_MAX_BLEND_MS = 60000;       // massimo per gap lunghissimi
const TL_HOLD_MS      = 700;         // pausa sul frame reale acquisito

let _tlPlaying   = false;
let _tlPlayTimer = null;
let _tlPlayRafId = null;

window._tlPlayToggle = function() {
  if (_tlPlaying) _tlPlayStop(); else _tlPlayStart();
};

function _tlPlayStart() {
  const sorted = [...(_S.sessions || [])].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));
  if (sorted.length < 2) return;
  _tlPlaying = true;
  document.getElementById('tl-play-btn')?.setAttribute('data-playing', '1');
  const btn = document.getElementById('tl-play-btn');
  if (btn) btn.textContent = '⏸';

  let fromIdx = Math.max(0, sorted.findIndex(s => s.id === _S.selSid));

  function _blendStep(toIdx) {
    if (!_tlPlaying) return;
    const sid1 = sorted[fromIdx].id;
    const sid2 = sorted[toIdx].id;
    const pos  = _scrubberPositions();
    const p1   = pos.find(p => p.sid === sid1);
    const p2   = pos.find(p => p.sid === sid2);

    // Durata proporzionale ai giorni reali tra le due sessioni
    const days1    = p1?.days ?? 0;
    const days2    = p2?.days ?? (days1 + 1);
    const deltaDays = Math.max(1, days2 - days1);
    const duration  = Math.min(TL_MAX_BLEND_MS,
                       Math.max(TL_MIN_BLEND_MS,
                         (deltaDays / TL_DAYS_PER_SEC) * 1000));
    const t0 = performance.now();

    function _frame(now) {
      if (!_tlPlaying) return;
      // t lineare: il tempo clinico scorre uniforme giorno per giorno
      const raw = Math.min(1, (now - t0) / duration);

      // Giorno sintetico corrente (intero)
      const dayNow = Math.round(days1 + deltaDays * raw);

      // Aggiorna marker sintetico + cursore con posizione interpolata
      if (p1 && p2) {
        const iPct   = (p1.pct + (p2.pct - p1.pct) * raw).toFixed(2);
        const synEl  = document.getElementById('tl-synthetic');
        const cursor = document.getElementById('tl-cursor');
        if (synEl) {
          synEl.style.left      = iPct + '%';
          synEl.dataset.label   = 'D+' + dayNow;
          synEl.title           = 'Sintetico · D+' + dayNow;
        }
        if (cursor) cursor.style.left = iPct + '%';
      }

      GlioViewer.blendTimeline(_S.nv2D, sid1, sid2, raw);

      if (raw < 1) {
        _tlPlayRafId = requestAnimationFrame(_frame);
      } else {
        _tlPlayRafId = null;
        // Snap sul frame reale
        if (GlioViewer.switchTimeline(_S.nv2D, sid1, sid2)) {
          _S.selSid = sid2;
          GlioTwin.state.currentSession = sid2;
          _updateScrubberTick(sid2);
        }
        document.getElementById('tl-cursor')?.style && (document.getElementById('tl-cursor').style.left = '-99px');
        fromIdx = toIdx;
        _tlPlayTimer = setTimeout(() => {
          const nextIdx = fromIdx < sorted.length - 1 ? fromIdx + 1 : 0;
          _blendStep(nextIdx);
        }, TL_HOLD_MS);
      }
    }
    _tlPlayRafId = requestAnimationFrame(_frame);
  }

  _tlPlayTimer = setTimeout(() => {
    const nextIdx = fromIdx < sorted.length - 1 ? fromIdx + 1 : 0;
    _blendStep(nextIdx);
  }, TL_HOLD_MS);
}

function _tlPlayStop() {
  _tlPlaying = false;
  if (_tlPlayRafId) { cancelAnimationFrame(_tlPlayRafId); _tlPlayRafId = null; }
  clearTimeout(_tlPlayTimer);
  _tlPlayTimer = null;
  document.getElementById('tl-play-btn')?.removeAttribute('data-playing');
  const btn = document.getElementById('tl-play-btn');
  if (btn) btn.textContent = '▶';
  const cursor = document.getElementById('tl-cursor');
  if (cursor) cursor.style.left = '-99px';
}

async function _switchGridTimepoint(sid) {
  if (!_S.nvGrid.size) {
    // Griglia non ancora costruita — path normale
    await window.mainSelectSes(sid);
    return;
  }
  _setStatus('Caricamento…', 'nv-loading');
  try {
    const [sesDetail, structs] = await Promise.all([
      GlioTwin.fetch(`/api/sessions/${sid}`),
      GlioTwin.fetch(`/api/sessions/${sid}/structures`)
        .catch(() => ({ sequences:[], computed:[], radiological:[] })),
    ]);

    const allStructs = [
      ...(structs.computed     || []),
      ...(structs.radiological || []),
    ].filter(s => s.mask_path);
    const hasRegistered = allStructs.some(s => s.reference_space === 'registered');
    const overlaySpace  = hasRegistered ? 'registered' : 'native';
    const filteredStructs = allStructs.filter(s => {
      const sp = s.reference_space || 'native';
      if (overlaySpace === 'native') return sp === 'native' || sp === 'canonical_1mm';
      return sp === overlaySpace;
    });

    _S.selSid        = sid;
    _S.overlaySpace  = overlaySpace;
    _S.allStructsRaw = filteredStructs;
    GlioTwin.state.currentSession = sid;
    _ovRenderList();

    const preferNative = overlaySpace === 'native';
    const newSeqs  = sesDetail.sequences || [];
    const newSlots = _buildViewableSlots(newSeqs, { preferNative });
    const overlays = _checkedOverlays();

    const total = _S.nvGrid.size;
    let loaded  = 0;

    for (const [key, nv] of _S.nvGrid) {
      const slotKey = key.split('::')[0];
      const oldSlot = _S.activeSlots.find(s => s.key === slotKey);
      if (!oldSlot) { loaded++; continue; }
      const newSlot = newSlots.find(s => s.sequenceType === oldSlot.sequenceType);
      if (newSlot?.seqPath) {
        try { await GlioViewer.loadInto(nv, newSlot.seqPath, overlays); } catch(e) {
          console.warn('[gridTP load]', key, e.message);
        }
      }
      loaded++;
      _setTlStatus(loaded, total);
    }

    _S.activeSlots = newSlots;
    _S._focusNvKey = null; // il focus NV è ormai stale
    _setTlStatus(total, total);
    _restoreGridCrosshair(); // mantieni la crosshair al cambio timepoint
    _setStatus('Caricato ✓', 'nv-ok');
    _updateScrubberTick(sid);

    document.querySelectorAll('#session-list .tree-item').forEach(r =>
      r.classList.toggle('selected', parseInt(r.dataset.sid) === sid));
  } catch(e) {
    _setStatus('Errore: ' + e.message, 'nv-err');
  }
}

window._tlScrubNav = async function(sid) {
  sid = parseInt(sid, 10);
  if (sid === _S.selSid || !window.mainSelectSes) return;

  // In modalità griglia: ricarica solo i volumi nelle NV già esistenti (senza ricostruire)
  if (_S.viewMode === 'grid') {
    await _switchGridTimepoint(sid);
    return;
  }

  // Modalità focus — fast path: timeline pronta, swap istantaneo
  if (GlioViewer.switchTimeline(_S.nv2D, _S.selSid, sid)) {
    _S.selSid = sid;
    GlioTwin.state.currentSession = sid;
    _updateScrubberTick(sid);
    return;
  }

  // Se build in corso, aspetta e riprova il fast path
  if (_S._tlBuilding) {
    await new Promise(resolve => {
      const t = setInterval(() => { if (!_S._tlBuilding) { clearInterval(t); resolve(); } }, 80);
      setTimeout(() => { clearInterval(t); resolve(); }, 12000);
    });
    if (GlioViewer.switchTimeline(_S.nv2D, _S.selSid, sid)) {
      _S.selSid = sid;
      GlioTwin.state.currentSession = sid;
      _updateScrubberTick(sid);
      return;
    }
  }

  // Slow path: navigazione completa
  const slot = _currentSlot();
  const seqType = slot?.sequenceType;
  if (seqType) _S._scrubberPreferredSeqType = seqType;
  await window.mainSelectSes(sid);
};

document.addEventListener('keydown', function _tlArrowKey(e) {
  if (e.target.matches('input,textarea,select')) return;

  // Su/Giù → scorri fette
  if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
    const delta = e.key === 'ArrowUp' ? 1 : -1;
    if (_S.viewMode === 'grid' && _S.nvGrid.size > 0) {
      e.preventDefault();
      if (!_S._crosshairLocked) _scrollAllGrid(delta);
    } else if (_S.nv2D) {
      e.preventDefault();
      if (!_S._crosshairLocked) GlioViewer.scrollSlice(_S.nv2D, _S.activeOrientation, delta);
    }
    return;
  }

  // Sinistra/Destra → timepoint precedente/successivo
  if (!_S.selSid || !_S.sessions?.length) return;
  if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
  e.preventDefault();
  const sorted = [..._S.sessions].sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''));
  const idx = sorted.findIndex(s => s.id === _S.selSid);
  if (idx < 0) return;
  const target = e.key === 'ArrowLeft' ? sorted[idx - 1] : sorted[idx + 1];
  if (target) window._tlScrubNav(target.id);
});

function _renderViewerControls() {
  const currentOrientation = String(_S.activeOrientation);
  const orientationOptions = VIEW_MODES.map(mode =>
    `<option value="${mode.sliceType}" ${String(mode.sliceType) === currentOrientation ? 'selected' : ''}>${mode.label}</option>`
  ).join('');

  return `
    <div class="single-viewer-shell">
      <div class="single-viewer-toolbar">
        <button class="viewer-focus-back" id="vg-back-btn" title="Torna alla visualizzazione griglia">← Griglia</button>
        <div class="vg-coord-bar vg-coord-bar--inline" id="sf-coord-bar">
          <button id="sf-lock-btn" class="vg-lock-btn" title="Blocca crosshair">🔓</button>
          <span class="vg-coord-label">Vox</span>
          <span id="sf-coord-xyz">—</span>
        </div>
        <label class="viewer-control">
          <span>Orientamento</span>
          <select id="viewer-orientation" class="viewer-select">
            ${orientationOptions}
          </select>
        </label>
        <label class="viewer-control">
          <span>W</span>
          <input type="range" id="vwr-ww" class="viewer-slider" min="1" max="4000" value="1500" step="1">
          <span id="vwr-ww-val" class="viewer-slider-val">1500</span>
        </label>
        <label class="viewer-control">
          <span>L</span>
          <input type="range" id="vwr-wl" class="viewer-slider" min="-1000" max="3000" value="400" step="1">
          <span id="vwr-wl-val" class="viewer-slider-val">400</span>
        </label>
        <div class="viewer-control viewer-zoom-ctrl">
          <span>Zoom</span>
          <button class="viewer-zoom-btn" id="vwr-zoom-out" title="Zoom out">−</button>
          <span id="vwr-zoom-val" class="viewer-slider-val">1.0×</span>
          <button class="viewer-zoom-btn" id="vwr-zoom-in" title="Zoom in">+</button>
          <button class="viewer-zoom-btn viewer-zoom-reset" id="vwr-zoom-reset" title="Reset zoom">⊙</button>
        </div>
        <div class="viewer-blend-state" id="blend-state">${_blendSummary()}</div>
      </div>
      <div class="single-viewer-layout">
        <aside class="seq-rail" id="seq-rail">${_renderSeqRail()}</aside>
        <div class="single-viewer-stage">
          <canvas id="sgcv-single"></canvas>
          <div class="nv-voxel-readout" id="nv-voxel" title="Intensità voxel sotto il cursore">—</div>
          <div id="vg-focus-lock-overlay" class="vg-lock-overlay"></div>
        </div>
      </div>
      <div class="tl-scrubber tl-scrubber-flow" id="tl-scrubber">${_renderTimelineScrubberContent()}</div>
    </div>`;
}

function _updateZoomDisplay(zoom) {
  const el = document.getElementById('vwr-zoom-val');
  if (el) el.textContent = zoom.toFixed(1) + '×';
}

function _syncWLFromVolume() {
  const range = GlioViewer.getVolumeRange(_S.nv2D);
  if (!range) return;
  const wwEl = document.getElementById('vwr-ww');
  const wlEl = document.getElementById('vwr-wl');
  const wwVal = document.getElementById('vwr-ww-val');
  const wlVal = document.getElementById('vwr-wl-val');
  if (!wwEl || !wlEl) return;

  const w = Math.round(range.width);
  const l = Math.round(range.level);
  const span = Math.round(range.dataMax - range.dataMin) || 4000;

  wwEl.min  = 1;
  wwEl.max  = span;
  wwEl.value = w;
  wlEl.min  = Math.round(range.dataMin - span * 0.1);
  wlEl.max  = Math.round(range.dataMax + span * 0.1);
  wlEl.value = l;
  if (wwVal) wwVal.textContent = w;
  if (wlVal) wlVal.textContent = l;
}

let _scrubDragging = false;
let _blendRafId    = null;
let _blendPending  = null; // {pos, pct} da applicare nel prossimo frame

function _attachScrubberDrag() {
  const scrubber = document.getElementById('tl-scrubber');
  if (!scrubber) return;

  function _pctFromEvent(e) {
    const touch = e.touches?.[0] ?? e;
    const rect  = scrubber.getBoundingClientRect();
    return Math.max(0, Math.min(100, ((touch.clientX - rect.left) / rect.width) * 100));
  }

  function _doBlend(pos, pct) {
    const minP = pos[0].pct, maxP = pos[pos.length - 1].pct;
    const p    = Math.max(minP, Math.min(maxP, pct));
    let left = pos[0], right = pos[1];
    for (let i = 0; i < pos.length - 1; i++) {
      if (p <= pos[i + 1].pct) { left = pos[i]; right = pos[i + 1]; break; }
    }
    const span = right.pct - left.pct;
    const t    = span > 0 ? (p - left.pct) / span : 0;
    GlioViewer.blendTimeline(_S.nv2D, left.sid, right.sid, t);
    const cursor = document.getElementById('tl-cursor');
    if (cursor) cursor.style.left = p.toFixed(2) + '%';
  }

  function _blendAt(pct) {
    const pos = _scrubberPositions();
    if (pos.length < 2) return;
    // Aggiorna subito il cursore visivo per feedback immediato
    const minP = pos[0].pct, maxP = pos[pos.length - 1].pct;
    const p    = Math.max(minP, Math.min(maxP, pct));
    const cursor = document.getElementById('tl-cursor');
    if (cursor) cursor.style.left = p.toFixed(2) + '%';
    // Throttle NiiVue render a un frame al massimo
    _blendPending = { pos, pct };
    if (_blendRafId) return;
    _blendRafId = requestAnimationFrame(() => {
      _blendRafId = null;
      if (!_blendPending || !_scrubDragging) return;
      const { pos: p2, pct: pct2 } = _blendPending;
      _blendPending = null;
      _doBlend(p2, pct2);
    });
  }

  function _snapAt(pct) {
    const pos = _scrubberPositions();
    if (!pos.length) return;
    const nearest = pos.reduce((best, p) =>
      Math.abs(p.pct - pct) < Math.abs(best.pct - pct) ? p : best, pos[0]);
    window._tlScrubNav(nearest.sid);
  }

  function _startDrag(e) {
    if (e.button !== undefined && e.button !== 0) return;
    _scrubDragging = true;
    scrubber.classList.add('dragging');
    _blendAt(_pctFromEvent(e));
    e.preventDefault();
  }
  function _moveDrag(e) {
    if (!_scrubDragging) return;
    _blendAt(_pctFromEvent(e));
  }
  function _endDrag(e) {
    if (!_scrubDragging) return;
    _scrubDragging = false;
    scrubber.classList.remove('dragging');
    const cursor = document.getElementById('tl-cursor');
    if (cursor) cursor.style.left = '-99px';
    _snapAt(_pctFromEvent(e));
  }

  scrubber.addEventListener('mousedown',  _startDrag);
  scrubber.addEventListener('touchstart', _startDrag, { passive: false });
  document.addEventListener('mousemove',  _moveDrag);
  document.addEventListener('touchmove',  _moveDrag, { passive: false });
  document.addEventListener('mouseup',    _endDrag);
  document.addEventListener('touchend',   _endDrag);
}

function _attachViewerExtras() {
  const nv     = _S.nv2D;
  const canvas = document.getElementById('sgcv-single');
  const voxEl  = document.getElementById('nv-voxel');

  GlioViewer.attachZoom(nv, canvas, _updateZoomDisplay);
  GlioViewer.attachVoxelReadout(nv, voxEl);

  const wwEl = document.getElementById('vwr-ww');
  const wlEl = document.getElementById('vwr-wl');
  const wwVal = document.getElementById('vwr-ww-val');
  const wlVal = document.getElementById('vwr-wl-val');

  function _applyWL() {
    const w = parseInt(wwEl.value, 10);
    const l = parseInt(wlEl.value, 10);
    if (wwVal) wwVal.textContent = w;
    if (wlVal) wlVal.textContent = l;
    GlioViewer.applyWL(nv, w, l);
  }
  wwEl?.addEventListener('input', _applyWL);
  wlEl?.addEventListener('input', _applyWL);

  document.getElementById('vwr-zoom-in')?.addEventListener('click', () => {
    GlioViewer.zoomIn(nv, _updateZoomDisplay);
  });
  document.getElementById('vwr-zoom-out')?.addEventListener('click', () => {
    GlioViewer.zoomOut(nv, _updateZoomDisplay);
  });
  document.getElementById('vwr-zoom-reset')?.addEventListener('click', () => {
    GlioViewer.resetZoom(nv, _updateZoomDisplay);
  });

  _attachScrubberDrag();
  _attachSeqRailEvents();
  _updateTodayMarker();
  _updateSyntheticMarker(_S.selSid);
}

// ── Gestione layer griglia / focus ───────────────────────────
function _switchToLayer(showId, hideId) {
  const show = document.getElementById(showId);
  const hide = document.getElementById(hideId);
  if (show) { show.removeAttribute('data-hidden'); }
  if (hide) { hide.setAttribute('data-hidden', ''); }
}

function _getOrCreateViewerLayer(id) {
  let layer = document.getElementById(id);
  if (!layer) {
    const gridEl = document.getElementById('seq-grid');
    if (!gridEl) return null;
    layer = document.createElement('div');
    layer.id = id;
    layer.className = 'viewer-layer';
    layer.setAttribute('data-hidden', '');
    gridEl.appendChild(layer);
  }
  return layer;
}

async function _renderSingleViewer() {
  const gridEl  = document.getElementById('seq-grid');
  const emptyEl = document.getElementById('main-empty');
  if (!gridEl) return;
  if (_S.viewerNotice) {
    gridEl.style.display = 'none';
    if (emptyEl) {
      emptyEl.style.display = 'flex';
      emptyEl.innerHTML = `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1">
          <rect x="2" y="2" width="20" height="20" rx="3"/>
          <path d="M7 7h10M7 12h10M7 17h6"/>
        </svg>
        <p>${_S.viewerNotice}</p>
      `;
    }
    _setStatus('');
    return;
  }
  if (!_S.activeSlots.length) {
    gridEl.style.display = 'none';
    if (emptyEl) emptyEl.style.display = 'flex';
    return;
  }

  const layer = _getOrCreateViewerLayer('vg-focus-layer');
  if (!layer) return;
  layer.innerHTML = _renderViewerControls();

  gridEl.style.display = '';
  if (emptyEl) emptyEl.style.display = 'none';
  _switchToLayer('vg-focus-layer', 'vg-grid-layer');

  const canvas = document.getElementById('sgcv-single');
  _S.nv2D = await GlioViewer.createInstance(canvas, _S.activeOrientation);
  _refreshBlendUi();
  _attachViewerExtras();

  document.getElementById('viewer-orientation')?.addEventListener('change', async (e) => {
    _S.activeOrientation = parseInt(e.target.value, 10);
    _S._focusNvKey = `${_S.activeSeriesKey}::${_S.activeOrientation}`;
    await _reloadOverlays();
  });

  await _reloadOverlays();

  // Crosshair sync e barra coordinate per la vista singola
  if (_S.nv2D) {
    _S.nv2D.onLocationChange = (data) => {
      if (_S._crosshairLocked) {
        if (_S._sharedCrosshairPos && _S.nv2D.scene) {
          _S.nv2D.scene.crosshairPos = [..._S._sharedCrosshairPos];
          _S.nv2D.drawScene?.();
        }
        return;
      }
      _updateFocusCoordBar(data, _S.nv2D);
      if (_S.nv2D.scene?.crosshairPos) {
        _S._sharedCrosshairPos = [..._S.nv2D.scene.crosshairPos];
        _S._sharedCrosshairMM = _crosshairFracToMM(_S.nv2D, _S._sharedCrosshairPos);
      }
    };
  }

  // Applica crosshair condivisa dalla griglia (se disponibile)
  if ((_S._sharedCrosshairMM || _S._sharedCrosshairPos) && _S.nv2D?.scene) {
    const focusPos = _S._sharedCrosshairMM
      ? (_crosshairMMToFrac(_S.nv2D, _S._sharedCrosshairMM) ?? _S._sharedCrosshairPos)
      : _S._sharedCrosshairPos;
    if (focusPos) _S.nv2D.scene.crosshairPos = [...focusPos];
    _S.nv2D.drawScene?.();
  }
  _updateFocusCoordBar(null, _S.nv2D);

  // Wiring lucchetto e overlay focus
  document.getElementById('sf-lock-btn')?.addEventListener('click', () => _setLock(!_S._crosshairLocked));
  document.getElementById('vg-focus-lock-overlay')?.addEventListener('dblclick', _backToGrid);
  _setLock(_S._crosshairLocked);
}

// ── Viewer a griglia (multi-serie) ───────────────────────────
function _renderGridViewer() {
  const gridEl  = document.getElementById('seq-grid');
  const emptyEl = document.getElementById('main-empty');
  if (!gridEl) return;

  if (_S.viewerNotice || !_S.activeSlots.length) {
    gridEl.style.display = 'none';
    if (emptyEl) {
      emptyEl.style.display = 'flex';
      emptyEl.innerHTML = _S.viewerNotice
        ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><rect x="2" y="2" width="20" height="20" rx="3"/><path d="M7 7h10M7 12h10M7 17h6"/></svg><p>${_S.viewerNotice}</p>`
        : '';
    }
    _setStatus('');
    return;
  }

  const layer = _getOrCreateViewerLayer('vg-grid-layer');
  if (!layer) return;

  const colHdrs = VIEW_MODES.map(m =>
    `<div class="viewer-grid-col-hdr">${m.label}</div>`).join('');

  const rowsHtml = _S.activeSlots.map((slot, si) => {
    const label = SERIES_ALIASES[slot.sequenceType] || slot.label || slot.sequenceType;
    const cells = VIEW_MODES.map(mode => {
      const cid = `vgcv-${si}-${mode.sliceType}`;
      const voxOverlay = mode.sliceType === 0
        ? `<div class="vg-voxel-val" id="vgvv-${si}">—</div>`
        : '';
      return `<div class="viewer-cell" data-slot-key="${slot.key}" data-slice-type="${mode.sliceType}"><canvas id="${cid}"></canvas>${voxOverlay}</div>`;
    }).join('');
    return `<div class="viewer-grid-row" data-slot-key="${slot.key}">
      <div class="viewer-series-label">${label}</div>
      ${cells}
    </div>`;
  }).join('');

  layer.innerHTML = `
    <div class="viewer-grid-shell">
      <div class="viewer-grid-col-hdrs">
        <div class="viewer-grid-col-hdr"></div>
        ${colHdrs}
      </div>
      <div class="vg-coord-bar" id="vg-coord-bar">
        <button id="vg-lock-btn" class="vg-lock-btn" title="Blocca crosshair">🔓</button>
        <span class="vg-coord-label">Vox</span>
        <span id="vg-coord-xyz">—</span>
        <span class="vg-coord-divider"></span>
        <span class="vg-coord-label">W</span><span id="vg-ww-val" class="vg-wl-val">—</span>
        <span class="vg-coord-label">L</span><span id="vg-wl-val" class="vg-wl-val">—</span>
        <span class="vg-coord-dot">·</span>
        <span id="vg-zoom-val" class="vg-wl-val">1.0×</span>
        <button id="vg-wlz-reset" class="vg-reset-btn" title="Reset W/L e zoom (doppio-click destro/centro)">↺</button>
      </div>
      <div class="viewer-grid-rows" id="vg-rows">${rowsHtml}<div id="vg-lock-overlay" class="vg-lock-overlay"></div></div>
      <div class="tl-scrubber tl-scrubber-flow" id="tl-scrubber">${_renderTimelineScrubberContent()}</div>
    </div>`;

  gridEl.style.display = '';
  if (emptyEl) emptyEl.style.display = 'none';
  _switchToLayer('vg-grid-layer', 'vg-focus-layer');
}

async function _buildGridInstances() {
  const slots   = _S.activeSlots;
  if (!slots.length) return;
  const overlays = _checkedOverlays();
  const total    = slots.length * VIEW_MODES.length;
  let   created  = 0;
  const allNv    = [];

  // 1. Crea tutte le istanze NiiVue
  for (let si = 0; si < slots.length; si++) {
    const slot = slots[si];
    for (const mode of VIEW_MODES) {
      const cid    = `vgcv-${si}-${mode.sliceType}`;
      const canvas = document.getElementById(cid);
      if (!canvas) continue;
      const nv = await GlioViewer.createInstance(canvas, mode.sliceType);
      if (!nv) continue;
      _S.nvGrid.set(`${slot.key}::${mode.sliceType}`, nv);
      allNv.push(nv);
      created++;
      _setTlStatus(Math.floor(created / 2), total);
    }
  }

  // 2. Crosshair sync al click (senza broadcastTo per evitare cascade)
  _setupGridCrosshairSync();

  // 3. Carica i volumi
  let loaded = 0;
  for (let si = 0; si < slots.length; si++) {
    const slot = slots[si];
    if (!slot.seqPath) continue;
    for (const mode of VIEW_MODES) {
      const nv = _S.nvGrid.get(`${slot.key}::${mode.sliceType}`);
      if (!nv) continue;
      try {
        await GlioViewer.loadInto(nv, slot.seqPath, overlays);
      } catch(e) {
        console.warn('[grid load]', slot.key, e.message);
      }
      loaded++;
      _setTlStatus(Math.floor(total / 2) + Math.floor(loaded / 2), total);
    }
  }
  _setTlStatus(total, total);
  _restoreGridCrosshair(); // ripristina se si ritorna a una sessione già vista
  _initGridWL();           // legge W/L iniziale dal primo volume
  _restoreGridZoom();      // ripristina zoom se già impostato

  _attachGridEvents();
}

function _scrollAllGrid(delta) {
  _gridCrosshairSyncing = true;
  for (const [key, nv] of _S.nvGrid) {
    const sliceType = parseInt(key.split('::').pop(), 10);
    GlioViewer.scrollSlice(nv, sliceType, delta);
  }
  _gridCrosshairSyncing = false;
  const axialEntry = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::0'));
  const refNv = axialEntry?.[1] ?? [..._S.nvGrid.values()][0];
  if (refNv?.scene?.crosshairPos) {
    _S._sharedCrosshairPos = [...refNv.scene.crosshairPos];
    _S._sharedCrosshairMM = _crosshairFracToMM(refNv, _S._sharedCrosshairPos);
    _updateGridCoordBar(refNv, null);
  }
  _updateAllVoxelVals(-1, null);
}

// Aggiorna la barra coordinate sopra la griglia
function _updateGridCoordBar(nv, data) {
  const el = document.getElementById('vg-coord-xyz');
  if (!el) return;
  if (data?.vox) {
    const [i, j, k] = data.vox;
    el.textContent = `X ${i}  Y ${j}  Z ${k}`;
    return;
  }
  // Calcola da crosshairPos + dimensioni volume
  const ref = nv ?? [..._S.nvGrid.values()].find(v => v?.scene?.crosshairPos && v?.volumes?.length);
  if (!ref?.scene?.crosshairPos || !ref.volumes?.length) { el.textContent = '—'; return; }
  const pos  = ref.scene.crosshairPos;
  const dims = ref.volumes[0].hdr?.dims ?? [1, 256, 256, 256];
  el.textContent = `X ${Math.round(pos[0] * (dims[1] - 1))}  Y ${Math.round(pos[1] * (dims[2] - 1))}  Z ${Math.round(pos[2] * (dims[3] - 1))}`;
}

// Legge il valore del voxel dal volume NiiVue alla posizione corrente della crosshair
function _readVoxelAtCrosshair(nv) {
  const vol = nv?.volumes?.[0];
  if (!vol?.img || !nv.scene?.crosshairPos) return null;
  const pos  = nv.scene.crosshairPos;
  const dims = vol.hdr?.dims;
  if (!dims || dims.length < 4) return null;
  const nx = dims[1], ny = dims[2], nz = dims[3];
  const i = Math.round(pos[0] * (nx - 1));
  const j = Math.round(pos[1] * (ny - 1));
  const k = Math.round(pos[2] * (nz - 1));
  if (i < 0 || i >= nx || j < 0 || j >= ny || k < 0 || k >= nz) return null;
  const idx = i + j * nx + k * nx * ny;
  if (idx < 0 || idx >= vol.img.length) return null;
  let raw = vol.img[idx];
  const slope = vol.hdr.scl_slope;
  const inter = vol.hdr.scl_inter;
  if (slope && slope !== 0 && !isNaN(slope)) raw = raw * slope + (inter || 0);
  return raw;
}

// Aggiorna i readout voxel di tutte le serie simultaneamente.
// triggerIdx/triggerData: la serie che ha scatenato l'evento (usa i dati NiiVue diretti, più precisi).
// Per le altre usa lettura diretta dal volume.
function _updateAllVoxelVals(triggerIdx, triggerData) {
  for (let si = 0; si < _S.activeSlots.length; si++) {
    const el = document.getElementById(`vgvv-${si}`);
    if (!el) continue;
    if (si === triggerIdx && triggerData?.values?.length) {
      const v = triggerData.values[0];
      el.textContent = typeof v.value === 'number' ? v.value.toFixed(1) : (v.value ?? '—');
      continue;
    }
    const slot = _S.activeSlots[si];
    const nv   = _S.nvGrid.get(`${slot.key}::0`);
    if (!nv) { el.textContent = '—'; continue; }
    const val = _readVoxelAtCrosshair(nv);
    if (val == null) { el.textContent = '—'; continue; }
    el.textContent = Number.isFinite(val)
      ? (Number.isInteger(val) ? String(val) : val.toFixed(1))
      : '—';
  }
}

// ── W/L e Zoom griglia ──────────────────────────────────────────────────────

function _updateGridWLDisplay() {
  const ww = document.getElementById('vg-ww-val');
  const wl = document.getElementById('vg-wl-val');
  if (ww) ww.textContent = _S._gridWW != null ? Math.round(_S._gridWW) : '—';
  if (wl) wl.textContent = _S._gridWL != null ? Math.round(_S._gridWL) : '—';
}

function _updateGridZoomDisplay() {
  const el = document.getElementById('vg-zoom-val');
  if (el) el.textContent = _S._gridZoom.toFixed(1) + '×';
}

// Applica zoom a tutte le istanze della griglia
function _applyZoomAll(zoom) {
  _S._gridZoom = Math.max(0.1, Math.min(20, zoom));
  for (const nv of _S.nvGrid.values()) {
    if (nv?.scene?.pan2Dxyzmm) {
      nv.scene.pan2Dxyzmm[3] = _S._gridZoom;
      nv.drawScene?.();
    }
  }
  _updateGridZoomDisplay();
}

// Applica W/L a tutte le istanze della griglia (tutti i canali, non solo assiale)
function _applyWLAll(ww, wl) {
  _S._gridWW = Math.max(1, ww);
  _S._gridWL = wl;
  for (const nv of _S.nvGrid.values()) {
    GlioViewer.applyWL(nv, _S._gridWW, _S._gridWL);
  }
  _updateGridWLDisplay();
}

// Legge W/L iniziale dal primo volume della griglia
function _initGridWL() {
  const axialNv = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::0'))?.[1];
  const range   = GlioViewer.getVolumeRange(axialNv);
  if (!range) return;
  // Usa il W/L già salvato se presente (cambio timepoint), altrimenti legge dal volume
  if (_S._gridWW == null) _S._gridWW = range.width;
  if (_S._gridWL == null) _S._gridWL = range.level;
  _S._gridDataRange = range.dataMax - range.dataMin;
  _updateGridWLDisplay();
}

// Ripristina lo zoom salvato su tutte le celle
function _restoreGridZoom() {
  if (_S._gridZoom === 1.0) return;
  for (const nv of _S.nvGrid.values()) {
    if (nv?.scene?.pan2Dxyzmm) nv.scene.pan2Dxyzmm[3] = _S._gridZoom;
  }
  _updateGridZoomDisplay();
}

function _crosshairFracToMM(nv, frac) {
  const vol = nv?.volumes?.[0];
  if (!vol || !Array.isArray(frac)) return null;
  if (typeof vol.convertFrac2MM === 'function') {
    const mm = vol.convertFrac2MM(frac, false);
    if (mm && Number.isFinite(mm[0]) && Number.isFinite(mm[1]) && Number.isFinite(mm[2])) {
      return [mm[0], mm[1], mm[2]];
    }
  }
  return null;
}

function _crosshairMMToFrac(nv, mm) {
  const vol = nv?.volumes?.[0];
  if (!vol || !Array.isArray(mm)) return null;
  if (typeof vol.convertMM2Frac === 'function') {
    const frac = vol.convertMM2Frac(mm, false);
    if (frac && Number.isFinite(frac[0]) && Number.isFinite(frac[1]) && Number.isFinite(frac[2])) {
      return [
        Math.max(0, Math.min(1, frac[0])),
        Math.max(0, Math.min(1, frac[1])),
        Math.max(0, Math.min(1, frac[2])),
      ];
    }
  }
  return null;
}

function _centerPanOnMM(nv, mm, zoom = null) {
  if (!nv?.scene?.pan2Dxyzmm || !mm) return;
  const next = [
    Number.isFinite(mm[0]) ? mm[0] : nv.scene.pan2Dxyzmm[0],
    Number.isFinite(mm[1]) ? mm[1] : nv.scene.pan2Dxyzmm[1],
    Number.isFinite(mm[2]) ? mm[2] : nv.scene.pan2Dxyzmm[2],
    Number.isFinite(zoom) ? zoom : (nv.scene.pan2Dxyzmm[3] ?? _S._gridZoom ?? 1),
  ];
  if (typeof nv.setPan2Dxyzmm === 'function') nv.setPan2Dxyzmm(next);
  else {
    nv.scene.pan2Dxyzmm[0] = next[0];
    nv.scene.pan2Dxyzmm[1] = next[1];
    nv.scene.pan2Dxyzmm[2] = next[2];
    nv.scene.pan2Dxyzmm[3] = next[3];
  }
}

function _sliceDisplayAxes(sliceType) {
  if (sliceType === 2) return { h: 1, v: 2 }; // sagittale: X canvas -> Y mm, Y canvas -> Z mm
  if (sliceType === 1) return { h: 0, v: 2 }; // coronale:  X canvas -> X mm, Y canvas -> Z mm
  return { h: 0, v: 1 };                      // assiale:   X canvas -> X mm, Y canvas -> Y mm
}

function _panOtherSliceFromClick(other, otherSlice, clickMM, triggerPan, triggerSlice, zoom) {
  if (!other?.scene?.pan2Dxyzmm || !clickMM || !triggerPan) return;
  const triggerAxes = _sliceDisplayAxes(triggerSlice);
  const otherAxes = _sliceDisplayAxes(otherSlice);
  const verticalOffsetMM = clickMM[triggerAxes.v] - triggerPan[triggerAxes.v];
  const next = [...other.scene.pan2Dxyzmm];

  // Verticale della croce al centro: centro viewport = coordinata anatomica sull'asse orizzontale.
  next[otherAxes.h] = clickMM[otherAxes.h];
  // Orizzontale della croce alla stessa altezza visiva del click nella vista sorgente.
  next[otherAxes.v] = clickMM[otherAxes.v] - verticalOffsetMM;
  next[3] = Number.isFinite(zoom) ? zoom : (next[3] ?? _S._gridZoom ?? 1);

  if (typeof other.setPan2Dxyzmm === 'function') other.setPan2Dxyzmm(next);
  else {
    other.scene.pan2Dxyzmm[0] = next[0];
    other.scene.pan2Dxyzmm[1] = next[1];
    other.scene.pan2Dxyzmm[2] = next[2];
    other.scene.pan2Dxyzmm[3] = next[3];
  }
}

// Pan sincronizzato su tutte le celle.
// pan2Dxyzmm: [X_world, Y_world, Z_world, zoom]
// Axiale   (sliceType 0): canvas-X → world-X [0], canvas-Y → world-Y [1]
// Coronale (sliceType 1): canvas-X → world-X [0], canvas-Y → world-Z [2]
// Sagittale(sliceType 2): canvas-X → world-Y [1], canvas-Y → world-Z [2]
function _panAll(panX, panY, panZ) {
  for (const [key, nv] of _S.nvGrid) {
    if (!nv?.scene?.pan2Dxyzmm) continue;
    const sliceType = parseInt(key.split('::').pop(), 10);
    if (sliceType === 2) { // sagittale
      nv.scene.pan2Dxyzmm[1] = panX;
      nv.scene.pan2Dxyzmm[2] = panZ;
    } else if (sliceType === 1) { // coronale
      nv.scene.pan2Dxyzmm[0] = panX;
      nv.scene.pan2Dxyzmm[2] = panZ;
    } else { // axiale
      nv.scene.pan2Dxyzmm[0] = panX;
      nv.scene.pan2Dxyzmm[1] = panY;
    }
    nv.drawScene?.();
  }
}

// Reset W/L al valore automatico e zoom a 1×
function _resetWLZoomAll() {
  for (const nv of _S.nvGrid.values()) {
    if (!nv?.volumes?.length) continue;
    const vol     = nv.volumes[0];
    const dataMin = vol.global_min ?? vol.robust_min ?? 0;
    const dataMax = vol.global_max ?? vol.robust_max ?? 1000;
    vol.cal_min   = dataMin;
    vol.cal_max   = dataMax;
    nv.updateGLVolume?.();
    GlioViewer.resetZoom(nv, null);
  }
  _S._gridWW   = null;
  _S._gridWL   = null;
  _S._gridZoom = 1.0;
  _initGridWL();
  _updateGridZoomDisplay();
}

// Gestione mouse drag per W/L (tasto destro) e zoom (tasto centrale)
let _gridDragState = null;

function _attachGridDragInteraction() {
  const rows = document.getElementById('vg-rows');
  if (!rows) return;

  // Blocca il menu contestuale sul tasto destro
  rows.addEventListener('contextmenu', e => e.preventDefault(), { capture: true });

  rows.addEventListener('mousedown', (e) => {
    if (_S.nvGrid.size === 0) return;

    if (e.button === 2) { // Tasto destro → zoom (standard) | Ctrl+destro → W/L
      e.preventDefault();
      e.stopPropagation();
      if (e.ctrlKey) {
        const axialNv = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::0'))?.[1];
        const range   = GlioViewer.getVolumeRange(axialNv);
        _gridDragState = {
          type:      'wl',
          startX:    e.clientX,
          startY:    e.clientY,
          startWW:   _S._gridWW ?? range?.width ?? 1500,
          startWL:   _S._gridWL ?? range?.level ?? 400,
          dataRange: _S._gridDataRange ?? (range ? range.dataMax - range.dataMin : 2000),
        };
        document.body.classList.add('grid-drag-wl');
      } else {
        _gridDragState = {
          type:      'zoom',
          startY:    e.clientY,
          startZoom: _S._gridZoom,
        };
        document.body.classList.add('grid-drag-zoom');
      }
    } else if (e.button === 1) { // Tasto centrale → pan (standard)
      e.preventDefault();
      e.stopPropagation();
      const axialNv = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::0'))?.[1];
      const corNv   = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::1'))?.[1];
      const sagNv   = [..._S.nvGrid.entries()].find(([k]) => k.endsWith('::2'))?.[1];
      const zNv     = corNv ?? sagNv;
      _gridDragState = {
        type:      'pan',
        startX:    e.clientX,
        startY:    e.clientY,
        startPanX: axialNv?.scene?.pan2Dxyzmm?.[0] ?? 0, // asse X world
        startPanY: axialNv?.scene?.pan2Dxyzmm?.[1] ?? 0, // asse Y world (axiale)
        startPanZ: zNv?.scene?.pan2Dxyzmm?.[2]    ?? 0, // asse Z world (coronale/sagittale)
      };
      document.body.classList.add('grid-drag-pan');
    }
  }, { capture: true });

  document.addEventListener('mousemove', (e) => {
    if (!_gridDragState) return;
    if (_gridDragState.type === 'wl') {
      const dx   = e.clientX - _gridDragState.startX;
      const dy   = e.clientY - _gridDragState.startY;
      const sens = Math.max(1, _gridDragState.dataRange) / 400;
      _applyWLAll(
        _gridDragState.startWW + dx * sens,
        _gridDragState.startWL + dy * sens
      );
    } else if (_gridDragState.type === 'zoom') {
      const dy      = e.clientY - _gridDragState.startY;
      const newZoom = _gridDragState.startZoom * Math.pow(2, -dy / 200);
      _applyZoomAll(newZoom);
    } else if (_gridDragState.type === 'pan') {
      const dx    = e.clientX - _gridDragState.startX;
      const dy    = e.clientY - _gridDragState.startY;
      const scale = 1.0 / _S._gridZoom;
      _panAll(
        _gridDragState.startPanX + dx * scale,
        _gridDragState.startPanY - dy * scale,  // Y world (axiale), invertito
        _gridDragState.startPanZ - dy * scale   // Z world (coronale/sagittale), invertito
      );
    }
  });

  document.addEventListener('mouseup', (e) => {
    if (e.button === 1 || e.button === 2) {
      _gridDragState = null;
      document.body.classList.remove('grid-drag-wl', 'grid-drag-zoom', 'grid-drag-pan');
    }
  });
}

// Imposta lo stato del lucchetto crosshair e aggiorna UI su tutti i layer attivi
function _setLock(locked) {
  _S._crosshairLocked = locked;
  const icon  = locked ? '🔒' : '🔓';
  const title = locked ? 'Sblocca crosshair' : 'Blocca crosshair';
  document.querySelectorAll('.vg-lock-btn').forEach(btn => {
    btn.textContent = icon;
    btn.title = title;
  });
  document.getElementById('vg-grid-layer')?.classList.toggle('crosshair-locked', locked);
  document.getElementById('vg-focus-layer')?.classList.toggle('crosshair-locked', locked);
}

// Aggiorna la barra coordinate nella vista singola
function _updateFocusCoordBar(data, nv) {
  const el = document.getElementById('sf-coord-xyz');
  if (!el) return;
  if (data?.vox) {
    const [i, j, k] = data.vox;
    el.textContent = `X ${i}  Y ${j}  Z ${k}`;
    return;
  }
  const ref = nv ?? _S.nv2D;
  if (!ref?.scene?.crosshairPos || !ref.volumes?.length) { el.textContent = '—'; return; }
  const pos  = ref.scene.crosshairPos;
  const dims = ref.volumes[0].hdr?.dims ?? [1, 256, 256, 256];
  el.textContent = `X ${Math.round(pos[0]*(dims[1]-1))}  Y ${Math.round(pos[1]*(dims[2]-1))}  Z ${Math.round(pos[2]*(dims[3]-1))}`;
}

// Quando l'utente clicca in una cella → propaga la crosshair e aggiorna display
function _setupGridCrosshairSync() {
  const entries = [..._S.nvGrid.entries()];
  const allNv   = entries.map(([, nv]) => nv);

  for (const [key, nv] of entries) {
    const parts     = key.split('::');
    const sliceType = parseInt(parts.pop(), 10);
    const slotKey   = parts.join('::');
    const slotIdx   = _S.activeSlots.findIndex(s => s.key === slotKey);

    nv.onLocationChange = (data) => {
      if (_gridCrosshairSyncing) return;
      const pos = nv.scene?.crosshairPos;
      if (!pos) return;
      _S._sharedCrosshairPos = [...pos];
      _S._sharedCrosshairMM = _crosshairFracToMM(nv, pos);

      // Aggiorna barra coordinate
      _updateGridCoordBar(nv, data);

      // Al click (non scroll) sincronizziamo anche il pan:
      // - stesso piano del trigger → copia il pan (stessa vista)
      // - piano diverso           → verticale centrata, orizzontale alla stessa altezza del click
      const syncPan = _crosshairFromClick;
      _crosshairFromClick = false;
      const triggerPan = syncPan && nv.scene?.pan2Dxyzmm ? [...nv.scene.pan2Dxyzmm] : null;
      const triggerZoom = triggerPan?.[3] ?? _S._gridZoom ?? 1;
      const clickMM = syncPan ? _S._sharedCrosshairMM : null;
      if (syncPan && Number.isFinite(triggerZoom)) {
        _S._gridZoom = triggerZoom;
        _updateGridZoomDisplay();
      }

      // Propaga crosshair a tutte le altre celle
      _gridCrosshairSyncing = true;
      for (const [otherKey, other] of entries) {
        if (other === nv || !other.scene) continue;
        const otherPos = clickMM ? (_crosshairMMToFrac(other, clickMM) ?? pos) : pos;
        other.scene.crosshairPos = [...otherPos];
        if (triggerPan && other.scene.pan2Dxyzmm) {
          const otherSlice = parseInt(otherKey.split('::').pop(), 10);
          if (otherSlice === sliceType) {
            // Stesso piano: replica pan e zoom identici
            other.scene.pan2Dxyzmm[0] = triggerPan[0];
            other.scene.pan2Dxyzmm[1] = triggerPan[1];
            other.scene.pan2Dxyzmm[2] = triggerPan[2];
            other.scene.pan2Dxyzmm[3] = triggerZoom;
          } else {
            _panOtherSliceFromClick(
              other,
              otherSlice,
              clickMM ?? _crosshairFracToMM(other, pos),
              triggerPan,
              sliceType,
              triggerZoom
            );
          }
        }
        other.drawScene?.();
      }
      _gridCrosshairSyncing = false;

      // Aggiorna i readout voxel per TUTTE le serie (le altre leggono dal volume direttamente)
      _updateAllVoxelVals(slotIdx, data);
    };
  }
}

// Ripristina la crosshair condivisa su tutte le celle dopo un loadVolumes
function _restoreGridCrosshair() {
  if (!_S._sharedCrosshairPos && !_S._sharedCrosshairMM) return;
  _gridCrosshairSyncing = true;
  for (const nv of _S.nvGrid.values()) {
    if (!nv.scene) continue;
    const pos = _S._sharedCrosshairMM
      ? (_crosshairMMToFrac(nv, _S._sharedCrosshairMM) ?? _S._sharedCrosshairPos)
      : _S._sharedCrosshairPos;
    if (pos) nv.scene.crosshairPos = [...pos];
    nv.drawScene?.();
  }
  _gridCrosshairSyncing = false;
  _updateAllVoxelVals(-1, null);
}

function _attachGridEvents() {
  const rows = document.getElementById('vg-rows');
  if (!rows) return;

  rows.addEventListener('wheel', (e) => {
    if (_S.nvGrid.size === 0) return;
    // Scroll sull'etichetta della serie → scrolla il contenitore, non le fette
    if (e.target.closest('.viewer-series-label')) {
      e.preventDefault();
      rows.scrollTop += e.deltaY;
      return;
    }
    e.preventDefault();
    e.stopPropagation(); // impedisce a NiiVue di gestire autonomamente lo zoom
    if (e.ctrlKey) {
      // Ctrl+scroll → zoom globale
      _applyZoomAll(_S._gridZoom * (e.deltaY > 0 ? 1 / 1.12 : 1.12));
      return;
    }
    if (_S._crosshairLocked) return;
    _scrollAllGrid(e.deltaY > 0 ? -1 : 1);
  }, { passive: false, capture: true });

  // Rileva il primo click di un potenziale doppio-click in fase capture
  // (prima che NiiVue riceva il pointerdown sul canvas e sposti la crosshair)
  let _savedBeforeDblClick = null;
  let _awaitingDbl = false;
  let _dblTimer = null;

  rows.addEventListener('pointerdown', (e) => {
    const cell = e.target.closest('.viewer-cell');
    if (!cell) return;
    // Tasto sinistro su canvas → il prossimo onLocationChange viene da un click utente
    if (e.button === 0) _crosshairFromClick = true;
    if (!_awaitingDbl) {
      // Primo click: salva la posizione attuale prima che NiiVue la cambi
      const nv = _S.nvGrid.get(`${cell.dataset.slotKey}::${parseInt(cell.dataset.sliceType, 10)}`);
      _savedBeforeDblClick = nv?.scene?.crosshairPos ? [...nv.scene.crosshairPos] : null;
      _awaitingDbl = true;
      _dblTimer = setTimeout(() => {
        _awaitingDbl = false;
        _savedBeforeDblClick = null;
      }, 400);
    }
    // Secondo click: non aggiorna _savedBeforeDblClick
  }, { capture: true });

  rows.addEventListener('dblclick', (e) => {
    clearTimeout(_dblTimer);
    _awaitingDbl = false;
    const cell = e.target.closest('.viewer-cell');
    if (!cell) return;
    // Ripristina la crosshair al valore prima del doppio-click
    if (_savedBeforeDblClick) {
      _gridCrosshairSyncing = true;
      for (const nv of _S.nvGrid.values()) {
        if (nv.scene) nv.scene.crosshairPos = [..._savedBeforeDblClick];
      }
      _gridCrosshairSyncing = false;
      _S._sharedCrosshairPos = [..._savedBeforeDblClick];
      const refNv = _S.nvGrid.get(`${cell.dataset.slotKey}::${parseInt(cell.dataset.sliceType, 10)}`);
      _S._sharedCrosshairMM = _crosshairFracToMM(refNv, _S._sharedCrosshairPos);
      _savedBeforeDblClick = null;
    }
    const slotKey   = cell.dataset.slotKey;
    const sliceType = parseInt(cell.dataset.sliceType, 10);
    _enterFocusMode(slotKey, sliceType);
  });

  // Lucchetto crosshair
  document.getElementById('vg-lock-btn')?.addEventListener('click', () => _setLock(!_S._crosshairLocked));
  document.getElementById('vg-wlz-reset')?.addEventListener('click', _resetWLZoomAll);

  // Overlay bloccato: doppio-click trova la cella sotto e apre il focus
  const lockOverlay = document.getElementById('vg-lock-overlay');
  if (lockOverlay) {
    lockOverlay.addEventListener('dblclick', (e) => {
      lockOverlay.style.pointerEvents = 'none';
      const el = document.elementFromPoint(e.clientX, e.clientY);
      lockOverlay.style.pointerEvents = '';
      const cell = el?.closest('.viewer-cell');
      if (!cell) return;
      _enterFocusMode(cell.dataset.slotKey, parseInt(cell.dataset.sliceType, 10));
    });
  }

  _setLock(_S._crosshairLocked);
  _attachGridDragInteraction();
}

function _backToGrid() {
  _S.viewMode = 'grid';
  // Propaga la crosshair del focus NV a tutte le celle della griglia
  if (_S.nv2D?.scene?.crosshairPos) {
    const pos = [..._S.nv2D.scene.crosshairPos];
    _S._sharedCrosshairPos = pos;
    _S._sharedCrosshairMM = _crosshairFracToMM(_S.nv2D, pos);
    _gridCrosshairSyncing = true;
    for (const nv of _S.nvGrid.values()) {
      if (!nv.scene) continue;
      const gridPos = _S._sharedCrosshairMM
        ? (_crosshairMMToFrac(nv, _S._sharedCrosshairMM) ?? pos)
        : pos;
      nv.scene.crosshairPos = [...gridPos];
    }
    _gridCrosshairSyncing = false;
  }
  _switchToLayer('vg-grid-layer', 'vg-focus-layer');
  for (const nv of _S.nvGrid.values()) nv.drawScene?.();
}

async function _enterFocusMode(slotKey, sliceType) {
  const st = Number.isNaN(sliceType) ? VIEW_MODES[0].sliceType : sliceType;
  _S.viewMode           = 'focus';
  _S.focusedSlotKey     = slotKey;
  _S.focusedSliceType   = st;
  _S.activeSeriesKey    = slotKey;
  _S.activeOrientation  = st;

  const focusKey = `${slotKey}::${st}`;

  // Stesso contenuto già caricato → toggle istantaneo
  if (_S.nv2D && _S._focusNvKey === focusKey) {
    // Applica la crosshair condivisa (potrebbe essere cambiata nella griglia)
    if (_S._sharedCrosshairPos && _S.nv2D.scene) {
      _S.nv2D.scene.crosshairPos = [..._S._sharedCrosshairPos];
    }
    _switchToLayer('vg-focus-layer', 'vg-grid-layer');
    _S.nv2D.drawScene?.();
    return;
  }

  _S._focusNvKey = focusKey;
  await _renderSingleViewer();

  document.getElementById('vg-back-btn')?.addEventListener('click', _backToGrid);
  document.getElementById('sgcv-single')?.addEventListener('dblclick', _backToGrid);
}

// ── Costruisce il viewer ─────────────────────────────────────
async function _buildGrid(seqs) {
  _stopBlendDrag();
  _S.lastSeqs = seqs;
  const preferNative = _S.overlaySpace === 'native';
  _S.activeSlots = _buildViewableSlots(seqs, { preferNative });

  const list = Array.isArray(seqs) ? seqs : [];
  const hasAnySequences = list.length > 0;
  const hasAnyViewable = list.some(seq => _isViewablePath(_sequenceViewPath(seq, { preferNative })));
  _S.viewerNotice = hasAnySequences && !hasAnyViewable
    ? 'This session currently contains raw DICOM references only. Run preprocessing or attach NIfTI volumes before opening it in the viewer.'
    : '';

  if (!_S.activeSlots.some(slot => slot.key === _S.activeSeriesKey)) {
    const preferred = _S._scrubberPreferredSeqType
      ? _S.activeSlots.find(s => s.sequenceType === _S._scrubberPreferredSeqType)
      : null;
    _S.activeSeriesKey = preferred ? preferred.key : _preferredSeriesKey(_S.activeSlots);
    _S._scrubberPreferredSeqType = null;
  }
  if (!_S.activeSlots.some(slot => slot.key === _S.targetSeriesKey)) {
    _clearBlendTransition();
  }
  if (!VIEW_MODES.some(mode => mode.sliceType === _S.activeOrientation)) {
    _S.activeOrientation = VIEW_MODES[0].sliceType;
  }

  if (_S.viewMode === 'focus') {
    await _renderSingleViewer();
  } else {
    _S.viewMode = 'grid';
    _renderGridViewer();
    _attachScrubberDrag();
    _updateTodayMarker();
    _updateSyntheticMarker(_S.selSid);
    await _buildGridInstances();
  }
}

function _timelineDateLabel(point) {
  return `RM ${GlioTwin.fmtDate(point.study_date, point.session_label || 'Session')}`;
}

function _friendlyEventTypeLabel(eventType) {
  const map = {
    diagnosis: 'Diagnosis',
    surgery: 'Surgery',
    radiotherapy_start: 'RT Start',
    radiotherapy_end: 'RT End',
    chemotherapy_start: 'CT Start',
    chemotherapy_end: 'CT End',
    response_assessment: 'Response',
    progression: 'Progression',
    death: 'Death',
    other: 'Event',
  };
  return map[eventType] || String(eventType || 'Event').replaceAll('_', ' ');
}

function _formatMetricValue(value) {
  if (!Number.isFinite(value)) return '—';
  return Math.abs(value) >= 100 ? value.toFixed(1) : value.toFixed(2);
}

function _disposeSignalCharts() {
  for (const chart of _signalChartInstances) {
    try { chart.dispose(); } catch (_) {}
  }
  _signalChartInstances = [];
}

function _chartCard({ containerId, title, emptyMessage = 'No compatible NIfTI data available for this metric.' }) {
  return `
    <div class="signal-chart-wrap">
      <div class="signal-chart-title">${title}</div>
      <div class="signal-chart-host" id="${containerId}"></div>
      <div class="signal-empty" id="${containerId}-empty" style="display:none">${emptyMessage}</div>
    </div>
  `;
}

function _buildEventMarkers(events = []) {
  return events
    .map((event) => ({ ...event, _dt: _eventTemporalDate(event) }))
    .filter((event) => event._dt)
    .map((event) => ({
      xAxis: event._dt.getTime(),
      lineStyle: {
        color: '#f59e0b',
        width: 2,
        type: 'dashed',
      },
      label: {
        show: true,
        formatter: `${_friendlyEventTypeLabel(event.event_type)}\n${GlioTwin.fmtDate(event.event_date)}`,
        color: '#fbbf24',
        fontWeight: 700,
        position: 'insideEndTop',
        distance: 6,
      },
    }));
}

function _renderEChart({ containerId, points, events = [], title, yLabel, valueKey, lowerKey = null, upperKey = null }) {
  const host = document.getElementById(containerId);
  const empty = document.getElementById(`${containerId}-empty`);
  if (!host || !empty) return;
  if (!points.length) {
    host.style.display = 'none';
    empty.style.display = 'block';
    return;
  }
  host.style.display = 'block';
  empty.style.display = 'none';
  if (typeof echarts === 'undefined') {
    host.style.display = 'none';
    empty.textContent = 'ECharts not available.';
    empty.style.display = 'block';
    return;
  }

  const seriesData = points
    .map((point) => {
      const dt = _pointTemporalDate(point);
      const value = point[valueKey];
      if (!dt || !Number.isFinite(value)) return null;
      return {
        value: [dt.getTime(), value],
        point,
      };
    })
    .filter(Boolean);
  const eventTimes = events
    .map((event) => _eventTemporalDate(event))
    .filter(Boolean)
    .map((dt) => dt.getTime());
  const pointTimes = seriesData.map((item) => item.value[0]);
  const allTimes = [...pointTimes, ...eventTimes].sort((a, b) => a - b);
  const minTime = allTimes.length ? allTimes[0] : null;
  const maxTime = allTimes.length ? allTimes[allTimes.length - 1] : null;

  const upperData = upperKey
    ? points.map((point) => {
        const dt = _pointTemporalDate(point);
        const value = point[upperKey];
        if (!dt || !Number.isFinite(value)) return null;
        return [dt.getTime(), value];
      }).filter(Boolean)
    : [];
  const lowerData = lowerKey
    ? points.map((point) => {
        const dt = _pointTemporalDate(point);
        const value = point[lowerKey];
        if (!dt || !Number.isFinite(value)) return null;
        return [dt.getTime(), value];
      }).filter(Boolean)
    : [];

  const chart = echarts.init(host, null, { renderer: 'canvas' });
  _signalChartInstances.push(chart);
  const option = {
    animation: false,
    backgroundColor: 'transparent',
    grid: { left: 58, right: 20, top: 40, bottom: 48 },
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(11,13,15,0.95)',
      borderColor: '#2a3342',
      textStyle: { color: '#e5edf7' },
      formatter: (items) => {
        const first = Array.isArray(items) ? items[0] : items;
        const point = first?.data?.point || null;
        if (!point) return '';
        const lines = [
          `<strong>RM ${GlioTwin.fmtDate(point.study_date, point.session_label || 'Session')}</strong>`,
          `${title}: ${_formatMetricValue(point[valueKey])}`,
        ];
        if (lowerKey && upperKey) {
          lines.push(`IQR: ${_formatMetricValue(point[lowerKey])} - ${_formatMetricValue(point[upperKey])}`);
        }
        return lines.join('<br>');
      },
    },
    xAxis: {
      type: 'time',
      min: minTime != null ? minTime : undefined,
      max: maxTime != null ? maxTime : undefined,
      axisLabel: {
        color: '#9aa6b8',
        formatter: (value) => echarts.format.formatTime('dd-MM-yyyy', value),
      },
      axisLine: { lineStyle: { color: '#445063' } },
      splitLine: { lineStyle: { color: 'rgba(154,166,184,0.14)' } },
    },
    yAxis: {
      type: 'value',
      name: yLabel,
      nameTextStyle: { color: '#9aa6b8', padding: [0, 0, 0, -8] },
      axisLabel: { color: '#9aa6b8' },
      axisLine: { lineStyle: { color: '#445063' } },
      splitLine: { lineStyle: { color: 'rgba(154,166,184,0.14)' } },
    },
    series: [
      ...(upperKey && lowerKey ? [
        {
          type: 'line',
          data: upperData,
          lineStyle: { opacity: 0 },
          symbol: 'none',
          stack: 'iqr',
          areaStyle: { color: 'rgba(59,126,248,0.14)' },
        },
        {
          type: 'line',
          data: lowerData.map((item, idx) => [item[0], upperData[idx][1] - item[1]]),
          lineStyle: { opacity: 0 },
          symbol: 'none',
          stack: 'iqr',
          areaStyle: { color: 'rgba(59,126,248,0.14)' },
        },
      ] : []),
      {
        name: title,
        type: 'line',
        smooth: false,
        symbol: 'circle',
        symbolSize: 8,
        itemStyle: { color: '#3b7ef8', borderColor: '#0f1623', borderWidth: 2 },
        lineStyle: { color: '#3b7ef8', width: 3 },
        label: {
          show: true,
          position: 'top',
          color: '#e5edf7',
          fontWeight: 700,
          formatter: (item) => _formatMetricValue(item.data.value[1]),
        },
        data: seriesData,
        markLine: {
          symbol: ['none', 'none'],
          silent: true,
          data: _buildEventMarkers(events),
        },
      },
    ],
  };
  chart.setOption(option);
  chart.resize();
}

function _renderSignalPanel() {
  const host = document.getElementById('signal-panel');
  if (!host) return;
  _disposeSignalCharts();
  const timeline = _S.signalTimeline;
  if (!_S.selPid) {
    host.innerHTML = '';
    return;
  }
  if (_S.signalTimelineLoading) {
    host.innerHTML = `
      <div class="signal-loading-card">
        <div class="spinner signal-spinner"></div>
        <div>
          <div class="signal-loading-title">Computing longitudinal metrics…</div>
          <div class="signal-loading-copy">Sto calcolando volume e statistiche voxel sulle strutture dei timepoint disponibili. Su casi con NIfTI grandi puo richiedere alcuni secondi.</div>
        </div>
      </div>
    `;
    return;
  }
  const latestJob = _S.signalMetricStatus?.latest_job || timeline?.cache_status?.latest_job || null;
  const cachedRows = _S.signalMetricStatus?.cached_rows ?? timeline?.cache_status?.cached_rows ?? 0;
  const latestJobStatus = latestJob?.status || 'idle';
  const isMetricJobRunning = latestJobStatus === 'queued' || latestJobStatus === 'running';
  if (!timeline) {
    _S.signalTimeline = {
      available_labels: _S.signalOptions.labels || [],
      available_sequence_types: _S.signalOptions.sequenceTypes || [],
      available_sources: _S.signalOptions.sources || ['preferred', 'radiological', 'computed'],
      selected_label: _S.selectedSignalLabel,
      selected_sequence_type: _S.selectedSignalSequence,
      selected_source: _S.selectedSignalSource,
      points: [],
      clinical_events: [],
    };
  }
  const activeTimeline = _S.signalTimeline;
  const points = (activeTimeline.points || []).filter((point) => point.signal && Number.isFinite(point.signal.median));
  const labels = activeTimeline.available_labels || _S.signalOptions.labels || [];
  const sequences = activeTimeline.available_sequence_types || _S.signalOptions.sequenceTypes || [];
  const sources = activeTimeline.available_sources || ['preferred', 'radiological', 'computed'];
  const pointCount = (activeTimeline.points || []).length;
  const validSignalCount = points.length;
  const failedCount = Math.max(0, pointCount - validSignalCount);
  const volumePoints = points.filter((point) => Number.isFinite(point.volume_ml));
  const signalPoints = points.map((point) => ({
    ...point,
    median: point.signal.median,
    q1: point.signal.q1,
    q3: point.signal.q3,
  }));
  const jobProgress = isMetricJobRunning && latestJob?.total_tasks > 0
    ? `${latestJob.completed_tasks ?? 0} / ${latestJob.total_tasks}`
    : null;
  const lastJobSummary = latestJob && !isMetricJobRunning
    ? (latestJob.status === 'completed'
        ? `Ultimo job #${latestJob.id}: ${latestJob.completed_tasks ?? 0} calcolati${latestJob.failed_tasks > 0 ? `, ${latestJob.failed_tasks} vuoti` : ''}`
        : latestJob.status === 'failed'
          ? `Ultimo job #${latestJob.id}: errore — ${latestJob.error_message || 'sconosciuto'}`
          : null)
    : null;

  host.innerHTML = `
    <div class="signal-panel-head">
      <div>
        <div class="signal-panel-title">Longitudinal Metrics</div>
        <div class="signal-panel-sub">Metriche precomputate globalmente per tutte le strutture, serie e sorgenti. I punti indicano acquisizioni RM reali; le linee verticali rappresentano eventi clinici datati.</div>
        <div class="signal-panel-status">
          <span class="signal-status-chip ${validSignalCount ? 'ok' : 'warn'}">${validSignalCount} timepoint pronti</span>
          <span class="signal-status-chip">${pointCount} considerati</span>
          ${failedCount ? `<span class="signal-status-chip warn">${failedCount} senza segnale compatibile</span>` : ''}
          <span class="signal-status-chip">${cachedRows} righe in cache</span>
          ${isMetricJobRunning ? `<span class="signal-status-chip warn">Calcolo in corso… ${jobProgress ? `(${jobProgress})` : ''}</span>` : ''}
          ${lastJobSummary ? `<span class="signal-status-chip">${lastJobSummary}</span>` : ''}
        </div>
      </div>
      <div class="signal-panel-actions">
        <button class="btn signal-calc-btn ${isMetricJobRunning ? 'signal-calc-btn--running' : ''}" id="signal-precompute-btn" ${isMetricJobRunning ? 'disabled' : ''} title="Calcola le metriche mancanti per tutte le serie e strutture di tutti i pazienti">
          ${isMetricJobRunning
            ? `<span class="signal-calc-spinner"></span> Calcolo in corso${jobProgress ? ` (${jobProgress})` : ''}…`
            : 'Aggiorna metriche globali'}
        </button>
        <button class="btn btn-secondary signal-calc-btn-force" id="signal-precompute-force-btn" ${isMetricJobRunning ? 'disabled' : ''} title="Ricalcola tutte le metriche da zero, sovrascrivendo la cache esistente">
          Ricalcola tutto
        </button>
      </div>
    </div>
    <div class="signal-metric-section">
      <div class="signal-metric-head">
        <div>
          <div class="signal-chart-title">Volume</div>
          <div class="signal-metric-sub">Andamento del volume per la struttura selezionata.</div>
        </div>
        <div class="signal-panel-controls">
          <label>
            <span>Struttura</span>
            <select id="signal-label" onchange="mainSignalSelectionChanged('label', this.value)" oninput="mainSignalSelectionChanged('label', this.value)">
              ${labels.map((item) => `<option value="${item}" ${item === _S.selectedSignalLabel ? 'selected' : ''}>${_friendlyStructureLabel(item)}</option>`).join('')}
            </select>
          </label>
          <label>
            <span>Sorgente</span>
            <select id="signal-source" onchange="mainSignalSelectionChanged('source', this.value)" oninput="mainSignalSelectionChanged('source', this.value)">
              ${sources.map((item) => `<option value="${item}" ${item === _S.selectedSignalSource ? 'selected' : ''}>${item}</option>`).join('')}
            </select>
          </label>
        </div>
      </div>
      ${_chartCard({
        containerId: 'signal-volume-chart',
        title: 'Structure Volume',
      })}
    </div>
    <div class="signal-metric-section">
      <div class="signal-metric-head">
        <div>
          <div class="signal-chart-title">Signal</div>
          <div class="signal-metric-sub">Segnale voxel nella struttura selezionata, misurato sulla serie scelta.</div>
        </div>
        <div class="signal-panel-controls">
          <label>
            <span>Signal Series</span>
            <select id="signal-sequence" onchange="mainSignalSelectionChanged('sequence', this.value)" oninput="mainSignalSelectionChanged('sequence', this.value)">
              ${sequences.map((item) => `<option value="${item}" ${item === _S.selectedSignalSequence ? 'selected' : ''}>${GlioTwin.friendlySequenceType(item)}</option>`).join('')}
            </select>
          </label>
        </div>
      </div>
      ${_chartCard({
        containerId: 'signal-series-chart',
        title: 'Voxel Signal Median + IQR',
      })}
    </div>
  `;
  _renderEChart({
    containerId: 'signal-volume-chart',
    points: volumePoints,
    events: activeTimeline.clinical_events || [],
    valueKey: 'volume_ml',
    yLabel: 'mL',
    title: 'Structure Volume',
  });
  _renderEChart({
    containerId: 'signal-series-chart',
    points: signalPoints,
    events: activeTimeline.clinical_events || [],
    valueKey: 'median',
    lowerKey: 'q1',
    upperKey: 'q3',
    yLabel: 'signal',
    title: 'Voxel Signal Median + IQR',
  });
  host.querySelector('#signal-precompute-btn')?.addEventListener('click', async () => {
    await _queueSignalMetricJob(false);
  });
  host.querySelector('#signal-precompute-force-btn')?.addEventListener('click', async () => {
    await _queueSignalMetricJob(true);
  });
}

function _renderPatientEditorPanel() {
  const host = document.getElementById('patient-editor-panel');
  if (!host) return;
  const patient = _currentPatientData();
  if (!patient) {
    host.innerHTML = '';
    return;
  }
  const latestRt = patient.latest_radiotherapy_course || {};
  const refs = patient.external_ref_map || {};
  const diagnosisDate = patient.diagnosis_date || latestRt.diagnosis_date || null;
  const fmtDay = (value) => value == null || value === '' ? '—' : `D+${value}`;
  const parseIso = (value) => {
    if (!value) return null;
    const dt = new Date(value);
    return Number.isNaN(dt.getTime()) ? null : dt;
  };
  const diffDays = (startValue, endValue) => {
    const start = parseIso(startValue);
    const end = parseIso(endValue);
    if (!start || !end) return null;
    return Math.round((end - start) / 86400000);
  };
  const relativeDays = (event) => {
    if (event.days_from_baseline != null && event.days_from_baseline !== '') return Number(event.days_from_baseline);
    return diffDays(diagnosisDate, event.event_date);
  };
  const eventLabel = (type) => ({
    imaging_mr: 'MR',
    imaging_pet: 'PET',
    imaging_other: 'Imaging',
    diagnosis: 'Diagnosis',
    surgery: 'Surgery',
    radiotherapy: 'Radiotherapy',
    radiotherapy_start: 'Radiotherapy start',
    radiotherapy_end: 'Radiotherapy end',
    chemotherapy_start: 'Chemotherapy start',
    chemotherapy_end: 'Chemotherapy end',
    progression: 'Progression',
    death: 'Death',
    other: 'Other',
  }[type] || String(type || 'event').replaceAll('_', ' '));
  const imagingLabel = (session) => {
    const hay = `${session.session_label || ''} ${session.timepoint_type || ''}`.toLowerCase();
    if (hay.includes('pet')) return 'imaging_pet';
    if (hay.includes('mr') || hay.includes('mri') || hay.includes('timepoint')) return 'imaging_mr';
    if (['pre_op','post_op','follow_up','recurrence','other'].includes(session.timepoint_type)) return 'imaging_mr';
    return 'imaging_other';
  };
  const buildEventRows = () => {
    const items = [];
    const rows = [...(patient.clinical_events || [])];
    const used = new Set();
    const firstUnused = (type) => rows.find((row, idx) => !used.has(idx) && row.event_type === type);
    const markUsed = (row) => {
      const idx = rows.indexOf(row);
      if (idx >= 0) used.add(idx);
    };
    if (!rows.some((row) => row.event_type === 'diagnosis') && diagnosisDate) {
      rows.push({
        event_type: 'diagnosis',
        event_date: diagnosisDate,
        days_from_baseline: 0,
        description: 'Derived from diagnosis date',
      });
    }
    const pushCombinedTherapy = ({
      title,
      tone,
      startRow,
      endRow,
      fallbackStartDate = null,
      fallbackEndDate = null,
      extraDetails = [],
    }) => {
      const startDate = startRow?.event_date || fallbackStartDate || null;
      const endDate = endRow?.event_date || fallbackEndDate || null;
      const startRel = startRow ? relativeDays(startRow) : diffDays(diagnosisDate, startDate);
      const endRel = endRow ? relativeDays(endRow) : diffDays(diagnosisDate, endDate);
      const agent = startRow?.treatment_agent || endRow?.treatment_agent || null;
      if (startRow) markUsed(startRow);
      if (endRow) markUsed(endRow);
      items.push({
        sortDate: parseIso(startDate)?.getTime() ?? null,
        sortDay: startRel,
        title,
        subtitle: [
          startDate ? GlioTwin.fmtDate(startDate) : null,
          startRel != null ? `D+${startRel}` : null,
          endDate ? `→ ${GlioTwin.fmtDate(endDate)}` : null,
          agent || null,
        ].filter(Boolean).join(' · ') || 'Evento temporale senza ancoraggio',
        details: [
          ['Start date', startDate ? GlioTwin.fmtDate(startDate) : '—'],
          ['End date', endDate ? GlioTwin.fmtDate(endDate) : '—'],
          ['Start day from diagnosis', fmtDay(startRel)],
          ['End day from diagnosis', fmtDay(endRel)],
          ['Treatment / agent', agent || '—'],
          ...extraDetails,
          ['Description', startRow?.description || endRow?.description || '—'],
        ],
        tone,
      });
    };
    const rtStart = firstUnused('radiotherapy_start');
    const rtEnd = firstUnused('radiotherapy_end');
    if (rtStart || rtEnd || latestRt.start_date || latestRt.end_date || latestRt.fractions_count != null || latestRt.dose) {
      pushCombinedTherapy({
        title: 'Radiotherapy',
        tone: 'radiotherapy',
        startRow: rtStart,
        endRow: rtEnd,
        fallbackStartDate: latestRt.start_date,
        fallbackEndDate: latestRt.end_date,
        extraDetails: [
          ['Prescription dose', latestRt.dose || latestRt.description || '—'],
          ['Fractions', latestRt.fractions_count ?? '—'],
          ['Course ID', latestRt.external_course_id || refs.ida || '—'],
        ],
      });
    }
    const chemoStart = firstUnused('chemotherapy_start');
    const chemoEnd = firstUnused('chemotherapy_end');
    if (chemoStart || chemoEnd) {
      pushCombinedTherapy({
        title: 'Chemotherapy',
        tone: 'chemotherapy_start',
        startRow: chemoStart,
        endRow: chemoEnd,
      });
    }
    rows.forEach((row, idx) => {
      if (used.has(idx)) return;
      const rel = relativeDays(row);
      items.push({
        sortDate: parseIso(row.event_date)?.getTime() ?? null,
        sortDay: rel,
        title: eventLabel(row.event_type),
        subtitle: [
          row.event_date ? GlioTwin.fmtDate(row.event_date) : null,
          rel != null ? `D+${rel}` : null,
          row.treatment_agent || null,
        ].filter(Boolean).join(' · ') || 'Evento temporale senza ancoraggio',
        details: [
          ['Event type', eventLabel(row.event_type)],
          ['Absolute date', row.event_date ? GlioTwin.fmtDate(row.event_date) : '—'],
          ['Days from diagnosis', fmtDay(rel)],
          ['Treatment / agent', row.treatment_agent || '—'],
          ['RANO response', row.rano_response || '—'],
          ['Session link', row.session_id || '—'],
          ['Description', row.description || '—'],
        ],
        tone: row.event_type || 'other',
      });
    });
    (_S.sessions || []).forEach((session) => {
      const rel = session.days_from_baseline != null && session.days_from_baseline !== '' ? Number(session.days_from_baseline) : diffDays(diagnosisDate, session.study_date);
      const tone = imagingLabel(session);
      items.push({
        sortDate: parseIso(session.study_date)?.getTime() ?? null,
        sortDay: rel,
        title: eventLabel(tone),
        subtitle: [
          session.session_label || null,
          session.study_date ? GlioTwin.fmtDate(session.study_date) : null,
          rel != null ? `D+${rel}` : null,
        ].filter(Boolean).join(' · ') || 'Esame di imaging',
        details: [
          ['Imaging exam', session.session_label || '—'],
          ['Modality', eventLabel(tone)],
          ['Study date', session.study_date ? GlioTwin.fmtDate(session.study_date) : '—'],
          ['Days from diagnosis', fmtDay(rel)],
          ['Timepoint type', session.timepoint_type || '—'],
          ['Sequences', session.n_sequences != null ? String(session.n_sequences) : '—'],
        ],
        tone,
      });
    });
    items.sort((a, b) => {
      const ad = Number.isFinite(a.sortDay) ? a.sortDay : (a.sortDate ?? Number.MAX_SAFE_INTEGER);
      const bd = Number.isFinite(b.sortDay) ? b.sortDay : (b.sortDate ?? Number.MAX_SAFE_INTEGER);
      return ad - bd;
    });
    return items;
  };
  const eventItems = buildEventRows();
  const overview = [
    ['Patient', GlioTwin.patientPrimary(patient)],
    ['Dataset', GlioTwin.humanizeDataset(patient.dataset)],
    ['Diagnosis', patient.diagnosis || '—'],
    ['Diagnosis date', GlioTwin.fmtDate(patient.diagnosis_date)],
    ['Age at diagnosis', patient.age_at_diagnosis != null ? `${Number(patient.age_at_diagnosis).toFixed(0)} yr` : '—'],
    ['Sex', patient.sex || '—'],
    ['Birth date', GlioTwin.fmtDate(patient.patient_birth_date)],
    ['Outcome', patient.vital_status ? patient.vital_status.replaceAll('_', ' ') : '—'],
    ['OS', patient.os_days != null ? `${patient.os_days} d` : '—'],
    ['IDH', patient.idh_status || '—'],
    ['MGMT', patient.mgmt_status || '—'],
    ['IDA', refs.ida || latestRt.external_course_id || '—'],
    ['Tax code', refs.tax_code || latestRt.tax_code || '—'],
    ['Notes', patient.notes || '—'],
  ];

  host.innerHTML = `
    <div class="signal-panel-head">
      <div>
        <div class="signal-panel-title">Patient Data</div>
        <div class="signal-panel-sub">Overview clinica e dati globali del soggetto. Gli eventi sotto sono navigabili ed espandibili con dettaglio assoluto e relativo quando disponibile.</div>
      </div>
    </div>
    <div class="patient-data-layout">
      <section class="patient-overview-card">
        <div class="patient-section-title">Overview & Global Data</div>
        <div class="patient-overview-grid">
          ${overview.map(([label, value]) => `
            <div class="patient-overview-item">
              <span class="patient-overview-label">${label}</span>
              <strong class="patient-overview-value">${GlioTwin.fmt(value, '—')}</strong>
            </div>
          `).join('')}
        </div>
      </section>
      <section class="patient-events-card">
        <div class="patient-section-title">Clinical Events Timeline</div>
        <div class="patient-events-list">
          ${eventItems.length ? eventItems.map((item, idx) => `
            <details class="patient-event ${item.tone || 'other'}" ${idx === 0 ? 'open' : ''}>
              <summary class="patient-event-summary">
                <span class="patient-event-main">
                  <strong>${item.title}</strong>
                  <small>${item.subtitle || 'Evento temporale senza ancoraggio'}</small>
                </span>
              </summary>
              <div class="patient-event-body">
                <div class="patient-event-grid">
                  ${item.details.map(([label, value]) => `
                    <div class="patient-event-row">
                      <span class="patient-event-label">${label}</span>
                      <strong class="patient-event-value">${GlioTwin.fmt(value, '—')}</strong>
                    </div>
                  `).join('')}
                </div>
              </div>
            </details>
          `).join('') : '<div class="signal-empty">No clinical events available.</div>'}
        </div>
      </section>
    </div>
  `;
}

async function _submitPatientEditor(event) {
  event.preventDefault();
  if (!_S.selPid) return;
  const form = event.currentTarget;
  const data = new FormData(form);
  const payload = {
    patient_name: data.get('patient_name') || null,
    patient_given_name: data.get('patient_given_name') || null,
    patient_family_name: data.get('patient_family_name') || null,
    patient_birth_date: data.get('patient_birth_date') || null,
    sex: data.get('sex') || null,
    diagnosis: data.get('diagnosis') || null,
    diagnosis_date: data.get('diagnosis_date') || null,
    death_date: data.get('death_date') || null,
    idh_status: data.get('idh_status') || null,
    mgmt_status: data.get('mgmt_status') || null,
    age_at_diagnosis: data.get('age_at_diagnosis') ? Number(data.get('age_at_diagnosis')) : null,
    os_days: data.get('os_days') ? Number(data.get('os_days')) : null,
    vital_status: data.get('vital_status') || null,
    ida: data.get('ida') || null,
    tax_code: data.get('tax_code') || null,
    radiotherapy_start_date: data.get('radiotherapy_start_date') || null,
    fractions_count: data.get('fractions_count') ? Number(data.get('fractions_count')) : null,
    notes: data.get('notes') || null,
  };
  try {
    const updated = await GlioTwin.put(`/api/patients/${_S.selPid}`, payload);
    _S.patientDetail = updated;
    _S.patients = (_S.patients || []).map((item) => item.id === _S.selPid ? { ...item, ...updated } : item);
    const renderPts = window.mainRenderPts;
    if (typeof renderPts === 'function') renderPts();
    _safeRenderMiniInfo(updated);
    _safeRenderPatientEditorPanel();
    await _loadSignalTimelineSafe();
    GlioTwin.toast('Patient data updated', 'info');
  } catch (error) {
    GlioTwin.toast(error.message, 'error');
  }
}

function _safeRenderPatientEditorPanel() {
  const host = document.getElementById('patient-editor-panel');
  try {
    _renderPatientEditorPanel();
  } catch (error) {
    console.error('[patient editor render error]', error);
    if (host) {
      host.innerHTML = `<div class="signal-empty">${error.message}</div>`;
    }
  }
}

async function _loadSignalTimelineSafe() {
  if (typeof window.mainLoadSignalTimeline === 'function') {
    await window.mainLoadSignalTimeline();
  }
}

function _safeRenderMiniInfo(patient) {
  const info = document.getElementById('mini-info');
  try {
    _renderMiniInfo(patient);
  } catch (error) {
    console.error('[mini info render error]', error);
    if (info) {
      info.innerHTML = `<div class="tree-hint" style="color:var(--red)">${error.message}</div>`;
    }
  }
}

function _renderRightTabUi() {
  if (_S.activeRightTab === 'metrics') _S.activeRightTab = 'patient';
  const viewerArea = document.getElementById('viewer-area');
  const patientEditorPanel = document.getElementById('patient-editor-panel');
  const tabViewer = document.getElementById('right-tab-viewer');
  const tabPatient = document.getElementById('right-tab-patient');
  if (!viewerArea || !patientEditorPanel || !tabViewer || !tabPatient) return;
  const isViewer = _S.activeRightTab === 'viewer';
  const isPatient = _S.activeRightTab === 'patient';
  viewerArea.style.display = isViewer ? 'flex' : 'none';
  patientEditorPanel.style.display = isPatient ? 'block' : 'none';
  tabViewer.classList.toggle('active', isViewer);
  tabPatient.classList.toggle('active', isPatient);
}

function _updateViewerEmptyState() {
  const emptyEl = document.getElementById('main-empty');
  if (!emptyEl) return;
  const message = !_S.selPid
    ? 'Select a patient'
    : !_S.selSid
      ? 'Select a session'
      : 'No viewable NIfTI series available';
  const textEl = emptyEl.querySelector('p');
  if (textEl) textEl.textContent = message;
}

// ═══════════════════════════════════════════════════════════════
GlioTwin.register('browser', async (app) => {
  if (GlioTwin.state.currentPatient) _S.selPid = GlioTwin.state.currentPatient;
  if (GlioTwin.state.currentSession) _S.selSid = GlioTwin.state.currentSession;

  app.innerHTML = `
  <div class="app-layout">

    <!-- ══ LEFT SIDEBAR ══ -->
    <div class="data-panel">
      <div class="data-panel-scroll data-panel-scroll--fill">

        <div class="data-sec">
          <div class="data-sec-title">Filters</div>
          <div class="filter-grid">
            <div class="filter-row">
              <select class="f-sel" id="f-ds">
                <option value="">All datasets</option>
                <option value="irst_dicom_raw">DICOM</option>
                <option value="mu_glioma_post">MU-Glioma-Post</option>
                <option value="ucsd_ptgbm">UCSD-PTGBM</option>
                <option value="rhuh_gbm">RHUH-GBM</option>
                <option value="qin_gbm">QIN-GBM</option>
                <option value="glis_rt">GLIS-RT</option>
                <option value="lumiere">LUMIERE</option>
              </select>
              <select class="f-sel" id="f-vital">
                <option value="">All outcomes</option>
              </select>
            </div>
            <div class="filter-row">
              <select class="f-sel" id="f-idh">
                <option value="">IDH all</option>
                <option value="mutated">IDH mutated</option>
                <option value="wildtype">IDH wildtype</option>
              </select>
              <select class="f-sel" id="f-mgmt">
                <option value="">MGMT all</option>
                <option value="methylated">Methylated</option>
                <option value="unmethylated">Unmethylated</option>
              </select>
            </div>
            <input class="f-search" id="f-q" placeholder="Search patient ID…">
          </div>
        </div>

        <div class="data-sec data-sec--grow">
          <div class="data-sec-title" id="pt-title">Patients</div>
          <div class="tree-scroll" id="patient-list">
            <div class="loading-screen" style="height:60px">
              <div class="spinner" style="width:20px;height:20px;border-width:2px"></div>
            </div>
          </div>
        </div>

        <div class="data-sec" id="ov-sec" style="display:none">
          <div class="data-sec-title">Structures</div>
          <div id="ov-list"></div>
        </div>

        <div class="data-sec" id="mini-sec" style="display:none">
          <div class="data-sec-title">Patient info</div>
          <div class="mini-info" id="mini-info"></div>
        </div>

      </div>
    </div>

    <!-- ══ RIGHT — viewer ══ -->
    <div class="right-col">
      <div class="right-tabs">
        <button class="right-tab ${_S.activeRightTab === 'patient' ? 'active' : ''}" id="right-tab-patient">Patient Data</button>
        <button class="right-tab ${_S.activeRightTab === 'viewer' ? 'active' : ''}" id="right-tab-viewer">Viewer</button>
      </div>
      <div class="viewer-area" id="viewer-area">
        <div class="viewer-empty" id="main-empty">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1">
            <rect x="2" y="2" width="20" height="20" rx="3"/>
            <circle cx="12" cy="10" r="3.5"/>
            <path d="M2 16q5-7 10 0t10 0"/>
          </svg>
          <p>Select a patient and a session</p>
        </div>
        <div class="seq-grid" id="seq-grid" style="display:none"></div>
        <div class="nv-status" id="nv-status"></div>
        <div class="tl-status" id="tl-status"></div>
      </div>
      <div class="signal-panel" id="patient-editor-panel" style="display:none"></div>
    </div>

  </div>`;

  // ── Carica pazienti ──────────────────────────────────────
  if (!_S.patients) {
    try {
      _S.patients = await GlioTwin.fetch('/api/patients');
    } catch(e) {
      document.getElementById('patient-list').innerHTML =
        `<div class="tree-hint" style="color:var(--red)">${e.message}</div>`;
      return;
    }
  }

  function fillSelect(id, values, formatter = (value) => value) {
    const el = document.getElementById(id);
    if (!el) return;
    const existing = new Set([...el.options].map(opt => opt.value));
    values
      .filter(Boolean)
      .sort((a, b) => String(a).localeCompare(String(b)))
      .forEach(value => {
        if (existing.has(value)) return;
        const opt = document.createElement('option');
        opt.value = value;
        opt.textContent = formatter(value);
        el.appendChild(opt);
      });
  }

  fillSelect(
    'f-ds',
    [...new Set((_S.patients || []).map(p => p.dataset))],
    value => GlioTwin.humanizeDataset(value),
  );
  fillSelect(
    'f-vital',
    [...new Set((_S.patients || []).map(p => p.vital_status))],
    value => value.replaceAll('_', ' '),
  );

  // ── Lista pazienti ───────────────────────────────────────
  function renderPts() {
    const q    = document.getElementById('f-q')?.value.toLowerCase()  || '';
    const ds   = document.getElementById('f-ds')?.value  || '';
    const vital= document.getElementById('f-vital')?.value || '';
    const idh  = document.getElementById('f-idh')?.value || '';
    const mgmt = document.getElementById('f-mgmt')?.value|| '';
    const el   = document.getElementById('patient-list');
    const title= document.getElementById('pt-title');
    if (!el) return;
    const list = (_S.patients || []).filter(p => {
      if (ds   && p.dataset !== ds)               return false;
      if (vital && p.vital_status !== vital)      return false;
      if (idh  && p.idh_status  !== idh)          return false;
      if (mgmt && p.mgmt_status !== mgmt)         return false;
      if (q) {
        const haystack = [
          p.subject_id,
          p.patient_name,
          p.patient_given_name,
          p.patient_family_name,
        ].filter(Boolean).join(' ').toLowerCase();
        if (!haystack.includes(q)) return false;
      }
      return true;
    }).sort((a, b) => (a.subject_id || '').localeCompare(b.subject_id || '', undefined, { numeric: true }));
    if (title) title.textContent = `Patients (${list.length}/${_S.patients.length})`;
    el.innerHTML = list.length
      ? list.map(p => `
          <div class="tree-item ${_S.selPid===p.id?'selected':''}"
               data-pid="${p.id}" onclick="mainSelectPt(${p.id})">
            <div class="tree-item-main">
              <span class="tree-item-id">${GlioTwin.patientPrimary(p)}</span>
              ${GlioTwin.patientSecondary(p) ? `<span class="tree-item-sub">${GlioTwin.patientSecondary(p)}</span>` : ''}
            </div>
            <span class="tree-item-meta">${p.n_sessions||0}×</span>
            ${GlioTwin.datasetBadge(p.dataset)}
          </div>`).join('')
      : '<div class="tree-hint">No results</div>';
  }

  renderPts();
  window.mainRenderPts = renderPts;
  ['f-ds','f-vital','f-idh','f-mgmt'].forEach(id =>
    document.getElementById(id)?.addEventListener('change', renderPts));
  document.getElementById('f-q')?.addEventListener('input', renderPts);
  document.getElementById('right-tab-patient')?.addEventListener('click', () => {
    _S.activeRightTab = 'patient';
    _safeRenderPatientEditorPanel();
    _renderRightTabUi();
  });
  document.getElementById('right-tab-viewer')?.addEventListener('click', () => {
    _S.activeRightTab = 'viewer';
    _renderRightTabUi();
  });
  _renderRightTabUi();
  _updateViewerEmptyState();

  // ── Selezione paziente ───────────────────────────────────
  window.mainSelectPt = async (pid) => {
    _tlPlayStop();
    clearTimeout(_signalMetricPollTimer);
    _S.selPid = pid; _S.selSid = null;
    GlioTwin.state.currentPatient = pid;
    GlioTwin.state.currentSession = null;
    _S.nv2D = null; _S.activeSlots = [];
    _S.viewerNotice = '';
    renderPts();
    document.getElementById('ov-sec').style.display = 'none';
    _S.signalTimeline = null;
    _S.signalMetricStatus = null;
    _S.patientDetail = null;
    _S.signalOptions = {
      labels: [],
      sequenceTypes: [],
      sources: ['preferred', 'radiological', 'computed'],
    };
    _renderSignalPanel();
    _safeRenderPatientEditorPanel();
    _renderRightTabUi();
    _updateViewerEmptyState();

    _safeRenderMiniInfo((_S.patients||[]).find(p => p.id === pid));

    try {
      const [sessions, patientDetail] = await Promise.all([
        GlioTwin.fetch(`/api/patients/${pid}/sessions`),
        GlioTwin.fetch(`/api/patients/${pid}`),
      ]);
      _S.sessions = sessions;
      _S.patientDetail = patientDetail;
    } catch(e) {
      _setStatus('Error: ' + e.message, 'nv-err');
      return;
    }
    _safeRenderMiniInfo(_currentPatientData());
    _safeRenderPatientEditorPanel();
    _updateViewerEmptyState();

    // Auto-seleziona il primo timepoint (ordinato per data)
    const firstSession = [..._S.sessions]
      .sort((a, b) => (a.study_date || '').localeCompare(b.study_date || ''))
      .find(() => true);
    if (firstSession) {
      await window.mainSelectSes(firstSession.id);
    } else {
      try {
        await _loadSignalMetricStatus();
        await _loadSignalTimeline();
      } catch (metricError) {
        console.error('[patient-level signal timeline load error]', metricError);
        _renderSignalPanel();
      }
    }
  };

  // ── Selezione sessione ───────────────────────────────────
  window.mainSelectSes = async (sid) => {
    clearTimeout(_signalMetricPollTimer);
    try {
      _S.selSid = sid;
      GlioTwin.state.currentSession = sid;
      _S.nv2D = null; _S.activeSlots = [];
      _S.nvGrid.clear();
      _S.viewMode = 'grid';
      _S.focusedSlotKey = null;
      _S.focusedSliceType = null;
      _S._focusNvKey = null;
      _S._sharedCrosshairPos = null;
      _S._sharedCrosshairMM = null;
      _S._crosshairLocked = false;
      _S._gridWW   = null;
      _S._gridWL   = null;
      _S._gridZoom = 1.0;
      _S.viewerNotice = '';
      // Rimuove i layer del viewer precedente (canvas con ID duplicati altrimenti)
      document.getElementById('vg-grid-layer')?.remove();
      document.getElementById('vg-focus-layer')?.remove();
      document.querySelectorAll('#session-list .tree-item').forEach(r =>
        r.classList.toggle('selected', parseInt(r.dataset.sid) === sid));
      document.getElementById('ov-sec').style.display = 'none';
      _S.signalTimeline = null;
      _S.signalMetricStatus = null;
      _S.signalOptions = {
        labels: [],
        sequenceTypes: [],
        sources: ['preferred', 'radiological', 'computed'],
      };
      _renderSignalPanel();
      _safeRenderPatientEditorPanel();
      _renderRightTabUi();
      _updateViewerEmptyState();

      let sesDetail, structs;
      [sesDetail, structs] = await Promise.all([
        GlioTwin.fetch(`/api/sessions/${sid}`),
        GlioTwin.fetch(`/api/sessions/${sid}/structures`)
          .catch(() => ({ sequences:[], computed:[], radiological:[] })),
      ]);

      const allStructs = [
        ...(structs.computed    || []),
        ...(structs.radiological|| []),
      ].filter(s => s.mask_path);
      const hasRegisteredStructs = allStructs.some((s) => s.reference_space === 'registered');
      _S.overlaySpace = hasRegisteredStructs ? 'registered' : 'native';
      _S.allStructsRaw = allStructs.filter((s) => {
        const sp = s.reference_space || 'native';
        if (_S.overlaySpace === 'native') return sp === 'native' || sp === 'canonical_1mm';
        return sp === _S.overlaySpace;
      });

      _S.signalOptions = {
        labels: [...new Set(_S.allStructsRaw.map(s => s.label).filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b))),
        sequenceTypes: _sortSequenceTypes(
          new Set((sesDetail.sequences || [])
            .map((seq) => seq.sequence_type)
            .filter(Boolean)
            .filter((sequenceType) => ['T1ce', 'T1', 'T2', 'FLAIR', 'APT'].includes(sequenceType)))
        ),
        sources: ['preferred', 'radiological', 'computed'],
      };

      _ovRenderList();

      await _buildGrid(sesDetail.sequences || [], _S.overlayItems);

      _S.selectedSignalLabel = (_S.signalOptions.labels || []).includes(_S.selectedSignalLabel)
        ? _S.selectedSignalLabel
        : (_S.allStructsRaw[0]?.label || _S.signalOptions.labels[0] || '');
      _S.selectedSignalSequence = _S.selectedSignalSequence && (sesDetail.sequences || []).some((seq) => seq.sequence_type === _S.selectedSignalSequence)
        ? _S.selectedSignalSequence
        : ((sesDetail.sequences || []).find((seq) => seq.sequence_type === 'T1ce')?.sequence_type || _S.signalOptions.sequenceTypes[0] || '');

      _renderSignalPanel();
      _safeRenderPatientEditorPanel();
      _updateViewerEmptyState();

      // Il viewer deve restare indipendente dal pannello longitudinale.
      // Se il fetch metriche fallisce, non deve bloccare la visualizzazione delle serie.
      try {
        await _loadSignalMetricStatus();
        await _loadSignalTimeline();
      } catch (metricError) {
        console.error('[signal timeline load error]', metricError);
        _renderSignalPanel();
      }
    } catch(e) {
      console.error('[mainSelectSes error]', e);
      _setStatus('Error: ' + e.message, 'nv-err');
      const emptyEl = document.getElementById('main-empty');
      const gridEl = document.getElementById('seq-grid');
      if (gridEl) gridEl.style.display = 'none';
      if (emptyEl) {
        emptyEl.style.display = 'flex';
        emptyEl.innerHTML = `<p>${e.message}</p>`;
      }
      return;
    }
  };

  async function _loadSignalMetricStatus() {
    try {
      _S.signalMetricStatus = await GlioTwin.fetch(`/api/signal-metrics/status?_ts=${Date.now()}`);
    } catch (error) {
      _S.signalMetricStatus = {
        latest_job: null,
        cached_rows: 0,
        error: error.message,
      };
    }
  }

  function _scheduleSignalMetricPoll() {
    clearTimeout(_signalMetricPollTimer);
    const latestJob = _S.signalMetricStatus?.latest_job || null;
    if (!latestJob || !['queued', 'running'].includes(latestJob.status)) return;
    const prevJobId = latestJob.id;
    _signalMetricPollTimer = setTimeout(async () => {
      await _loadSignalMetricStatus();
      await _loadSignalTimeline();
      _renderSignalPanel();
      const newJob = _S.signalMetricStatus?.latest_job;
      if (newJob && newJob.id === prevJobId && !['queued', 'running'].includes(newJob.status)) {
        if (newJob.status === 'completed') {
          const n = newJob.completed_tasks ?? 0;
          const f = newJob.failed_tasks ?? 0;
          const msg = n === 0
            ? 'Nessuna metrica mancante — cache già aggiornata.'
            : `Completato: ${n} metriche calcolate${f > 0 ? `, ${f} strutture vuote (maschera vuota)` : ''}.`;
          GlioTwin.toast(msg, n === 0 ? 'info' : 'success');
        } else if (newJob.status === 'failed') {
          GlioTwin.toast(`Calcolo fallito: ${newJob.error_message || 'errore sconosciuto'}`, 'error');
        }
      } else {
        _scheduleSignalMetricPoll();
      }
    }, 2500);
  }

  async function _queueSignalMetricJob(force = false) {
    try {
      const job = await GlioTwin.post('/api/signal-metrics/jobs/queue-missing', { patient_id: null, force });
      _S.signalMetricStatus = {
        latest_job: job,
        cached_rows: _S.signalMetricStatus?.cached_rows || 0,
      };
      const label = force ? 'Ricalcolo completo avviato' : 'Calcolo metriche mancanti avviato';
      GlioTwin.toast(`${label} (job #${job.id})`, 'info');
    } catch (error) {
      GlioTwin.toast(`Errore avvio job: ${error.message}`, 'error');
    }
    _renderSignalPanel();
    _scheduleSignalMetricPoll();
  }

  async function _loadSignalTimeline() {
    if (!_S.selPid) return;
    const requestSeq = ++_signalTimelineRequestSeq;
    _S.signalTimelineLoading = true;
    _renderSignalPanel();
    await _loadSignalMetricStatus();
    const params = new URLSearchParams();
    if (_S.selectedSignalLabel) params.set('label', _S.selectedSignalLabel);
    if (_S.selectedSignalSequence) params.set('sequence_type', _S.selectedSignalSequence);
    if (_S.selectedSignalSource) params.set('structure_source', _S.selectedSignalSource);
    params.set('_ts', String(Date.now()));
    try {
      const response = await GlioTwin.fetch(`/api/patients/${_S.selPid}/signal-timeline?${params.toString()}`);
      if (requestSeq !== _signalTimelineRequestSeq) return;
      _S.signalTimeline = response;
      _S.selectedSignalLabel = response.selected_label || '';
      _S.selectedSignalSequence = response.selected_sequence_type || '';
      _S.selectedSignalSource = response.selected_source || 'preferred';
      _S.signalMetricStatus = response.cache_status || _S.signalMetricStatus;
    } catch (error) {
      if (requestSeq !== _signalTimelineRequestSeq) return;
      _S.signalTimeline = {
        available_labels: [],
        available_sequence_types: [],
        available_sources: ['preferred', 'radiological', 'computed'],
        selected_label: _S.selectedSignalLabel,
        selected_sequence_type: _S.selectedSignalSequence,
        selected_source: _S.selectedSignalSource,
        points: [],
        clinical_events: [],
        error: error.message,
      };
    } finally {
      if (requestSeq !== _signalTimelineRequestSeq) return;
      _S.signalTimelineLoading = false;
    }
    _renderSignalPanel();
    _renderRightTabUi();
    _scheduleSignalMetricPoll();
  }
  window.mainLoadSignalTimeline = _loadSignalTimeline;

  window.mainSignalSelectionChanged = async (field, value) => {
    if (field === 'label') _S.selectedSignalLabel = value;
    if (field === 'sequence') _S.selectedSignalSequence = value;
    if (field === 'source') _S.selectedSignalSource = value;
    await _loadSignalTimeline();
  };

  // ── Helpers ──────────────────────────────────────────────
  function _renderMiniInfo(patient) {
    const sec  = document.getElementById('mini-sec');
    const info = document.getElementById('mini-info');
    if (!patient || !sec || !info) return;
    const latestRt = patient.latest_radiotherapy_course || {};
    const refs = patient.external_ref_map || {};
    const diagnosis = patient.diagnosis || '—';
    const outcome = patient.vital_status ? patient.vital_status.replaceAll('_', ' ') : '—';
    info.innerHTML = `
      <div class="mi-row">
        ${GlioTwin.datasetBadge(patient.dataset)}
        <span class="badge badge-status-${patient.vital_status || 'unknown'}">${outcome}</span>
      </div>
      <div class="mi-kv">
        <span class="mi-k">Patient</span> <span>${GlioTwin.patientPrimary(patient)}</span>
        ${GlioTwin.state.showSensitive ? `<span class="mi-k">DOB</span>  <span>${GlioTwin.fmtDate(patient.patient_birth_date)}</span>` : ''}
        <span class="mi-k">Dx</span>   <span>${diagnosis}</span>
        <span class="mi-k">Dx Date</span> <span>${GlioTwin.fmtDate(patient.diagnosis_date)}</span>
        <span class="mi-k">Death</span> <span>${GlioTwin.fmtDate(patient.death_date)}</span>
        <span class="mi-k">IDH</span>  <span>${patient.idh_status||'—'}</span>
        <span class="mi-k">MGMT</span> <span>${patient.mgmt_status||'—'}</span>
        <span class="mi-k">Age</span>  <span>${patient.age_at_diagnosis!=null?patient.age_at_diagnosis.toFixed(0)+' yr':'—'}</span>
        <span class="mi-k">OS</span>   <span>${patient.os_days!=null?patient.os_days+' d':'—'}</span>
        <span class="mi-k">IDA</span> <span>${refs.ida || latestRt.external_course_id || '—'}</span>
        <span class="mi-k">CF</span> <span>${refs.tax_code || latestRt.tax_code || '—'}</span>
        <span class="mi-k">RT Start</span> <span>${GlioTwin.fmtDate(patient.radiotherapy_start_date || latestRt.start_date)}</span>
        <span class="mi-k">Fractions</span> <span>${latestRt.fractions_count ?? '—'}</span>
      </div>`;
    sec.style.display = '';
  }

  // ── Ripristina vista al ritorno ──────────────────────────
  if (_S.selPid) {
    const _restoreSid = _S.selSid;
    await mainSelectPt(_S.selPid);
    // Se la sessione salvata è diversa da quella auto-selezionata, ripristinala
    if (_restoreSid && _restoreSid !== _S.selSid) await mainSelectSes(_restoreSid);
  }
});

GlioTwin.register('viewer',  async () => GlioTwin.navigate('#/browser'));
GlioTwin.register('patient', async () => GlioTwin.navigate('#/browser'));
