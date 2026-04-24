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

const SERIES_ORDER = ['T1ce', 'CT1', 'T1', 'T2', 'FLAIR', 'APT'];
const SERIES_ALIASES = {
  T1ce: 'T1+C',
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
  allStructsRaw: [],   // all native structures for current session (all models)
  selPid:        null,
  selSid:        null,
  // viewer
  nv2D:          null,
  activeSlots:   [],
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
  const match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const dt = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  return Number.isNaN(dt.getTime()) ? null : dt;
}

function _pointTemporalDate(point) {
  return _parseIsoDate(point?.study_date) || null;
}

function _eventTemporalDate(event) {
  return _parseIsoDate(event?.event_date) || null;
}

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
function _ovBuildItems(structs) {
  return structs.map(s => ({
    url:          _structUrl(s.mask_path, s.label_code),
    name:         s.mask_path.split('/').pop(),
    label:        s.label,
    displayLabel: `${_friendlyStructureLabel(s.label)} · ${_ovModelShort(s.model_name)}`,
    color:        _ovColor(s.label, s.model_name),
    volume_ml:    s.volume_ml,
    model_name:   s.model_name,
  }));
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

function _buildViewableSlots(seqs) {
  return (Array.isArray(seqs) ? seqs : [])
    .filter(seq => _isViewablePath(seq.processed_path || seq.raw_path || ''))
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
      seqPath: seq.processed_path || seq.raw_path || '',
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
  if (!host) return;
  host.innerHTML = _renderBlendController();
  _bindBlendController();
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

// ── Ricarica overlay su tutti i pannelli ─────────────────────
async function _reloadOverlays() {
  const requestId = ++_loadSeq;
  const overlays = _checkedOverlays();
  const current = _currentSlot();
  if (!_S.nv2D || !current?.seqPath) return;
  _setStatus('Loading…', 'nv-loading');
  try {
    _S.nv2D.setSliceType(_S.activeOrientation);
    await GlioViewer.loadInto(_S.nv2D, current.seqPath, overlays);
    if (requestId !== _loadSeq) return;
    _syncWLFromVolume();
    _setStatus('Loaded ✓', 'nv-ok');
  } catch(e) {
    if (requestId !== _loadSeq) return;
    _setStatus(e.message, 'nv-err');
  }
}

function _checkedOverlays() {
  return [...document.querySelectorAll('.ov-cb:checked')]
    .map(el => {
      const item = _S.overlayItems[parseInt(el.value)];
      return item ? { url: item.url, label: item.label, name: item.name } : null;
    }).filter(Boolean);
}

function _renderViewerControls() {
  const currentOrientation = String(_S.activeOrientation);
  const orientationOptions = VIEW_MODES.map(mode =>
    `<option value="${mode.sliceType}" ${String(mode.sliceType) === currentOrientation ? 'selected' : ''}>${mode.label}</option>`
  ).join('');

  return `
    <div class="single-viewer-shell">
      <div class="single-viewer-toolbar">
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
        <div class="single-viewer-stage">
          <canvas id="sgcv-single"></canvas>
          <div class="nv-voxel-readout" id="nv-voxel" title="Intensità voxel sotto il cursore">—</div>
        </div>
        <aside class="blend-controller-panel">
          <div class="blend-controller-host" id="blend-controller-host"></div>
        </aside>
      </div>
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
  gridEl.innerHTML = _renderViewerControls();
  gridEl.style.display = '';
  if (emptyEl) emptyEl.style.display = 'none';

  const canvas = document.getElementById('sgcv-single');
  _S.nv2D = await GlioViewer.createInstance(canvas, _S.activeOrientation);
  _refreshBlendUi();
  _attachViewerExtras();

  document.getElementById('viewer-orientation')?.addEventListener('change', async (e) => {
    _S.activeOrientation = parseInt(e.target.value, 10);
    await _reloadOverlays();
  });

  await _reloadOverlays();
}

// ── Costruisce il viewer singolo ─────────────────────────────
async function _buildGrid(seqs) {
  _stopBlendDrag();
  _S.activeSlots = _buildViewableSlots(seqs);

  const list = Array.isArray(seqs) ? seqs : [];
  const hasAnySequences = list.length > 0;
  const hasAnyViewable = list.some(seq => _isViewablePath(seq.processed_path || seq.raw_path || ''));
  _S.viewerNotice = hasAnySequences && !hasAnyViewable
    ? 'This session currently contains raw DICOM references only. Run preprocessing or attach NIfTI volumes before opening it in the viewer.'
    : '';

  if (!_S.activeSlots.some(slot => slot.key === _S.activeSeriesKey)) {
    _S.activeSeriesKey = _preferredSeriesKey(_S.activeSlots);
  }
  if (!_S.activeSlots.some(slot => slot.key === _S.targetSeriesKey)) {
    _clearBlendTransition();
  }
  if (!VIEW_MODES.some(mode => mode.sliceType === _S.activeOrientation)) {
    _S.activeOrientation = VIEW_MODES[0].sliceType;
  }

  await _renderSingleViewer();
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
      <div class="data-panel-scroll">

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

        <div class="data-sec">
          <div class="data-sec-title" id="pt-title">Patients</div>
          <div class="tree-scroll" id="patient-list" style="max-height:180px">
            <div class="loading-screen" style="height:60px">
              <div class="spinner" style="width:20px;height:20px;border-width:2px"></div>
            </div>
          </div>
        </div>

        <div class="data-sec">
          <div class="data-sec-title" id="sess-title">Sessions</div>
          <div class="tree-scroll" id="session-list" style="max-height:130px">
            <div class="tree-hint">← Select a patient</div>
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
    });
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

    const sesList  = document.getElementById('session-list');
    const sesTitle = document.getElementById('sess-title');
    sesList.innerHTML = '<div class="loading-screen" style="height:50px">' +
      '<div class="spinner" style="width:18px;height:18px;border-width:2px"></div></div>';
    _safeRenderMiniInfo((_S.patients||[]).find(p => p.id === pid));

    try {
      const [sessions, patientDetail] = await Promise.all([
        GlioTwin.fetch(`/api/patients/${pid}/sessions`),
        GlioTwin.fetch(`/api/patients/${pid}`),
      ]);
      _S.sessions = sessions;
      _S.patientDetail = patientDetail;
    } catch(e) {
      sesList.innerHTML = `<div class="tree-hint" style="color:var(--red)">${e.message}</div>`;
      return;
    }
    if (sesTitle) sesTitle.textContent = `Sessions (${_S.sessions.length})`;
    sesList.innerHTML = _S.sessions.length
      ? _S.sessions.map(s => `
          <div class="tree-item" data-sid="${s.id}" onclick="mainSelectSes(${s.id})">
            <div class="tree-item-main">
              <span class="tree-item-id">${s.session_label}</span>
              ${GlioTwin.sessionMeta(s) ? `<span class="tree-item-sub">${GlioTwin.sessionMeta(s)}</span>` : ''}
            </div>
            ${s.timepoint_type
              ? `<span class="tp-pill tp-${s.timepoint_type}">${s.timepoint_type.replace('_',' ')}</span>`
              : ''}
            <span class="tree-item-meta">${GlioTwin.fmtDays(s.days_from_baseline)}</span>
          </div>`).join('')
      : '<div class="tree-hint">No sessions</div>';
    _safeRenderMiniInfo(_currentPatientData());
    _safeRenderPatientEditorPanel();
    _updateViewerEmptyState();
    try {
      await _loadSignalMetricStatus();
      await _loadSignalTimeline();
    } catch (metricError) {
      console.error('[patient-level signal timeline load error]', metricError);
      _renderSignalPanel();
    }
  };

  // ── Selezione sessione ───────────────────────────────────
  window.mainSelectSes = async (sid) => {
    clearTimeout(_signalMetricPollTimer);
    try {
      _S.selSid = sid;
      GlioTwin.state.currentSession = sid;
      _S.nv2D = null; _S.activeSlots = [];
      _S.viewerNotice = '';
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

      // ── Overlay nativi ────────────────────────────────────
      _S.allStructsRaw = [
        ...(structs.computed    || []),
        ...(structs.radiological|| []),
      ].filter(s => s.mask_path && s.reference_space === 'native');

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
    await mainSelectPt(_S.selPid);
    if (_S.selSid) await mainSelectSes(_S.selSid);
  }
});

GlioTwin.register('viewer',  async () => GlioTwin.navigate('#/browser'));
GlioTwin.register('patient', async () => GlioTwin.navigate('#/browser'));
