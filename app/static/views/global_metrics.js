/* global_metrics.js — cross-patient population charts */
'use strict';

GlioTwin.register('global-metrics', async (app) => {
  app.innerHTML = `
    <div class="gm-layout">
      <div class="gm-header">
        <div>
          <div class="gm-title">Analysis</div>
          <div class="gm-subtitle">Distribuzione dei volumi e del segnale APT su tutta la coorte. Ogni punto rappresenta un timepoint. Clic su un punto per aprire il paziente.</div>
        </div>
        <div class="gm-controls">
          <label class="gm-ctrl-label">
            <span>Struttura</span>
            <select id="gm-label-filter">
              <option value="all">Tutte</option>
              <option value="enhancing_tumor">Enhancing Tumor</option>
              <option value="edema">Edema</option>
              <option value="necrotic_core">Necrotic Core</option>
              <option value="resection_cavity">Resection Cavity</option>
            </select>
          </label>
          <label class="gm-ctrl-label">
            <span>Sorgente</span>
            <select id="gm-source-filter">
              <option value="computed">Computed (FeTS)</option>
              <option value="radiological">Radiological</option>
            </select>
          </label>
          <label class="gm-ctrl-label">
            <span>Colora per</span>
            <select id="gm-color-by">
              <option value="patient">Paziente</option>
              <option value="label">Struttura</option>
              <option value="timepoint">Timepoint</option>
            </select>
          </label>
        </div>
      </div>

      <div id="gm-loading" class="gm-loading">
        <div class="spinner"></div>
        <span>Caricamento dati globali…</span>
      </div>

      <div id="gm-content" style="display:none">
        <div class="gm-summary" id="gm-summary"></div>
        <div class="gm-charts-grid">
          <div class="gm-chart-card">
            <div class="gm-chart-title">Volume per struttura <span class="gm-chart-unit">(mL)</span></div>
            <div class="gm-chart-sub">Boxplot + punti individuali. Clic su un punto per aprire il paziente.</div>
            <div id="gm-vol-chart" class="gm-chart-container"></div>
          </div>
          <div class="gm-chart-card">
            <div class="gm-chart-title">Segnale APT per struttura <span class="gm-chart-unit">(%MTR<sub>asym</sub>)</span></div>
            <div class="gm-chart-sub">Mediana del segnale APT nella maschera. Solo timepoint con APT disponibile.</div>
            <div id="gm-apt-chart" class="gm-chart-container"></div>
            <div id="gm-stats-panel" class="gm-stats-panel"></div>
          </div>
        </div>
      </div>
    </div>
  `;

  let _allRows = [];
  let _charts  = [];
  const _patientColorMap = new Map();

  // ─── palettes ─────────────────────────────────────────────────────────────
  const PALETTE = [
    '#3b7ef8','#f97316','#22d3ee','#a78bfa','#34d399','#fb923c',
    '#f472b6','#facc15','#60a5fa','#4ade80','#e879f9','#94a3b8',
    '#ff6b6b','#48cae4','#ffb703','#06d6a0','#ef476f','#118ab2',
    '#ffd166','#0096c7','#cbf3f0','#ffcb77','#fe5d9f','#7678ed',
  ];

  const LABEL_COLORS = {
    enhancing_tumor:  '#f97316',
    edema:            '#3b7ef8',
    necrotic_core:    '#ef4444',
    resection_cavity: '#a78bfa',
  };

  const LABEL_NAMES = {
    enhancing_tumor:  'Enhancing Tumor',
    edema:            'Edema',
    necrotic_core:    'Necrotic Core',
    resection_cavity: 'Resection Cavity',
  };

  const LABEL_SHORT = {
    enhancing_tumor:  'ET',
    edema:            'ED',
    necrotic_core:    'NC',
    resection_cavity: 'RC',
  };

  const LABEL_ORDER = ['resection_cavity', 'necrotic_core', 'edema', 'enhancing_tumor'];

  function _patientColor(sid) {
    if (!_patientColorMap.has(sid)) {
      _patientColorMap.set(sid, PALETTE[_patientColorMap.size % PALETTE.length]);
    }
    return _patientColorMap.get(sid);
  }

  function _pointColor(row, colorBy) {
    if (colorBy === 'label')     return LABEL_COLORS[row.label] || '#8395b0';
    if (colorBy === 'timepoint') {
      const idx = parseInt((row.session_label || '').replace(/\D/g, '')) || 1;
      const t   = Math.min((idx - 1) / 6, 1);
      return `hsl(${220 - t * 140}, 80%, ${55 + t * 15}%)`;
    }
    return _patientColor(row.subject_id);
  }

  // ─── helpers ──────────────────────────────────────────────────────────────
  function _filterRows(rows, labelFilter, sourceFilter) {
    return rows.filter(r =>
      (labelFilter === 'all' || r.label === labelFilter) &&
      r.structure_source === sourceFilter &&
      r.signal_error === null
    );
  }

  function _jitter() { return (Math.random() - 0.5) * 0.42; }

  function _boxStats(values) {
    if (!values.length) return null;
    const s = [...values].sort((a, b) => a - b);
    const q = p => {
      const pos = (s.length - 1) * p;
      const lo = Math.floor(pos), hi = Math.ceil(pos);
      return s[lo] + (s[hi] - s[lo]) * (pos - lo);
    };
    const q1 = q(0.25), q3 = q(0.75), iqr = q3 - q1;
    return [
      Math.max(s[0],             q1 - 1.5 * iqr),
      q1, q(0.5), q3,
      Math.min(s[s.length - 1], q3 + 1.5 * iqr),
    ];
  }

  // ─── statistics ───────────────────────────────────────────────────────────
  function _normalCDF(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x));
    const d = 0.3989423 * Math.exp(-x * x / 2);
    const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.7814779 + t * (-1.8212560 + t * 1.3302744))));
    return x > 0 ? 1 - p : p;
  }

  function _logGamma(x) {
    const c = [0.99999999999980993,676.5203681218851,-1259.1392167224028,
               771.32342877765313,-176.61502916214059,12.507343278686905,
               -0.13857109526572012,9.9843695780195716e-6,1.5056327351493116e-7];
    if (x < 0.5) return Math.log(Math.PI / Math.sin(Math.PI * x)) - _logGamma(1 - x);
    x -= 1;
    let a = c[0];
    const t = x + 7.5;
    for (let i = 1; i < 9; i++) a += c[i] / (x + i);
    return 0.5 * Math.log(2 * Math.PI) + (x + 0.5) * Math.log(t) - t + Math.log(a);
  }

  function _gammaRegP(a, x) {
    if (x < 0) return 0;
    if (x < a + 1) {
      let term = 1 / a, sum = term;
      for (let n = 1; n < 200; n++) {
        term *= x / (a + n); sum += term;
        if (Math.abs(term) < 1e-10 * Math.abs(sum)) break;
      }
      return Math.exp(-x + a * Math.log(x) - _logGamma(a)) * sum;
    }
    let b = x + 1 - a, c = 1e30, d = 1 / b, h = d;
    for (let i = 1; i <= 200; i++) {
      const an = -i * (i - a);
      b += 2;
      d = an * d + b; if (Math.abs(d) < 1e-30) d = 1e-30;
      c = b + an / c; if (Math.abs(c) < 1e-30) c = 1e-30;
      d = 1 / d;
      const del = d * c;
      h *= del;
      if (Math.abs(del - 1) < 1e-10) break;
    }
    return 1 - Math.exp(-x + a * Math.log(x) - _logGamma(a)) * h;
  }

  function _chi2pvalue(H, df) {
    if (H <= 0 || df <= 0) return 1;
    return 1 - _gammaRegP(df / 2, H / 2);
  }

  function _mannWhitneyU(a, b) {
    if (a.length < 2 || b.length < 2) return null;
    const tagged = [...a.map(v => ({ v, g: 0 })), ...b.map(v => ({ v, g: 1 }))].sort((x, y) => x.v - y.v);
    const ranks = new Array(tagged.length);
    let i = 0;
    while (i < tagged.length) {
      let j = i;
      while (j < tagged.length - 1 && tagged[j + 1].v === tagged[i].v) j++;
      const avg = (i + j) / 2 + 1;
      for (let k = i; k <= j; k++) ranks[k] = avg;
      i = j + 1;
    }
    let R1 = 0;
    tagged.forEach((item, idx) => { if (item.g === 0) R1 += ranks[idx]; });
    const n1 = a.length, n2 = b.length;
    const U1 = R1 - n1 * (n1 + 1) / 2;
    const U2 = n1 * n2 - U1;
    const U  = Math.min(U1, U2);
    const mu = n1 * n2 / 2;
    const sigma = Math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12);
    const z = (U - mu) / sigma;
    const p = 2 * _normalCDF(-Math.abs(z));
    // rank-biserial: positive = first group tends to be larger
    const r = (U1 - U2) / (n1 * n2);
    return { u: U, z, p, r, n1, n2 };
  }

  function _kruskalWallis(groupsMap, labels) {
    const groups = labels.map(l => groupsMap[l] || []).filter(g => g.length > 1);
    if (groups.length < 2) return null;
    const N = groups.reduce((s, g) => s + g.length, 0);
    if (N < 4) return null;
    const tagged = groups.flatMap((g, gi) => g.map(v => ({ v, gi }))).sort((a, b) => a.v - b.v);
    const ranks = new Array(tagged.length);
    let i = 0;
    while (i < tagged.length) {
      let j = i;
      while (j < tagged.length - 1 && tagged[j + 1].v === tagged[i].v) j++;
      const avg = (i + j) / 2 + 1;
      for (let k = i; k <= j; k++) ranks[k] = avg;
      i = j + 1;
    }
    const rankSums = new Array(groups.length).fill(0);
    tagged.forEach((item, idx) => { rankSums[item.gi] += ranks[idx]; });
    const H = (12 / (N * (N + 1))) * groups.reduce((s, g, gi) => s + rankSums[gi] ** 2 / g.length, 0) - 3 * (N + 1);
    const df = groups.length - 1;
    return { H, df, p: _chi2pvalue(H, df), N };
  }

  // ─── stats panel ──────────────────────────────────────────────────────────
  function _renderStatsPanel(aptRows) {
    const el = document.getElementById('gm-stats-panel');
    if (!el) return;

    const present = LABEL_ORDER.filter(l => aptRows.some(r => r.label === l && r.median != null));
    if (present.length < 2) {
      el.innerHTML = '<div class="gm-stats-note">Dati APT insufficienti per analisi statistica.</div>';
      return;
    }

    const groups = Object.fromEntries(present.map(l => [
      l, aptRows.filter(r => r.label === l && r.median != null).map(r => r.median)
    ]));

    const kw = _kruskalWallis(groups, present);

    const pairs = [];
    for (let i = 0; i < present.length; i++) {
      for (let j = i + 1; j < present.length; j++) {
        const res = _mannWhitneyU(groups[present[i]], groups[present[j]]);
        if (res) pairs.push({ la: present[i], lb: present[j], ...res });
      }
    }
    const m = pairs.length || 1;
    const corrected = pairs.map(p => ({ ...p, p_corr: Math.min(p.p * m, 1) }));

    const pMatrix = {};
    corrected.forEach(({ la, lb, p_corr, r }) => {
      pMatrix[`${la}|${lb}`] = { p: p_corr, r };
      pMatrix[`${lb}|${la}`] = { p: p_corr, r: -r };
    });

    function _pCell(la, lb) {
      if (la === lb) return '<td class="gm-sc-diag">—</td>';
      const entry = pMatrix[`${la}|${lb}`];
      if (!entry) return '<td class="gm-sc-na">—</td>';
      const { p, r } = entry;
      const cls = p < 0.001 ? 'gm-sc-sig3' : p < 0.01 ? 'gm-sc-sig2' : p < 0.05 ? 'gm-sc-sig1' : 'gm-sc-ns';
      const pStr = p < 0.001 ? '<0.001' : p < 0.01 ? p.toFixed(3) : p.toFixed(2);
      const rStr = (r >= 0 ? '+' : '') + r.toFixed(2);
      const rColor = Math.abs(r) > 0.5 ? '#22d3ee' : Math.abs(r) > 0.3 ? '#a78bfa' : '#8395b0';
      return `<td class="gm-sc-cell ${cls}">
        <div class="gm-sc-p">${pStr}</div>
        <div class="gm-sc-r" style="color:${rColor}">${rStr}</div>
      </td>`;
    }

    const kwLine = kw
      ? `<div class="gm-stats-kw">
           Kruskal-Wallis: H = ${kw.H.toFixed(2)}, df = ${kw.df},
           <span class="${kw.p < 0.05 ? 'gm-stats-sig' : 'gm-stats-ns'}">
             p ${kw.p < 0.001 ? '< 0.001' : '= ' + kw.p.toFixed(3)}
           </span>
           &nbsp;(N = ${kw.N} misure APT)
         </div>`
      : '';

    const headerCells = present.map(l =>
      `<th style="color:${LABEL_COLORS[l]||'#8395b0'}">${LABEL_SHORT[l]||l}</th>`
    ).join('');

    const bodyRows = present.map(la => `
      <tr>
        <th style="color:${LABEL_COLORS[la]||'#8395b0'};white-space:nowrap">${LABEL_SHORT[la]}</th>
        ${present.map(lb => _pCell(la, lb)).join('')}
      </tr>
    `).join('');

    el.innerHTML = `
      <div class="gm-stats-title">Differenziazione APT tra strutture</div>
      ${kwLine}
      <div class="gm-stats-subtitle">Mann-Whitney U, Bonferroni (×${m}) — p corretto / r ranghi</div>
      <div class="gm-stats-wrap">
        <table class="gm-stats-table">
          <thead><tr><th></th>${headerCells}</tr></thead>
          <tbody>${bodyRows}</tbody>
        </table>
        <div class="gm-stats-legend">
          <span class="gm-sc-cell gm-sc-sig3">p&lt;0.001</span>
          <span class="gm-sc-cell gm-sc-sig2">p&lt;0.01</span>
          <span class="gm-sc-cell gm-sc-sig1">p&lt;0.05</span>
          <span class="gm-sc-cell gm-sc-ns">n.s.</span>
          &nbsp;·&nbsp; r&gt;0 = riga &gt; colonna
        </div>
      </div>
    `;
  }

  // ─── chart disposal ───────────────────────────────────────────────────────
  function _destroyCharts() {
    _charts.forEach(c => { try { c.dispose(); } catch (_) {} });
    _charts = [];
  }

  // ─── strip chart (horizontal: Y=category, X=value) ────────────────────────
  function _makeStripChart(containerId, rows, valueKey, xLabel, colorBy, onClickRow) {
    const el = document.getElementById(containerId);
    if (!el) return;

    const chart = echarts.init(el, 'dark');
    _charts.push(chart);

    const present  = new Set(rows.map(r => r.label));
    const labels   = LABEL_ORDER.filter(l => present.has(l));
    const catIndex = Object.fromEntries(labels.map((l, i) => [l, i]));
    const N        = labels.length;

    // ── scatter series ────────────────────────────────────────────────────
    const scatterSeries = labels.map(label => ({
      name: LABEL_NAMES[label] || label,
      type: 'scatter',
      symbolSize: 8,
      data: rows
        .filter(r => r.label === label && r[valueKey] != null)
        .map(r => ({
          value: [r[valueKey], catIndex[label] + _jitter()],
          itemStyle: { color: _pointColor(r, colorBy), opacity: 0.88 },
          _row: r,
        })),
      emphasis: { scale: 1.7 },
      z: 10,
    }));

    // ── boxplot overlay (custom series) ───────────────────────────────────
    const boxCustomData = labels.map((label, i) => {
      const vals  = rows.filter(r => r.label === label && r[valueKey] != null).map(r => r[valueKey]);
      const stats = _boxStats(vals);
      if (!stats) return null;
      const [wLo, q1, med, q3, wHi] = stats;
      const col = LABEL_COLORS[label] || '#8395b0';
      return { i, q1, med, q3, wLo, wHi, col, label, n: vals.length };
    }).filter(Boolean);

    const isApt = valueKey === 'median';

    const boxCustomSeries = {
      type: 'custom',
      name: '_box',
      silent: true,
      z: 2,
      renderItem(params, api) {
        const d = boxCustomData[params.dataIndex];
        if (!d) return { type: 'group', children: [] };
        const [xGridLeft, yCenter] = api.coord([0, d.i]);
        const xQ1   = api.coord([d.q1,  d.i])[0];
        const xMed  = api.coord([d.med,  d.i])[0];
        const xQ3   = api.coord([d.q3,  d.i])[0];
        const xWLo  = api.coord([d.wLo, d.i])[0];
        const xWHi  = api.coord([d.wHi, d.i])[0];
        const halfH = 13;
        const ls    = { stroke: d.col, lineWidth: 1.5, opacity: 0.6 };
        const medVal = d.med.toFixed(isApt ? 2 : 1);

        return {
          type: 'group',
          children: [
            // Left colored bar indicator
            { type: 'rect',
              shape: { x: xGridLeft, y: yCenter - halfH - 2, width: 3, height: (halfH + 2) * 2 },
              style: { fill: d.col, opacity: 0.85 }, z2: 3 },
            // Structure name label (right-aligned, in left margin)
            { type: 'text',
              style: {
                x: xGridLeft - 8, y: yCenter,
                text: LABEL_NAMES[d.label] || d.label,
                fill: d.col, opacity: 0.95,
                fontSize: 11, fontWeight: 700,
                textAlign: 'right', textVerticalAlign: 'middle',
              }, z2: 20 },
            // Whisker line
            { type: 'line',
              shape: { x1: xWLo, y1: yCenter, x2: xWHi, y2: yCenter },
              style: ls },
            // Whisker caps
            { type: 'line', shape: { x1: xWLo, y1: yCenter - halfH * 0.5, x2: xWLo, y2: yCenter + halfH * 0.5 }, style: ls },
            { type: 'line', shape: { x1: xWHi, y1: yCenter - halfH * 0.5, x2: xWHi, y2: yCenter + halfH * 0.5 }, style: ls },
            // IQR filled box
            { type: 'rect',
              shape: { x: xQ1, y: yCenter - halfH, width: xQ3 - xQ1, height: halfH * 2 },
              style: { fill: d.col, opacity: 0.22 } },
            // IQR box border
            { type: 'rect',
              shape: { x: xQ1, y: yCenter - halfH, width: xQ3 - xQ1, height: halfH * 2 },
              style: { fill: 'none', stroke: d.col, lineWidth: 2, opacity: 0.75 } },
            // Median line (white)
            { type: 'line',
              shape: { x1: xMed, y1: yCenter - halfH, x2: xMed, y2: yCenter + halfH },
              style: { stroke: '#ffffff', lineWidth: 2.5, opacity: 0.95 } },
            // Median value annotation
            { type: 'text',
              style: {
                x: xMed + 5, y: yCenter - halfH - 2,
                text: medVal,
                fill: '#ffffff', opacity: 0.75,
                fontSize: 10, fontWeight: 600,
                textAlign: 'left', textVerticalAlign: 'bottom',
              }, z2: 25 },
            // n label (sample size)
            { type: 'text',
              style: {
                x: xWHi + 4, y: yCenter,
                text: `n=${d.n}`,
                fill: '#8395b0', opacity: 0.7,
                fontSize: 9,
                textAlign: 'left', textVerticalAlign: 'middle',
              }, z2: 20 },
          ],
        };
      },
      data: boxCustomData.map((_, i) => [i]),
      encode: { x: 0 },
    };

    const option = {
      backgroundColor: 'transparent',
      grid: { left: 140, right: 40, top: 12, bottom: 44 },
      xAxis: {
        type: 'value',
        name: xLabel,
        nameLocation: 'middle',
        nameGap: 28,
        nameTextStyle: { color: '#8395b0', fontSize: 11 },
        axisLabel:  { color: '#8395b0', fontSize: 11 },
        splitLine:  { lineStyle: { color: '#1e2a40' } },
        axisLine:   { lineStyle: { color: '#2d4a7a' } },
        min: 0,
      },
      yAxis: {
        type: 'value',
        min: -0.5,
        max: N - 0.5,
        interval: 1,
        axisLabel: { show: false },
        axisTick:  { show: false },
        axisLine:  { lineStyle: { color: '#2d4a7a' } },
        splitLine: { show: true, lineStyle: { color: '#172033', type: 'dashed' } },
      },
      tooltip: {
        trigger: 'item',
        backgroundColor: '#0f1623',
        borderColor: '#2d4a7a',
        textStyle: { color: '#dde4f0', fontSize: 12 },
        formatter: params => {
          const r = params.data?._row;
          if (!r) return '';
          const val = r[valueKey];
          return [
            `<div style="font-weight:700;margin-bottom:3px">${r.subject_id}</div>`,
            `<div style="color:#8395b0;font-size:11px">${r.session_label}${r.study_date ? ' · ' + r.study_date : ''}</div>`,
            `<div style="margin-top:5px"><span style="color:${LABEL_COLORS[r.label]||'#8395b0'}">■</span> ${LABEL_NAMES[r.label]||r.label}</div>`,
            `<div>${xLabel}: <b>${val != null ? val.toFixed(valueKey === 'median' ? 3 : 1) : '—'}</b></div>`,
            `<div style="color:#3b7ef8;margin-top:3px;font-size:11px">Clic per aprire →</div>`,
          ].join('');
        },
      },
      series: [boxCustomSeries, ...scatterSeries],
    };

    chart.setOption(option);
    chart.on('click', params => {
      const r = params.data?._row;
      if (r) onClickRow(r);
    });
    return chart;
  }

  // ─── summary chips ────────────────────────────────────────────────────────
  function _renderSummary(rows) {
    const patients = new Set(rows.map(r => r.subject_id)).size;
    const sessions = new Set(rows.map(r => r.session_id)).size;
    const aptRows  = rows.filter(r => r.sequence_type === 'APT' && !r.signal_error).length;
    const el = document.getElementById('gm-summary');
    if (el) el.innerHTML = `
      <span class="signal-status-chip ok">${patients} pazienti</span>
      <span class="signal-status-chip">${sessions} timepoint</span>
      <span class="signal-status-chip">${aptRows} misure APT</span>
    `;
  }

  // ─── main render ──────────────────────────────────────────────────────────
  function _render() {
    const labelFilter  = document.getElementById('gm-label-filter')?.value  || 'all';
    const sourceFilter = document.getElementById('gm-source-filter')?.value || 'computed';
    const colorBy      = document.getElementById('gm-color-by')?.value      || 'patient';

    _destroyCharts();

    const baseRows = _filterRows(_allRows, labelFilter, sourceFilter);
    const aptRows  = baseRows.filter(r => r.sequence_type === 'APT');

    const volSeen = new Set();
    const volRows = baseRows
      .filter(r => r.volume_ml != null)
      .filter(r => {
        const key = `${r.session_id}|${r.label}`;
        if (volSeen.has(key)) return false;
        volSeen.add(key); return true;
      });

    _renderSummary(_allRows);

    function _onClickRow(r) {
      GlioTwin.state.currentPatient = r.patient_id;
      GlioTwin.state.currentSession = r.session_id;
      location.hash = '#/browser';
    }

    _makeStripChart('gm-vol-chart', volRows, 'volume_ml', 'Volume (mL)', colorBy, _onClickRow);
    _makeStripChart('gm-apt-chart', aptRows, 'median',    '% MTRasym',  colorBy, _onClickRow);

    _renderStatsPanel(aptRows);
  }

  // ─── load ─────────────────────────────────────────────────────────────────
  try {
    const data = await GlioTwin.fetch('/api/global-metrics');
    _allRows = data.rows || [];
    document.getElementById('gm-loading').style.display = 'none';
    document.getElementById('gm-content').style.display = 'block';
    _render();
    ['gm-label-filter', 'gm-source-filter', 'gm-color-by'].forEach(id =>
      document.getElementById(id)?.addEventListener('change', _render)
    );
    window.addEventListener('resize', () => _charts.forEach(c => c.resize()));
  } catch (err) {
    document.getElementById('gm-loading').innerHTML =
      `<span style="color:#ef4444">Errore caricamento: ${err.message}</span>`;
  }
});
