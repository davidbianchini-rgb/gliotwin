/* rhglioseg.js — Segmentation: patient/timepoint list, model selector, run/delete */
'use strict';

const SEG_MODELS = [
  {
    key: 'fets_postop',
    label: 'FeTS postop',
    description: 'Tumor segmentation FeTS su casi gia preprocessati',
    requires_preprocessing: true,
    run_endpoint: '/api/fetsseg/run',
    status_endpoint: '/api/fetsseg/status',
  },
  {
    key: 'rh-glioseg-v3',
    label: 'rh-GlioSeg v3',
    description: 'nnUNet 5-fold ensemble · Dataset016_RH-GlioSeg_v3',
    requires_preprocessing: true,
    run_endpoint: '/api/rhglioseg/run',
    status_endpoint: '/api/rhglioseg/status',
  },
];

const SegView = {
  state: {
    sessions: [],
    selected: {},        // session_id → bool
    selectedModel: 'fets_postop',
    filterPreproc: false,
    filterQ: '',
    jobStatus: {},
    pollTimer: null,
    loading: true,
  },

  selectedIds() {
    return Object.entries(this.state.selected)
      .filter(([, v]) => v)
      .map(([k]) => parseInt(k, 10));
  },

  filtered() {
    const q = this.state.filterQ.trim().toLowerCase();
    return this.state.sessions.filter(s => {
      if (this.state.filterPreproc && !s.preprocessing_ready) return false;
      if (!q) return true;
      return [s.subject_id, s.session_label, s.dataset]
        .filter(Boolean).join(' ').toLowerCase().includes(q);
    });
  },

  modelInfo() {
    return SEG_MODELS.find(m => m.key === this.state.selectedModel) || SEG_MODELS[0];
  },

  async load() {
    this.state.loading = true;
    this.render();
    const [sessions, ...statuses] = await Promise.all([
      GlioTwin.fetch('/api/segmentation/sessions'),
      ...SEG_MODELS.map(model => GlioTwin.fetch(model.status_endpoint).catch(() => null)),
    ]);
    this.state.sessions  = sessions.sessions || [];
    this.state.jobStatus = Object.fromEntries(SEG_MODELS.map((model, index) => [model.key, statuses[index]]));
    this.state.loading   = false;
    this.render();
    if (statuses.some(status => status?.running)) this._startPoll();
  },

  async refreshStatus() {
    const statuses = await Promise.all(
      SEG_MODELS.map(model => GlioTwin.fetch(model.status_endpoint).catch(() => null))
    );
    this.state.jobStatus = Object.fromEntries(SEG_MODELS.map((model, index) => [model.key, statuses[index]]));
    return this.state.jobStatus;
  },

  _startPoll() {
    clearTimeout(this.state.pollTimer);
    this.state.pollTimer = setTimeout(async () => {
      const statuses = await this.refreshStatus();
      if (Object.values(statuses || {}).some(s => s?.running)) {
        this._startPoll();
      } else {
        const sessions = await GlioTwin.fetch('/api/segmentation/sessions').catch(() => null);
        if (sessions) this.state.sessions = sessions.sessions || [];
      }
      this.render();
    }, 3000);
  },

  async runSelected() {
    const ids = this.selectedIds();
    const model = this.modelInfo();
    const modelStatus = this.state.jobStatus?.[model.key];
    if (!ids.length) { GlioTwin.toast('Seleziona almeno una sessione', 'error'); return; }
    if (modelStatus?.running) { GlioTwin.toast('Segmentazione già in corso per questo modello', 'error'); return; }
    try {
      await GlioTwin.post(model.run_endpoint, { session_ids: ids, force: false });
      GlioTwin.toast(`Avviata segmentazione su ${ids.length} sessione/i`, 'info');
      this.state.selected = {};
      this.state.jobStatus = {
        ...this.state.jobStatus,
        [model.key]: { running: true, current: 0, total: ids.length, last_msg: 'Avvio…' },
      };
      this.render();
      this._startPoll();
    } catch (e) { GlioTwin.toast(e.message, 'error'); }
  },

  async deleteSegmentation(sessionId, modelName) {
    try {
      const res = await fetch('/api/segmentation/structures', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ session_id: sessionId, model_name: modelName }),
      });
      if (!res.ok) throw new Error((await res.json().catch(() => ({}))).detail || res.statusText);
      const data = await res.json();
      GlioTwin.toast(`Cancellate ${data.deleted} strutture (${modelName})`, 'info');
      const sessions = await GlioTwin.fetch('/api/segmentation/sessions');
      this.state.sessions = sessions.sessions || [];
      this.render();
    } catch (e) { GlioTwin.toast(e.message, 'error'); }
  },

  _modelChips(session) {
    const model = this.state.selectedModel;
    const done = session.segmented_models || [];
    if (done.includes(model)) {
      return `
        <span class="seg-chip seg-chip--done" title="${model}">✓ ${model}</span>
        <button class="seg-delete-btn" data-sid="${session.session_id}" data-model="${model}" title="Cancella e ripeti">✕</button>
      `;
    }
    if (!session.preprocessing_ready) {
      return `<span class="seg-chip seg-chip--wait" title="Preprocessing non completato">Preprocessing assente</span>`;
    }
    return `<span class="seg-chip seg-chip--pending">Non segmentato</span>`;
  },

  render() {
    const app = document.getElementById('app');
    if (!app) return;

    const job    = this.state.jobStatus?.[this.state.selectedModel];
    const rows   = this.filtered();
    const selIds = this.selectedIds();
    const allChecked = rows.length > 0 && rows.every(s => this.state.selected[s.session_id]);
    const model  = this.modelInfo();

    const doneCount    = this.state.sessions.filter(s => (s.segmented_models || []).includes(this.state.selectedModel)).length;
    const pendingCount = this.state.sessions.filter(s => !(s.segmented_models || []).includes(this.state.selectedModel) && s.preprocessing_ready).length;

    app.innerHTML = `
      <div class="seg-layout">

        <div class="seg-header">
          <div>
            <div class="seg-title">Segmentation</div>
            <div class="seg-subtitle">Seleziona pazienti e timepoint, scegli il modello e avvia la segmentazione.</div>
          </div>
          <div class="seg-header-actions">
            <button class="btn btn-primary" id="seg-run-btn" ${job?.running || !selIds.length ? 'disabled' : ''}>
              ▶ Segmenta selezionati (${selIds.length})
            </button>
          </div>
        </div>

        ${job?.running ? `
          <div class="seg-status-bar seg-status-bar--running">
            <div class="rh-status-spinner"></div>
            <span>Segmentazione in corso… ${job.current}/${job.total}
              ${job.last_msg ? '· ' + job.last_msg : ''}</span>
          </div>
        ` : job?.result ? `
          <div class="seg-status-bar seg-status-bar--ok">
            ✓ ${job.last_msg || 'Completato'}
          </div>
        ` : job?.error ? `
          <div class="seg-status-bar seg-status-bar--err">✗ ${job.error}</div>
        ` : ''}

        <div class="seg-toolbar">
          <div class="seg-model-selector">
            ${SEG_MODELS.map(m => `
              <button class="seg-model-btn ${m.key === this.state.selectedModel ? 'selected' : ''}"
                      data-model="${m.key}" title="${m.description}">
                <span class="seg-model-label">${m.label}</span>
                <span class="seg-model-counts">
                  <span class="seg-count-done">${doneCount} ✓</span>
                  <span class="seg-count-pending">${pendingCount} in attesa</span>
                </span>
              </button>
            `).join('')}
          </div>

          <div class="seg-filters">
            <label class="seg-filter-check">
              <input type="checkbox" id="seg-filter-preproc" ${this.state.filterPreproc ? 'checked' : ''}>
              Solo preprocessing pronto
            </label>
            <input class="f-search" id="seg-filter-q" placeholder="Cerca paziente…" value="${this.state.filterQ}">
          </div>
        </div>

        ${this.state.loading ? `
          <div class="gm-loading"><div class="spinner"></div><span>Caricamento…</span></div>
        ` : `
          <div class="seg-table-wrap">
            <table class="import-table seg-table">
              <thead>
                <tr>
                  <th>
                    <input type="checkbox" id="seg-select-all" ${allChecked ? 'checked' : ''}>
                  </th>
                  <th>Paziente</th>
                  <th>Timepoint</th>
                  <th>Data</th>
                  <th>Dataset</th>
                  <th>Preprocessing</th>
                  <th>${model.label}</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                ${rows.length ? rows.map(s => {
                  const checked = !!this.state.selected[s.session_id];
                  const canSelect = s.preprocessing_ready && !(s.segmented_models || []).includes(this.state.selectedModel);
                  return `
                    <tr class="${checked ? 'selected' : ''}">
                      <td>
                        <input type="checkbox" class="seg-cb"
                               data-sid="${s.session_id}"
                               ${checked ? 'checked' : ''}
                               ${canSelect ? '' : 'disabled'}
                               title="${canSelect ? '' : !s.preprocessing_ready ? 'Preprocessing non completato' : 'Già segmentato con questo modello'}">
                      </td>
                      <td>
                        <div class="import-cell-title">${GlioTwin.patientPrimary(s)}</div>
                        <div class="import-cell-sub">${GlioTwin.state.showSensitive ? GlioTwin.patientSecondary(s) : ''}</div>
                      </td>
                      <td>
                        <span class="tp-pill tp-${s.timepoint_type || 'other'}">${(s.timepoint_type || 'other').replace('_', ' ')}</span>
                        <span class="import-cell-sub">${s.session_label}</span>
                      </td>
                      <td class="import-cell-sub">${GlioTwin.fmtDate(s.study_date)}</td>
                      <td>${GlioTwin.datasetBadge(s.dataset)}</td>
                      <td>
                        ${s.preprocessing_ready
                          ? '<span class="seg-chip seg-chip--done">✓ Pronto</span>'
                          : '<span class="seg-chip seg-chip--wait">Mancante</span>'}
                      </td>
                      <td>${this._modelChips(s)}</td>
                      <td>
                        <button class="btn btn-linklike seg-viewer-btn"
                                data-pid="${s.patient_id}" data-sid="${s.session_id}">Viewer</button>
                      </td>
                    </tr>
                  `;
                }).join('') : `
                  <tr><td colspan="8" class="import-empty-cell">Nessuna sessione trovata.</td></tr>
                `}
              </tbody>
            </table>
          </div>
        `}
      </div>
    `;

    this._bindEvents(app);
  },

  _bindEvents(app) {
    app.querySelector('#seg-run-btn')?.addEventListener('click', () => this.runSelected());

    app.querySelector('#seg-select-all')?.addEventListener('change', e => {
      const rows = this.filtered();
      rows.forEach(s => {
        const canSelect = s.preprocessing_ready && !(s.segmented_models || []).includes(this.state.selectedModel);
        if (canSelect) this.state.selected[s.session_id] = e.target.checked;
      });
      this.render();
    });

    app.querySelectorAll('.seg-cb').forEach(el => {
      el.addEventListener('change', e => {
        this.state.selected[parseInt(el.dataset.sid)] = e.target.checked;
        this.render();
      });
    });

    app.querySelectorAll('.seg-model-btn').forEach(el => {
      el.addEventListener('click', () => {
        this.state.selectedModel = el.dataset.model;
        this.state.selected = {};
        this.render();
      });
    });

    app.querySelector('#seg-filter-preproc')?.addEventListener('change', e => {
      this.state.filterPreproc = e.target.checked;
      this.render();
    });

    app.querySelector('#seg-filter-q')?.addEventListener('input', e => {
      this.state.filterQ = e.target.value;
      this.render();
    });

    app.querySelectorAll('.seg-delete-btn').forEach(el => {
      el.addEventListener('click', () => {
        const sid   = parseInt(el.dataset.sid);
        const model = el.dataset.model;
        if (confirm(`Cancellare la segmentazione "${model}" per questa sessione?`)) {
          this.deleteSegmentation(sid, model);
        }
      });
    });

    app.querySelectorAll('.seg-viewer-btn').forEach(el => {
      el.addEventListener('click', () => {
        GlioTwin.state.currentPatient = parseInt(el.dataset.pid);
        GlioTwin.state.currentSession = parseInt(el.dataset.sid);
        location.hash = '#/browser';
      });
    });
  },
};

GlioTwin.register('rhglioseg', async (app) => {
  app.innerHTML = `<div class="gm-loading"><div class="spinner"></div><span>Caricamento…</span></div>`;
  await SegView.load();
});
