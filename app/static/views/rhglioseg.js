'use strict';

const SEG_MODELS = [
  {
    key: 'fets_postop',
    label: 'FeTS Post-op',
    description: 'Tumor segmentation FeTS su casi già preprocessati',
    run_endpoint: '/api/fetsseg/run',
    status_endpoint: '/api/fetsseg/status',
  },
  {
    key: 'rh-glioseg-v3',
    label: 'rh-GlioSeg v3',
    description: 'nnUNet 5-fold ensemble · Dataset016_RH-GlioSeg_v3',
    run_endpoint: '/api/rhglioseg/run',
    status_endpoint: '/api/rhglioseg/status',
  },
];

const SegView = {
  state: {
    sessions:        [],
    selectedByModel: {},   // { modelKey: { sessionId: bool } }
    filterPreproc:   false,
    filterQ:         '',
    jobStatus:       {},
    pollTimer:       null,
    loading:         true,
  },

  _initSelected() {
    for (const m of SEG_MODELS) {
      if (!this.state.selectedByModel[m.key]) this.state.selectedByModel[m.key] = {};
    }
  },

  selectedForModel(modelKey) {
    return Object.entries(this.state.selectedByModel[modelKey] || {})
      .filter(([, v]) => v).map(([k]) => parseInt(k, 10));
  },

  totalSelected() {
    const ids = new Set();
    for (const m of SEG_MODELS) this.selectedForModel(m.key).forEach(id => ids.add(id));
    return ids.size;
  },

  canSelect(session, modelKey) {
    return session.preprocessing_ready && !(session.segmented_models || []).includes(modelKey);
  },

  filtered() {
    const q = this.state.filterQ.trim().toLowerCase();
    return this.state.sessions.filter(s => {
      if (this.state.filterPreproc && !s.preprocessing_ready) return false;
      if (!q) return true;
      return [s.subject_id, s.patient_name, s.patient_given_name, s.patient_family_name, s.session_label, s.dataset]
        .filter(Boolean).join(' ').toLowerCase().includes(q);
    });
  },

  async load() {
    this._initSelected();
    this.state.loading = true;
    this.render();
    const [sessions, ...statuses] = await Promise.all([
      GlioTwin.fetch('/api/segmentation/sessions'),
      ...SEG_MODELS.map(m => GlioTwin.fetch(m.status_endpoint).catch(() => null)),
    ]);
    this.state.sessions  = sessions.sessions || [];
    this.state.jobStatus = Object.fromEntries(SEG_MODELS.map((m, i) => [m.key, statuses[i]]));
    this.state.loading   = false;
    this.render();
    if (statuses.some(s => s?.running)) this._startPoll();
  },

  async refreshStatus() {
    const statuses = await Promise.all(
      SEG_MODELS.map(m => GlioTwin.fetch(m.status_endpoint).catch(() => null))
    );
    this.state.jobStatus = Object.fromEntries(SEG_MODELS.map((m, i) => [m.key, statuses[i]]));
    return this.state.jobStatus;
  },

  _startPoll() {
    clearTimeout(this.state.pollTimer);
    this.state.pollTimer = setTimeout(async () => {
      if (!location.hash.startsWith('#/rhglioseg')) return;
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
    const tasks = SEG_MODELS
      .map(m => ({ model: m, ids: this.selectedForModel(m.key) }))
      .filter(t => t.ids.length > 0);

    if (!tasks.length) { GlioTwin.toast('Seleziona almeno una sessione per almeno un modello', 'error'); return; }

    const busy = tasks.filter(t => this.state.jobStatus?.[t.model.key]?.running);
    if (busy.length) {
      GlioTwin.toast(`Già in corso: ${busy.map(t => t.model.label).join(', ')}`, 'error');
      return;
    }
    try {
      await Promise.all(tasks.map(t => GlioTwin.post(t.model.run_endpoint, { session_ids: t.ids, force: false })));
      const n = new Set(tasks.flatMap(t => t.ids)).size;
      GlioTwin.toast(`Avviate ${tasks.length} segmentazioni su ${n} sessione/i`, 'info');
      this.state.selectedByModel = {};
      this._initSelected();
      for (const t of tasks) {
        this.state.jobStatus[t.model.key] = { running: true, current: 0, total: t.ids.length, last_msg: 'Avvio…' };
      }
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

  _modelCell(session, model) {
    const done = (session.segmented_models || []).includes(model.key);
    if (done) {
      return `
        <div class="seg-model-done">
          <span class="seg-chip seg-chip--done" title="${model.key}">✓</span>
          <button class="seg-delete-btn" data-sid="${session.session_id}" data-model="${model.key}" title="Cancella e ripeti">✕</button>
        </div>`;
    }
    if (!session.preprocessing_ready) {
      return `<span class="seg-chip seg-chip--wait" title="Preprocessing non completato">—</span>`;
    }
    const checked = !!this.state.selectedByModel[model.key]?.[session.session_id];
    return `
      <label class="seg-cb-label">
        <input type="checkbox" class="seg-model-cb"
               data-sid="${session.session_id}" data-model="${model.key}"
               ${checked ? 'checked' : ''}>
        <span class="seg-cb-mark"></span>
      </label>`;
  },

  _allChecked(modelKey, rows) {
    const eligible = rows.filter(s => this.canSelect(s, modelKey));
    return eligible.length > 0 && eligible.every(s => this.state.selectedByModel[modelKey]?.[s.session_id]);
  },

  _statusBar(model) {
    const job = this.state.jobStatus?.[model.key];
    if (!job) return '';
    if (job.running) return `
      <div class="seg-status-bar seg-status-bar--running">
        <div class="rh-status-spinner"></div>
        <span><strong>${model.label}</strong> — In corso ${job.current}/${job.total}${job.last_msg ? ' · ' + job.last_msg : ''}</span>
      </div>`;
    if (job.error) return `
      <div class="seg-status-bar seg-status-bar--err"><strong>${model.label}</strong> — ✗ ${job.error}</div>`;
    if (job.result) return `
      <div class="seg-status-bar seg-status-bar--ok"><strong>${model.label}</strong> — ✓ ${job.last_msg || 'Completato'}</div>`;
    return '';
  },

  render() {
    const app = document.getElementById('app');
    if (!app) return;

    const rows      = this.filtered();
    const totalSel  = this.totalSelected();
    const anyRunning = SEG_MODELS.some(m => this.state.jobStatus?.[m.key]?.running);
    const nCols     = 5 + SEG_MODELS.length + 1;

    app.innerHTML = `
      <div class="seg-layout">

        <div class="seg-header">
          <div>
            <div class="seg-title">Segmentation</div>
            <div class="seg-subtitle">
              Seleziona le sessioni e i modelli da applicare, poi premi Segmenta.
              Le colonne modello mostrano lo stato per ciascun caso.
            </div>
          </div>
          <div class="seg-header-actions">
            <input class="f-search" id="seg-filter-q" placeholder="Cerca paziente, ID, dataset…" value="${this.state.filterQ}" style="width:180px">
            <label class="seg-filter-check">
              <input type="checkbox" id="seg-filter-preproc" ${this.state.filterPreproc ? 'checked' : ''}>
              Solo preprocessing pronto
            </label>
            <button class="btn btn-primary" id="seg-run-btn" ${anyRunning || !totalSel ? 'disabled' : ''}>
              ▶ Segmenta (${totalSel})
            </button>
          </div>
        </div>

        ${SEG_MODELS.map(m => this._statusBar(m)).join('')}

        ${this.state.loading ? `
          <div class="gm-loading"><div class="spinner"></div><span>Caricamento…</span></div>
        ` : `
          <div class="seg-table-wrap">
            <table class="import-table seg-table">
              <thead>
                <tr>
                  <th>Paziente</th>
                  <th>Timepoint</th>
                  <th>Dataset</th>
                  <th>Data</th>
                  <th>Preprocessing</th>
                  ${SEG_MODELS.map(m => `
                    <th class="seg-model-col">
                      <div class="seg-model-head">
                        <label class="seg-cb-label" title="Seleziona tutti idonei per ${m.label}">
                          <input type="checkbox" class="seg-all-cb" data-model="${m.key}"
                                 ${this._allChecked(m.key, rows) ? 'checked' : ''}>
                          <span class="seg-cb-mark"></span>
                        </label>
                        <span>${m.label}</span>
                      </div>
                    </th>
                  `).join('')}
                  <th></th>
                </tr>
              </thead>
              <tbody>
                ${rows.length ? rows.map(s => {
                  const rowSel = SEG_MODELS.some(m => this.state.selectedByModel[m.key]?.[s.session_id]);
                  return `
                    <tr class="${rowSel ? 'selected' : ''}">
                      <td>
                        <div class="import-cell-title">${GlioTwin.patientPrimary(s)}</div>
                        <div class="import-cell-sub">${GlioTwin.state.showSensitive ? GlioTwin.patientSecondary(s) : s.subject_id}</div>
                      </td>
                      <td>
                        <span class="tp-pill tp-${s.timepoint_type || 'other'}">${(s.timepoint_type || 'other').replace('_', ' ')}</span>
                        <div class="import-cell-sub">${s.session_label}</div>
                      </td>
                      <td>${GlioTwin.datasetBadge(s.dataset)}</td>
                      <td class="import-cell-sub">${GlioTwin.fmtDate(s.study_date)}</td>
                      <td>
                        ${s.preprocessing_ready
                          ? '<span class="seg-chip seg-chip--done">✓ pronto</span>'
                          : '<span class="seg-chip seg-chip--wait">mancante</span>'}
                      </td>
                      ${SEG_MODELS.map(m => `<td class="seg-model-col">${this._modelCell(s, m)}</td>`).join('')}
                      <td>
                        <button class="btn btn-linklike seg-viewer-btn"
                                data-pid="${s.patient_id}" data-sid="${s.session_id}">Viewer</button>
                      </td>
                    </tr>
                  `;
                }).join('') : `
                  <tr><td colspan="${nCols}" class="import-empty-cell">Nessuna sessione trovata.</td></tr>
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

    app.querySelector('#seg-filter-preproc')?.addEventListener('change', e => {
      this.state.filterPreproc = e.target.checked;
      this.render();
    });

    app.querySelector('#seg-filter-q')?.addEventListener('input', e => {
      this.state.filterQ = e.target.value;
      this.render();
    });

    app.querySelectorAll('.seg-all-cb').forEach(el => {
      el.addEventListener('change', e => {
        const key  = el.dataset.model;
        const rows = this.filtered();
        if (!this.state.selectedByModel[key]) this.state.selectedByModel[key] = {};
        rows.forEach(s => {
          if (this.canSelect(s, key)) this.state.selectedByModel[key][s.session_id] = e.target.checked;
        });
        this.render();
      });
    });

    app.querySelectorAll('.seg-model-cb').forEach(el => {
      el.addEventListener('change', e => {
        const sid = parseInt(el.dataset.sid);
        const key = el.dataset.model;
        if (!this.state.selectedByModel[key]) this.state.selectedByModel[key] = {};
        this.state.selectedByModel[key][sid] = e.target.checked;
        this.render();
      });
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
  SegView._initSelected();
  await SegView.load();
});
