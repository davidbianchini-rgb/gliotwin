const WorkspaceView = {
  state: {
    overview: null,
    caseData: null,
    system: null,
    selectedJobId: null,
    selectedSessions: {},
    filters: {
      status: 'active',
      q: '',
    },
    logText: '',
    loading: false,
    pollHandle: null,
  },

  resetPolling() {
    if (this.state.pollHandle) {
      clearInterval(this.state.pollHandle);
      this.state.pollHandle = null;
    }
  },

  async loadOverview() {
    this.state.loading = true;
    const [overview, system] = await Promise.all([
      GlioTwin.fetch('/api/workspace'),
      GlioTwin.fetch('/api/system/status'),
    ]);
    this.state.overview = overview;
    this.state.system = system;
    this.state.loading = false;
  },

  async refreshOverview() {
    const [overview, system] = await Promise.all([
      GlioTwin.fetch('/api/workspace'),
      GlioTwin.fetch('/api/system/status'),
    ]);
    this.state.overview = overview;
    this.state.system = system;
  },

  async loadCase(sessionId) {
    this.state.loading = true;
    this.state.caseData = await GlioTwin.fetch(`/api/workspace/${sessionId}`);
    this.state.selectedJobId = this.state.caseData.jobs?.[0]?.id || null;
    if (this.state.selectedJobId) {
      const log = await GlioTwin.fetch(`/api/processing/jobs/${this.state.selectedJobId}/log?tail=16000`);
      this.state.logText = log.text || '';
    } else {
      this.state.logText = '';
    }
    this.state.loading = false;
  },

  async refreshCase(sessionId) {
    this.state.caseData = await GlioTwin.fetch(`/api/workspace/${sessionId}`);
    const jobs = this.state.caseData.jobs || [];
    if (!jobs.some(job => job.id === this.state.selectedJobId)) {
      this.state.selectedJobId = jobs[0]?.id || null;
    }
    if (this.state.selectedJobId) {
      const log = await GlioTwin.fetch(`/api/processing/jobs/${this.state.selectedJobId}/log?tail=16000`);
      this.state.logText = log.text || '';
    } else {
      this.state.logText = '';
    }
  },

  selectedSessionIds() {
    return Object.entries(this.state.selectedSessions)
      .filter(([, checked]) => checked)
      .map(([id]) => parseInt(id, 10));
  },

  filteredSessions() {
    const overview = this.state.overview || { sessions: [] };
    const q = (this.state.filters.q || '').trim().toLowerCase();
    return (overview.sessions || []).filter(item => {
      if (this.state.filters.status === 'active' && !['ready', 'queued', 'running', 'failed', 'cancelled'].includes(item.operational_status)) {
        return false;
      }
      if (this.state.filters.status !== 'all' && this.state.filters.status !== 'active' && item.operational_status !== this.state.filters.status) {
        return false;
      }
      if (!q) return true;
      return [
        item.patient_code,
        item.patient_name,
        item.patient_given_name,
        item.patient_family_name,
        item.session_label,
      ].filter(Boolean).join(' ').toLowerCase().includes(q);
    });
  },

  statusBadge(status) {
    const map = {
      ready: 'badge-import-ready',
      queued: 'badge-import-review',
      running: 'badge-import-review',
      failed: 'badge-import-incomplete',
      cancelled: 'badge-import-incomplete',
      completed: 'badge-import-ready',
      incomplete: 'badge-import-incomplete',
    };
    const cls = map[status] || 'badge-import-review';
    return `<span class="badge ${cls}">${status}</span>`;
  },

  stepBadge(status) {
    const map = {
      pending: 'badge-status-pending',
      running: 'badge-status-running',
      done: 'badge-status-completed',
      failed: 'badge-status-failed',
    };
    const cls = map[status] || 'badge-status-pending';
    return `<span class="badge ${cls}">${status}</span>`;
  },

  renderPipelineSummary(item) {
    const summary = item.pipeline_state?.steps || [];
    const seqs = item.sequences || [];
    const computed = item.computed_structures || [];

    // derive highest completed phase for compact display
    const stepDone  = key => summary.find(s => s.key === key)?.status === 'done';
    const stepFailed= key => summary.find(s => s.key === key)?.status === 'failed';
    const hasFets   = computed.some(s => s.model_name && s.model_name !== 'rh-glioseg-v3');
    const hasRhg    = computed.some(s => s.model_name === 'rh-glioseg-v3');

    if (stepFailed('tumor_segmentation'))
      return `<span class="workspace-step-inline failed">Segmentazione fallita</span>`;
    if (hasFets || hasRhg)
      return `<span class="workspace-step-inline done">Segmentazione completata</span>`;
    if (stepDone('brain_extraction'))
      return `<span class="workspace-step-inline done">Preprocessing completato</span>`;
    if (stepDone('nifti_conversion'))
      return `<span class="workspace-step-inline done">NIfTI convertito</span>`;
    if (seqs.some(s => s.raw_path))
      return `<span class="workspace-step-inline done">Import completato</span>`;
    return `<span class="workspace-step-inline pending">Nessuna attività</span>`;
  },

  _buildChecklist(data) {
    const steps    = data.pipeline_state?.steps || [];
    const seqs     = data.sequences || [];
    const computed = data.computed_structures || [];
    const jobs     = data.jobs || [];

    const stepStatus = key => steps.find(s => s.key === key)?.status || 'pending';
    const stepError  = key => steps.find(s => s.key === key)?.error_message || '—';

    const CORE = ['T1', 'T1ce', 'T2', 'FLAIR'];
    const coreWithRaw  = seqs.filter(s => CORE.includes(s.sequence_type) && s.raw_path);
    const coreWithProc = seqs.filter(s => CORE.includes(s.sequence_type) && s.processed_path);
    const jobRunning   = jobs.some(j => j.status === 'running');

    const nativeStructs = computed.filter(s => s.reference_space === 'native');
    const fetsStructs   = computed.filter(s => s.model_name && s.model_name !== 'rh-glioseg-v3');
    const rhgStructs    = computed.filter(s => s.model_name === 'rh-glioseg-v3');

    // IMPORT
    const importOk = coreWithRaw.length >= 2;
    const importStatus = importOk ? 'ok' : 'blocked';
    const importDetail = importOk
      ? `${coreWithRaw.length} serie core (${coreWithRaw.map(s => s.sequence_type).join(', ')})`
      : `Solo ${coreWithRaw.length} serie core con raw_path (min. 2)`;

    // PREPROCESSING
    const niiftiDone = stepStatus('nifti_conversion') === 'done';
    const brainDone  = stepStatus('brain_extraction') === 'done';
    const preprocOk  = niiftiDone && brainDone;
    const preprocStatus =
      preprocOk                                       ? 'ok'      :
      stepStatus('nifti_conversion') === 'failed' ||
      stepStatus('brain_extraction') === 'failed'     ? 'blocked' :
      jobRunning                                      ? 'running' :
      coreWithProc.length > 0                         ? 'partial' : 'pending';
    const preprocDetail =
      preprocStatus === 'ok'      ? `NIfTI + brain extraction completati` :
      preprocStatus === 'blocked' ? `Step fallito: ${stepError('nifti_conversion') !== '—' ? stepError('nifti_conversion') : stepError('brain_extraction')}` :
      preprocStatus === 'running' ? 'In esecuzione…'                      :
      preprocStatus === 'partial' ? `${coreWithProc.length} serie preprocessate (parziale)` :
      !importOk                   ? 'Richiede import completato'           :
                                    'Non avviato — usa Queue nella lista sessioni';

    // SEGMENTATION — FeTS
    const segStep    = stepStatus('tumor_segmentation');
    const fetsStatus =
      fetsStructs.length > 0   ? 'ok'      :
      segStep === 'failed'      ? 'blocked' :
      (jobRunning && segStep === 'running') ? 'running' :
      preprocOk                 ? 'pending' : 'waiting';
    const fetsDetail =
      fetsStatus === 'ok'      ? `${fetsStructs.length} strutture FeTS` :
      fetsStatus === 'blocked' ? `Errore: ${stepError('tumor_segmentation')}` :
      fetsStatus === 'running' ? 'Segmentazione FeTS in esecuzione…'     :
      fetsStatus === 'pending' ? 'Preprocessing pronto — avvia dalla lista sessioni' :
                                 'Richiede preprocessing completato';

    // SEGMENTATION — rh-GlioSeg
    const rhgStatus =
      rhgStructs.length > 0 ? 'ok'     :
      preprocOk              ? 'pending': 'waiting';
    const rhgDetail =
      rhgStatus === 'ok'     ? `${rhgStructs.length} strutture rh-GlioSeg`          :
      rhgStatus === 'pending'? 'Avviabile dalla view Segmentation'                  :
                               'Richiede preprocessing completato';

    // ANALYSIS
    const analysisStatus = nativeStructs.length > 0 ? 'ok' : 'waiting';
    const analysisDetail = nativeStructs.length > 0
      ? `${nativeStructs.length} strutture in spazio nativo — metriche calcolabili`
      : 'Richiede almeno una struttura completata';

    // EXPORT
    const exportStatus = nativeStructs.length > 0 ? 'ok' : 'waiting';
    const exportDetail = nativeStructs.length > 0
      ? 'Strutture in spazio nativo disponibili per RTSTRUCT'
      : 'Richiede strutture in spazio nativo';

    return [
      { phase: 'IMPORT',        label: 'Serie core riconosciute',  status: importStatus,   detail: importDetail   },
      { phase: 'PREPROCESSING', label: 'Preprocessing',            status: preprocStatus,  detail: preprocDetail  },
      { phase: 'SEGMENTATION',  label: 'Segmentazione FeTS',       status: fetsStatus,     detail: fetsDetail     },
      { phase: 'SEGMENTATION',  label: 'Segmentazione rh-GlioSeg', status: rhgStatus,      detail: rhgDetail      },
      { phase: 'ANALYSIS',      label: 'Metriche disponibili',     status: analysisStatus, detail: analysisDetail },
      { phase: 'EXPORT',        label: 'Export RTSTRUCT',          status: exportStatus,   detail: exportDetail   },
    ];
  },

  renderChecklistCard(data) {
    const items = this._buildChecklist(data);
    const icons = { ok: '✓', blocked: '✗', running: '◌', partial: '◑', pending: '○', waiting: '·' };
    const PHASE_COLORS = {
      IMPORT:        'var(--accent)',
      PREPROCESSING: '#f97316',
      SEGMENTATION:  '#a78bfa',
      ANALYSIS:      '#22d3ee',
      EXPORT:        '#4ade80',
    };

    let lastPhase = null;
    const rows = items.map(item => {
      const phaseHeader = item.phase !== lastPhase
        ? `<div class="cl-phase-label" style="color:${PHASE_COLORS[item.phase] || 'var(--text-dim)'}">${item.phase}</div>`
        : '';
      lastPhase = item.phase;
      return `
        ${phaseHeader}
        <div class="cl-row cl-row-${item.status}">
          <span class="cl-icon cl-icon-${item.status}" title="${item.status}">${icons[item.status] || '○'}</span>
          <div class="cl-body">
            <span class="cl-label">${item.label}</span>
            <span class="cl-detail">${item.detail}</span>
          </div>
        </div>
      `;
    }).join('');

    return `
      <div class="card cl-card">
        <div class="card-title">Checklist di fase</div>
        <div class="cl-list">${rows}</div>
      </div>
    `;
  },

  renderPipelineCard(pipelineState) {
    const steps = pipelineState?.steps || [];
    if (!steps.length) {
      return `
        <div class="card">
          <div class="card-title">Pipeline State</div>
          <div class="workspace-empty-block">No pipeline state for this case yet.</div>
        </div>
      `;
    }
    return `
      <div class="card">
        <div class="card-title">Pipeline State</div>
        <div class="workspace-pipeline-caption">Stato lineare per questo caso. L'ultimo step completato e l'eventuale errore sono evidenziati direttamente qui.</div>
        <div class="workspace-pipeline-list">
          ${steps.map(step => `
            <div class="workspace-pipeline-step ${step.status}">
              <div class="workspace-pipeline-head">
                <strong>${step.step_name}</strong>
                ${this.stepBadge(step.status)}
              </div>
              <div class="workspace-pipeline-grid">
                <div class="workspace-pipeline-meta"><span>Input</span><strong>${GlioTwin.fmt(step.input_expected)}</strong></div>
                <div class="workspace-pipeline-meta"><span>Output</span><strong>${GlioTwin.fmt(step.output_expected)}</strong></div>
                <div class="workspace-pipeline-meta workspace-pipeline-meta-wide"><span>Path</span><strong>${GlioTwin.fmt(step.output_path)}</strong></div>
                <div class="workspace-pipeline-meta"><span>Start</span><strong>${GlioTwin.fmt(step.started_at)}</strong></div>
                <div class="workspace-pipeline-meta"><span>Fine</span><strong>${GlioTwin.fmt(step.finished_at)}</strong></div>
              </div>
              ${step.error_message ? `<div class="workspace-pipeline-error">${step.error_message}</div>` : ''}
            </div>
          `).join('')}
        </div>
      </div>
    `;
  },

  renderSystemCard() {
    const s = this.state.system || {};
    const mem = s.memory || {};
    const gpu = (s.gpus || [])[0];
    return `
      <div class="card">
        <div class="card-title">Machine Status</div>
        <div class="workspace-metrics">
          <div class="workspace-metric"><span>CPU</span><strong>${s.cpu_percent != null ? s.cpu_percent + '%' : '—'}</strong></div>
          <div class="workspace-metric"><span>Load</span><strong>${s.loadavg ? s.loadavg.join(' / ') : '—'}</strong></div>
          <div class="workspace-metric"><span>RAM</span><strong>${mem.used_percent != null ? mem.used_percent + '%' : '—'}</strong></div>
          <div class="workspace-metric"><span>Avail RAM</span><strong>${mem.available_mb != null ? mem.available_mb + ' MB' : '—'}</strong></div>
          <div class="workspace-metric"><span>GPU</span><strong>${gpu ? gpu.utilization_gpu + '%' : '—'}</strong></div>
          <div class="workspace-metric"><span>GPU RAM</span><strong>${gpu ? `${gpu.memory_used_mb}/${gpu.memory_total_mb} MB` : '—'}</strong></div>
        </div>
      </div>
    `;
  },

  renderCurrentJobCard() {
    const overview = this.state.overview || {};
    const current = overview.current_job || null;
    const elapsed = current?.elapsed_seconds != null
      ? `${Math.floor(current.elapsed_seconds / 60)}m ${current.elapsed_seconds % 60}s`
      : '—';
    return `
      <div class="card">
        <div class="card-title">Current Job</div>
        ${current ? `
          <div class="workspace-current-job">
            <div class="workspace-current-main">${GlioTwin.patientPrimary(current)} · ${current.session_label}</div>
            <div class="workspace-current-sub">Job #${current.id} · ${current.progress_label || current.progress_stage || current.status}</div>
            ${GlioTwin.state.showSensitive && GlioTwin.sessionMeta(current) ? `<div class="workspace-current-sub">${GlioTwin.sessionMeta(current)}</div>` : ''}
            <div class="workspace-current-sub">Status: ${current.status}</div>
            <div class="workspace-current-sub">Elapsed: ${elapsed}</div>
            <div class="workspace-current-actions">
              <button class="btn workspace-cancel-job" data-job-id="${current.id}">Stop Current Job</button>
              <button class="btn workspace-stop-all">Stop All</button>
              <button class="btn btn-linklike workspace-open-case" data-session-id="${current.session_id}">Open Case</button>
            </div>
          </div>
        ` : `
          <div class="workspace-empty-block">No job running.</div>
          <div class="workspace-current-actions" style="margin-top:10px">
            <button class="btn workspace-stop-all">Stop All</button>
          </div>
        `}
      </div>
    `;
  },

  renderSessionRow(item) {
    const checked = !!this.state.selectedSessions[item.session_id];
    const coreSummary = item.sequences
      .filter(seq => ['T1', 'T1ce', 'T2', 'FLAIR'].includes(seq.sequence_type))
      .map(seq => `<span class="import-core-pill ${seq.raw_path ? 'ok' : 'missing'}">${seq.sequence_type}</span>`)
      .join('');
    return `
      <tr>
        <td><input type="checkbox" class="workspace-session-cb" data-session-id="${item.session_id}" ${checked ? 'checked' : ''}></td>
        <td>
          <div class="import-cell-title">${GlioTwin.patientPrimary(item)}</div>
          <div class="import-cell-sub">
            ${item.session_label}${GlioTwin.state.showSensitive && GlioTwin.sessionMeta(item) ? ` · ${GlioTwin.sessionMeta(item)}` : ''}
          </div>
        </td>
        <td>${this.statusBadge(item.operational_status)}</td>
        <td><div class="import-core-strip">${coreSummary}</div></td>
        <td>
          ${item.latest_job ? `#${item.latest_job.id} · ${item.latest_job.progress_label || item.latest_job.progress_stage || item.latest_job.status}` : '—'}
          <div>${this.renderPipelineSummary(item)}</div>
        </td>
        <td>
          <button class="btn btn-linklike workspace-open-case" data-session-id="${item.session_id}">Open</button>
        </td>
      </tr>
    `;
  },

  renderJob(job) {
    return `
      <div class="workspace-job-overview">
        <div class="workspace-job-row">
          <span class="workspace-job-main">#${job.id} · ${GlioTwin.patientPrimary(job)} · ${job.session_label}</span>
          <span class="workspace-job-sub">${job.status} · ${GlioTwin.fmt(job.progress_label || job.progress_stage)}</span>
        </div>
        <div class="workspace-job-actions">
          ${job.status === 'running' ? `<button class="btn workspace-cancel-job" data-job-id="${job.id}">Stop</button>` : ''}
          ${job.status === 'queued' ? `<button class="btn workspace-remove-job" data-job-id="${job.id}">Remove</button>` : ''}
          <button class="btn btn-linklike workspace-open-case" data-session-id="${job.session_id}">Open Case</button>
        </div>
      </div>
    `;
  },

  renderCase(app, arg) {
    const data = this.state.caseData;
    if (!data) {
      app.innerHTML = `<div class="loading-screen"><div class="spinner"></div><p>Loading case…</p></div>`;
      return;
    }
    const selectedJob = (data.jobs || []).find(job => job.id === this.state.selectedJobId) || data.jobs?.[0] || null;
    app.innerHTML = `
      <div class="workspace-view">
        <div class="workspace-header">
          <div>
            <div class="import-title">Preprocessing — Case Detail</div>
            <div class="import-subtitle">
              ${GlioTwin.patientPrimary(data.subject)} · ${data.session.session_label}
              ${GlioTwin.state.showSensitive && GlioTwin.sessionMeta(data.session) ? ` · ${GlioTwin.sessionMeta(data.session)}` : ''}
            </div>
          </div>
          <div class="workspace-header-actions">
            <button class="btn" id="workspace-back">Back to Queue</button>
            <button class="btn" id="workspace-open-viewer" ${!data.computed_structures.length ? 'disabled' : ''}>Open Viewer</button>
          </div>
        </div>
        <div class="workspace-grid">
          <section class="workspace-main">
            ${this.renderChecklistCard(data)}
            <div class="card">
              <div class="card-title">Pipeline Steps</div>
              <div class="workspace-step-grid">
                ${data.steps.map(step => `
                  <div class="workspace-step-card">
                    <div class="workspace-step-title">${step.label}</div>
                    <span class="workspace-step-status workspace-step-${step.status}">${step.status}</span>
                  </div>
                `).join('')}
              </div>
            </div>
            <div class="card">
              <div class="card-title">Computed Structures</div>
              ${data.computed_structures.length ? data.computed_structures.map(item => `
                <div class="workspace-struct-row"><span>${item.label}</span><span>${GlioTwin.fmtVol(item.volume_ml)}</span></div>
              `).join('') : `<div class="workspace-empty-block">No computed structures yet.</div>`}
            </div>
            ${this.renderPipelineCard(data.pipeline_state)}
          </section>
          <aside class="workspace-side">
            <div class="card">
              <div class="card-title">Jobs</div>
              <div class="workspace-job-list">
                ${(data.jobs || []).length ? data.jobs.map(job => `
                  <button class="workspace-job-row ${job.id === this.state.selectedJobId ? 'selected' : ''}" data-job-id="${job.id}">
                    <span class="workspace-job-main">#${job.id} · ${job.status}</span>
                    <span class="workspace-job-sub">${job.progress_label || job.progress_stage || '—'}</span>
                  </button>
                `).join('') : `<div class="workspace-empty-block">No jobs yet.</div>`}
              </div>
            </div>
            <div class="card">
              <div class="card-title">Log</div>
              <div class="workspace-log-meta">${selectedJob ? `Job #${selectedJob.id} · ${selectedJob.status} · ${selectedJob.progress_label || selectedJob.progress_stage || '—'}` : 'No job selected'}</div>
              <pre class="workspace-log">${this.state.logText || 'No log yet.'}</pre>
            </div>
          </aside>
        </div>
      </div>
    `;

    app.querySelector('#workspace-back')?.addEventListener('click', () => {
      location.hash = '#/workspace';
    });
    app.querySelector('#workspace-open-viewer')?.addEventListener('click', () => {
      GlioTwin.state.currentPatient = data.subject.id;
      GlioTwin.state.currentSession = data.session.id;
      location.hash = '#/browser';
    });
    app.querySelectorAll('.workspace-job-row[data-job-id]').forEach(el => {
      el.addEventListener('click', async () => {
        this.state.selectedJobId = parseInt(el.dataset.jobId, 10);
        const log = await GlioTwin.fetch(`/api/processing/jobs/${this.state.selectedJobId}/log?tail=16000`);
        this.state.logText = log.text || '';
        this.render(app, arg);
      });
    });
  },

  renderOverview(app) {
    const overview = this.state.overview || { sessions: [], running_jobs: [], queued_jobs: [], recent_jobs: [] };
    const filtered = this.filteredSessions();
    const selectedCount = this.selectedSessionIds().length;

    app.innerHTML = `
      <div class="workspace-view">
        <div class="workspace-header">
          <div>
            <div class="import-title">Preprocessing</div>
            <div class="import-subtitle">Gestione batch delle sessioni importate: pipeline state, job FeTS, avanzamento per stato. La coda è seriale: un job alla volta.</div>
          </div>
          <div class="workspace-header-actions">
            <button id="workspace-refresh" class="btn" ${this.state.loading ? 'disabled' : ''}>Refresh</button>
            <button id="workspace-dispatch" class="btn" ${this.state.loading ? 'disabled' : ''}>Start Next</button>
            <button id="workspace-queue-unprocessed" class="btn">Queue All Unprocessed</button>
            <button id="workspace-queue" class="btn btn-primary" ${selectedCount ? '' : 'disabled'}>Queue Selected</button>
          </div>
        </div>

        <div class="import-summary-grid">
          <div class="import-summary-card"><div class="import-summary-label">Running</div><div class="import-summary-value">${overview.running_jobs.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Queued</div><div class="import-summary-value">${overview.queued_jobs.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Candidates</div><div class="import-summary-value">${overview.sessions.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Selected</div><div class="import-summary-value">${selectedCount}</div></div>
        </div>

        <div class="workspace-grid">
          <section class="workspace-main">
            <div class="card workspace-ops-card">
              <div class="card-title">Run Flow</div>
              <div class="workspace-ops-line"><strong>1.</strong><span>Seleziona uno o piu casi nella tabella.</span></div>
              <div class="workspace-ops-line"><strong>2.</strong><span>Clicca <code>Queue Selected</code> per metterli in coda.</span></div>
              <div class="workspace-ops-line"><strong>3.</strong><span>Clicca <code>Start Next</code> una volta per avviare il primo job disponibile.</span></div>
              <div class="workspace-ops-line"><strong>4.</strong><span>Apri il caso con <code>Open</code> per seguire step, errori, output e log.</span></div>
            </div>
            ${this.renderCurrentJobCard()}
            ${this.renderSystemCard()}
            <div class="card">
              <div class="card-title">Cases</div>
              <div class="workspace-table-hint">Uso consigliato: fai partire un solo caso alla volta, aprilo subito e controlla <code>Pipeline State</code> e <code>Log</code>.</div>
              <div class="import-control-grid">
                <label class="import-field import-field-small">
                  <span>Status</span>
                  <select id="workspace-filter-status" class="import-input">
                    <option value="active" ${this.state.filters.status === 'active' ? 'selected' : ''}>Active</option>
                    <option value="all" ${this.state.filters.status === 'all' ? 'selected' : ''}>All</option>
                    <option value="ready" ${this.state.filters.status === 'ready' ? 'selected' : ''}>Ready</option>
                    <option value="queued" ${this.state.filters.status === 'queued' ? 'selected' : ''}>Queued</option>
                    <option value="running" ${this.state.filters.status === 'running' ? 'selected' : ''}>Running</option>
                    <option value="failed" ${this.state.filters.status === 'failed' ? 'selected' : ''}>Failed</option>
                    <option value="completed" ${this.state.filters.status === 'completed' ? 'selected' : ''}>Completed</option>
                  </select>
                </label>
                <label class="import-field">
                  <span>Search</span>
                  <input id="workspace-filter-q" class="import-input" value="${this.state.filters.q}" placeholder="patient or session">
                </label>
              </div>
              <div class="import-table-wrap">
                <table class="import-table">
                  <thead>
                    <tr>
                      <th><input type="checkbox" id="workspace-select-all"></th>
                      <th>Case</th>
                      <th>Status</th>
                      <th>Core</th>
                      <th>Latest Job</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    ${filtered.length ? filtered.map(item => this.renderSessionRow(item)).join('') : `<tr><td colspan="6" class="import-empty-cell">No matching sessions.</td></tr>`}
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          <aside class="workspace-side">
            <div class="card">
              <div class="card-title">Queue</div>
              <div class="workspace-job-list">
                ${[...overview.running_jobs, ...overview.queued_jobs].length
                  ? [...overview.running_jobs, ...overview.queued_jobs].map(job => this.renderJob(job)).join('')
                  : `<div class="workspace-empty-block">No active queue.</div>`}
              </div>
            </div>

            <div class="card">
              <div class="card-title">Recent Jobs</div>
              <div class="workspace-job-list">
                ${(overview.recent_jobs || []).slice(0, 10).map(job => this.renderJob(job)).join('') || `<div class="workspace-empty-block">No recent jobs.</div>`}
              </div>
            </div>
          </aside>
        </div>
      </div>
    `;

    app.querySelector('#workspace-refresh')?.addEventListener('click', async () => {
      await this.refreshOverview();
      this.render(app);
    });
    app.querySelector('#workspace-dispatch')?.addEventListener('click', async () => {
      try {
        await GlioTwin.post('/api/processing/dispatch', {});
        await this.refreshOverview();
        this.render(app);
      } catch (error) {
        GlioTwin.toast(error.message, 'error');
      }
    });
    app.querySelector('#workspace-queue')?.addEventListener('click', async () => {
      try {
        const ids = this.selectedSessionIds();
        if (!ids.length) return;
        await GlioTwin.post('/api/processing/jobs/queue', { session_ids: ids });
        await this.refreshOverview();
        this.render(app);
        GlioTwin.toast(`Queued ${ids.length} session(s)`, 'info');
      } catch (error) {
        GlioTwin.toast(error.message, 'error');
      }
    });
    app.querySelector('#workspace-queue-unprocessed')?.addEventListener('click', async () => {
      try {
        const result = await GlioTwin.post('/api/processing/jobs/queue-unprocessed', {});
        await this.refreshOverview();
        this.render(app);
        GlioTwin.toast(`Queued ${result.queued_jobs?.length || 0} unprocessed session(s)`, 'info');
      } catch (error) {
        GlioTwin.toast(error.message, 'error');
      }
    });
    app.querySelector('#workspace-filter-status')?.addEventListener('change', (e) => {
      this.state.filters.status = e.target.value;
      this.render(app);
    });
    app.querySelector('#workspace-filter-q')?.addEventListener('input', (e) => {
      this.state.filters.q = e.target.value;
      this.render(app);
    });
    app.querySelector('#workspace-select-all')?.addEventListener('change', (e) => {
      const checked = e.target.checked;
      for (const item of filtered) this.state.selectedSessions[item.session_id] = checked;
      this.render(app);
    });
    app.querySelectorAll('.workspace-session-cb').forEach(el => {
      el.addEventListener('change', (e) => {
        this.state.selectedSessions[parseInt(e.target.dataset.sessionId, 10)] = e.target.checked;
      });
    });
    app.querySelectorAll('.workspace-open-case').forEach(el => {
      el.addEventListener('click', () => {
        location.hash = `#/workspace/${el.dataset.sessionId}`;
      });
    });
    app.querySelectorAll('.workspace-stop-all').forEach(el => {
      el.addEventListener('click', async () => {
        try {
          await GlioTwin.post('/api/processing/stop-all', {});
          await this.refreshOverview();
          this.render(app);
          GlioTwin.toast('All running and queued jobs stopped', 'info');
        } catch (error) {
          GlioTwin.toast(error.message, 'error');
        }
      });
    });
    app.querySelectorAll('.workspace-cancel-job').forEach(el => {
      el.addEventListener('click', async () => {
        try {
          await GlioTwin.post(`/api/processing/jobs/${el.dataset.jobId}/cancel`, {});
          await this.refreshOverview();
          this.render(app);
        } catch (error) {
          GlioTwin.toast(error.message, 'error');
        }
      });
    });
    app.querySelectorAll('.workspace-remove-job').forEach(el => {
      el.addEventListener('click', async () => {
        try {
          await fetch(`/api/processing/jobs/${el.dataset.jobId}`, { method: 'DELETE' });
          await this.refreshOverview();
          this.render(app);
        } catch (error) {
          GlioTwin.toast(error.message, 'error');
        }
      });
    });

    this.resetPolling();
    this.state.pollHandle = setInterval(async () => {
      if (!location.hash.startsWith('#/workspace')) {
        this.resetPolling();
        return;
      }
      await this.refreshOverview();
      this.render(document.getElementById('app'));
    }, 7000);
  },

  render(app, arg) {
    if (arg) {
      this.resetPolling();
      this.state.pollHandle = setInterval(async () => {
        if (location.hash !== `#/workspace/${arg}`) {
          this.resetPolling();
          return;
        }
        await this.refreshCase(arg);
        this.render(document.getElementById('app'), arg);
      }, 7000);
      this.renderCase(app, arg);
      return;
    }
    this.renderOverview(app);
  },
};

GlioTwin.register('workspace', async (app, arg) => {
  if (arg) {
    await WorkspaceView.loadCase(arg);
    WorkspaceView.render(app, arg);
    return;
  }
  await WorkspaceView.loadOverview();
  WorkspaceView.render(app);
});
