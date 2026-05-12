const WorkspaceView = {
  state: {
    overview: null,
    caseData: null,
    system: null,
    selectedJobId: null,
    focusedSessionId: null,
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
    if (!this.state.focusedSessionId) {
      this.state.focusedSessionId = overview.current_job?.session_id || overview.running_jobs?.[0]?.session_id || overview.queued_jobs?.[0]?.session_id || overview.sessions?.[0]?.session_id || null;
    }
    if (this.state.focusedSessionId) {
      await this.loadCase(this.state.focusedSessionId);
    }
    this.state.loading = false;
  },

  async refreshOverview() {
    const [overview, system] = await Promise.all([
      GlioTwin.fetch('/api/workspace'),
      GlioTwin.fetch('/api/system/status'),
    ]);
    this.state.overview = overview;
    this.state.system = system;
    const activeSessionId = overview.current_job?.session_id || overview.running_jobs?.[0]?.session_id || overview.queued_jobs?.[0]?.session_id || null;
    if (activeSessionId) {
      this.state.focusedSessionId = activeSessionId;
    }
    if (this.state.focusedSessionId) {
      await this.refreshCase(this.state.focusedSessionId);
    }
  },

  async loadCase(sessionId) {
    this.state.loading = true;
    this.state.focusedSessionId = parseInt(sessionId, 10);
    this.state.caseData = await GlioTwin.fetch(`/api/workspace/${sessionId}`);
    const preferredJob = (this.state.caseData.jobs || []).find(job => job.status === 'running' || job.status === 'queued') || null;
    this.state.selectedJobId = preferredJob?.id || null;
    if (this.state.selectedJobId) {
      const log = await GlioTwin.fetch(`/api/processing/jobs/${this.state.selectedJobId}/log?tail=16000`);
      this.state.logText = log.text || '';
    } else {
      this.state.logText = '';
    }
    this.state.loading = false;
  },

  async refreshCase(sessionId) {
    this.state.focusedSessionId = parseInt(sessionId, 10);
    this.state.caseData = await GlioTwin.fetch(`/api/workspace/${sessionId}`);
    const jobs = this.state.caseData.jobs || [];
    const preferredJob = jobs.find(job => job.status === 'running' || job.status === 'queued') || null;
    if (!jobs.some(job => job.id === this.state.selectedJobId) || preferredJob?.id !== this.state.selectedJobId) {
      this.state.selectedJobId = preferredJob?.id || null;
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

  compactMachineItems() {
    const s = this.state.system || {};
    const mem = s.memory || {};
    const gpu = (s.gpus || [])[0];
    return [
      ['CPU', s.cpu_percent != null ? `${s.cpu_percent}%` : '—'],
      ['RAM', mem.used_percent != null ? `${mem.used_percent}%` : '—'],
      ['Free', mem.available_mb != null ? `${mem.available_mb} MB` : '—'],
      ['GPU', gpu ? `${gpu.utilization_gpu}%` : '—'],
      ['VRAM', gpu ? `${gpu.memory_used_mb}/${gpu.memory_total_mb} MB` : '—'],
    ];
  },

  elapsedLabel(job) {
    if (!job?.started_at) return '—';
    const start = new Date(job.started_at);
    const end = job.finished_at ? new Date(job.finished_at) : new Date();
    const seconds = Math.max(0, Math.round((end - start) / 1000));
    return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  },

  _parseTime(value) {
    if (!value) return null;
    const dt = new Date(value);
    return Number.isNaN(dt.getTime()) ? null : dt;
  },

  _fmtClock(value) {
    const dt = this._parseTime(value);
    if (!dt) return '—';
    return dt.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  },

  _fmtDuration(startedAt, finishedAt) {
    const start = this._parseTime(startedAt);
    if (!start) return '—';
    const end = this._parseTime(finishedAt) || new Date();
    const seconds = Math.max(0, Math.round((end - start) / 1000));
    return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  },

  currentStepLabel(job) {
    const stage = job?.progress_stage || '';
    const map = {
      queued: 'In coda',
      preparing_input: 'Preparazione input',
      running_fets: 'Avvio preprocessing',
      initial_validation: 'Validazione input',
      nifti_conversion: 'Conversione NIfTI',
      brain_extraction: 'Brain extraction',
      brain_queue: 'Brain extraction',
      brain_inference: 'Brain extraction',
      completed: 'Preprocessing completato',
      failed: 'Fallito',
      cancelled: 'Fermato',
    };
    return map[stage] || job?.progress_label || job?.status || 'In attesa';
  },

  clearPreprocessingStateLocally(sessionId) {
    const clearSteps = (steps = []) => steps.map(step => (
      ['initial_validation', 'nifti_conversion', 'brain_extraction', 'tumor_segmentation'].includes(step.key)
        ? { ...step, status: 'pending', output_path: null, error_message: null, started_at: null, finished_at: null }
        : step
    ));

    if (this.state.caseData?.session?.id === sessionId) {
      this.state.caseData = {
        ...this.state.caseData,
        pipeline_state: {
          ...this.state.caseData.pipeline_state,
          steps: clearSteps(this.state.caseData.pipeline_state?.steps || []),
        },
      };
    }
  },

  phaseIndicators(data, activeJob = null) {
    const steps = data.pipeline_state?.steps || [];
    const byKey = Object.fromEntries(steps.map(step => [step.key, step]));
    const groupInfo = (keys, fallbackLabel, fallbackDetail, opts = {}) => {
      const group = keys.map(key => byKey[key]).filter(Boolean);
      const statuses = group.map(step => step.status);
      let status = 'waiting';
      if (statuses.some(s => s === 'failed')) status = 'blocked';
      else if (statuses.some(s => s === 'running')) status = 'running';
      else if (group.length && group.every(step => step.status === 'done')) status = 'ok';
      else if (statuses.some(s => s === 'done')) status = 'partial';
      else if (statuses.some(s => s === 'pending')) status = 'waiting';
      let started = group.map(step => step.started_at).filter(Boolean).sort()[0] || null;
      const finishedValues = group.map(step => step.finished_at).filter(Boolean).sort();
      const finished = status === 'ok' ? (finishedValues[finishedValues.length - 1] || null) : null;
      let visibleStarted = started;
      let visibleFinished = finished;
      if (!activeJob && status !== 'ok') {
        visibleStarted = null;
        visibleFinished = null;
      }
      return {
        status,
        started_at: visibleStarted,
        finished_at: visibleFinished,
        duration: this._fmtDuration(visibleStarted, visibleFinished) || '—',
        detail: fallbackDetail,
        label: fallbackLabel,
        showTimes: opts.showTimes !== false,
      };
    };

    const seqs = data.sequences || [];
    const coreWithRaw = seqs.filter(s => ['T1', 'T1ce', 'T2', 'FLAIR'].includes(s.sequence_type) && s.raw_path);
    const hasReference = coreWithRaw.some(s => ['T1ce', 'T1'].includes(s.sequence_type));
    const fetsCapable = ['T1', 'T1ce', 'T2', 'FLAIR'].every(t => coreWithRaw.some(s => s.sequence_type === t));
    const coreWithProc = seqs.filter(s => ['T1', 'T1ce', 'T2', 'FLAIR'].includes(s.sequence_type) && s.processed_path);
    const outputReady = coreWithRaw.length > 0 && coreWithRaw.every(s => s.processed_path);

    const prepMode = fetsCapable ? 'FeTS' : (hasReference ? 'SimpleITK (T1ce ref)' : null);

    const items = [
      {
        label: 'Serie core',
        detail: coreWithRaw.length
          ? `${coreWithRaw.length} core (${coreWithRaw.map(s => s.sequence_type).join(', ')}) · modalità: ${prepMode || '—'}`
          : 'Nessuna serie core con raw_path',
        status: hasReference ? (fetsCapable ? 'ok' : 'partial') : 'blocked',
        started_at: null,
        finished_at: null,
        duration: '—',
        showTimes: false,
      },
      groupInfo(['initial_validation'], 'Validazione input', 'Controllo tecnico dell input FeTS'),
      groupInfo(['nifti_conversion'], 'NIfTI core', 'Disponibilita dei volumi NIfTI delle serie core'),
      groupInfo(['brain_extraction'], 'Brain mask', 'Estrazione del parenchima cerebrale'),
      {
        label: 'Output pronti',
        detail: outputReady ? 'Volumi preprocessati disponibili' : 'Strutture non ancora disponibili',
        status: outputReady ? 'ok' : 'waiting',
        started_at: null,
        finished_at: null,
        duration: '—',
        showTimes: false,
      },
    ];

    let previousFinishedAt = null;
    return items.map(item => {
      if (item.showTimes === false) return item;
      let startedAt = item.started_at;
      let finishedAt = item.finished_at;
      if (previousFinishedAt && (startedAt || finishedAt)) {
        startedAt = previousFinishedAt;
      }
      if (finishedAt && startedAt) {
        const startMs = this._parseTime(startedAt)?.getTime();
        const endMs = this._parseTime(finishedAt)?.getTime();
        if (startMs != null && endMs != null && endMs < startMs) {
          finishedAt = startedAt;
        }
      }
      if (finishedAt) {
        previousFinishedAt = finishedAt;
      }
      return {
        ...item,
        started_at: startedAt,
        finished_at: finishedAt,
        duration: this._fmtDuration(startedAt, finishedAt),
      };
    });
  },

  renderPreprocSummary(item) {
    const summary = item.pipeline_state?.steps || [];
    const stepDone  = key => summary.find(s => s.key === key)?.status === 'done';
    const stepRun   = key => summary.find(s => s.key === key)?.status === 'running';
    const stepFail  = key => summary.find(s => s.key === key)?.status === 'failed';
    if (stepFail('initial_validation') || stepFail('nifti_conversion') || stepFail('brain_extraction')) {
      return `<span class="workspace-step-inline failed">Preprocessing fallito</span>`;
    }
    if (stepDone('brain_extraction')) return `<span class="workspace-step-inline done">Preprocessing completato</span>`;
    if (stepRun('initial_validation') || stepRun('nifti_conversion') || stepRun('brain_extraction')) {
      return `<span class="workspace-step-inline running">Preprocessing in corso</span>`;
    }
    if (item.sequences?.some(seq => seq.raw_path)) return `<span class="workspace-step-inline pending">Pronto a partire</span>`;
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
    const hasRef       = coreWithRaw.some(s => ['T1ce', 'T1'].includes(s.sequence_type));
    const fetsOk       = CORE.every(t => coreWithRaw.some(s => s.sequence_type === t));

    const nativeStructs = computed.filter(s => s.reference_space === 'native');
    // IMPORT
    const importOk = hasRef;
    const importStatus = importOk ? 'ok' : 'blocked';
    const importDetail = importOk
      ? `${coreWithRaw.length} serie core (${coreWithRaw.map(s => s.sequence_type).join(', ')}) · ${fetsOk ? 'FeTS' : 'SimpleITK ref=T1ce'}`
      : `Nessuna sequenza di riferimento (T1ce/T1) con raw_path`;

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

    return [
      { phase: 'IMPORT',        label: 'Serie core riconosciute',  status: importStatus,   detail: importDetail   },
      { phase: 'PREPROCESSING', label: 'Preprocessing',            status: preprocStatus,  detail: preprocDetail  },
      { phase: 'PREPROCESSING', label: 'Output nativi disponibili', status: nativeStructs.length > 0 ? 'ok' : 'waiting', detail: nativeStructs.length > 0 ? `${nativeStructs.length} strutture in spazio nativo` : 'Strutture non ancora disponibili' },
    ];
  },

  renderChecklistCard(data) {
    const items = this.phaseIndicators(data);
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

  renderProcessingSummaryCard(data, activeJob, selectedJob) {
    const items = this._buildChecklist(data);
    const importItem = items.find(item => item.label === 'Serie core riconosciute');
    const prepItem = items.find(item => item.label === 'Preprocessing');
    const displayJob = activeJob && activeJob.session_id === data.session.id ? activeJob : selectedJob;
    return `
      <div class="workspace-processing-summary">
        <div class="workspace-processing-row">
          <span class="workspace-processing-label">Import</span>
          <span class="workspace-processing-value">${importItem?.detail || '—'}</span>
        </div>
        <div class="workspace-processing-row">
          <span class="workspace-processing-label">Preprocessing</span>
          <span class="workspace-processing-value">${prepItem?.detail || '—'}</span>
        </div>
        <div class="workspace-processing-row">
          <span class="workspace-processing-label">Step corrente</span>
          <span class="workspace-processing-value">${displayJob?.progress_label || displayJob?.progress_stage || displayJob?.status || 'ready'}</span>
        </div>
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
    return `
      <div class="card workspace-system-card">
        <div class="card-title">Machine Status</div>
        <div class="workspace-metrics">
          ${this.compactMachineItems().map(([label, value]) => `
            <div class="workspace-metric"><span>${label}</span><strong>${value}</strong></div>
          `).join('')}
        </div>
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
          <div>${this.renderPreprocSummary(item)}</div>
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

  renderProcessingCaseCard() {
    const data = this.state.caseData;
    if (!data) {
      return `
        <div class="card">
          <div class="card-title">Processing Case</div>
          <div class="workspace-empty-block">No case selected.</div>
        </div>
      `;
    }
    const selectedJob = (data.jobs || []).find(job => job.id === this.state.selectedJobId) || data.jobs?.[0] || null;
    const activeJob = (data.jobs || []).find(job => job.status === 'running' || job.status === 'queued') || null;
    const items = this.phaseIndicators(data, activeJob);
    return `
      <div class="card">
        <div class="card-title">Processing Case</div>
        <div class="workspace-current-job">
          <div class="workspace-current-main">${GlioTwin.patientPrimary(data.subject)} · ${data.session.session_label}</div>
          <div class="workspace-current-sub">Ora: ${this.currentStepLabel(activeJob)}</div>
        </div>
        <div class="workspace-log-meta">Attivita monitorate del preprocessing: possono sovrapporsi, non sono fasi seriali.</div>
        <div class="workspace-phase-strip">
          ${items.map(item => `
            <div class="workspace-phase-dot phase-${item.status}" title="${item.label}: ${item.detail}">
              <span class="workspace-phase-bullet"></span>
              <span class="workspace-phase-text">
                <strong>${item.label}</strong>
                <small>
                  ${item.detail}
                  ${item.showTimes !== false ? ` · Inizio: ${this._fmtClock(item.started_at)} · Fine: ${this._fmtClock(item.finished_at)} · Durata: ${item.duration || '—'}` : ''}
                </small>
              </span>
            </div>
          `).join('')}
        </div>
        <div class="workspace-side-stack">
          <div class="workspace-log-wrap">
            <div class="workspace-log-meta">${selectedJob ? `Job #${selectedJob.id} · ${selectedJob.status} · ${selectedJob.progress_label || selectedJob.progress_stage || '—'}` : 'No job selected'}</div>
            <pre class="workspace-log">${this.state.logText || 'No log yet.'}</pre>
          </div>
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
            <button class="btn workspace-stop-all">Stop All</button>
            <button class="btn" id="workspace-open-viewer" ${!data.computed_structures.length ? 'disabled' : ''}>Open Viewer</button>
          </div>
        </div>
        <div class="workspace-grid">
          <section class="workspace-main">
            ${this.renderProcessingSummaryCard(data, selectedJob, selectedJob)}
            <div class="card">
              <div class="card-title">Computed Structures</div>
              ${data.computed_structures.length ? data.computed_structures.map(item => `
                <div class="workspace-struct-row"><span>${item.label}</span><span>${GlioTwin.fmtVol(item.volume_ml)}</span></div>
              `).join('') : `<div class="workspace-empty-block">No computed structures yet.</div>`}
            </div>
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
            <div class="import-subtitle">Seleziona un caso, mettilo in coda e segui a destra il preprocessing del caso corrente.</div>
          </div>
          <div class="workspace-header-actions">
            <button id="workspace-refresh" class="btn" ${this.state.loading ? 'disabled' : ''}>Refresh</button>
            <button class="btn workspace-stop-all">Stop All</button>
            <button id="workspace-queue" class="btn btn-primary" ${selectedCount ? '' : 'disabled'}>Queue Selected</button>
          </div>
        </div>

        <div class="import-summary-grid">
          <div class="import-summary-card"><div class="import-summary-label">Running</div><div class="import-summary-value">${overview.running_jobs.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Queued</div><div class="import-summary-value">${overview.queued_jobs.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Cases</div><div class="import-summary-value">${overview.sessions.length}</div></div>
          <div class="import-summary-card"><div class="import-summary-label">Selected</div><div class="import-summary-value">${selectedCount}</div></div>
        </div>

        <div class="workspace-grid">
          <section class="workspace-main">
            ${this.renderSystemCard()}
            <div class="card">
              <div class="card-title">Cases</div>
              <div class="workspace-table-hint">Seleziona i casi da preprocessare. Il click sulla riga aggiorna il pannello destro con il caso corrente.</div>
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
            ${this.renderProcessingCaseCard()}
          </aside>
        </div>
      </div>
    `;

    app.querySelector('#workspace-refresh')?.addEventListener('click', async () => {
      await this.refreshOverview();
      this.render(app);
    });
    app.querySelector('#workspace-queue')?.addEventListener('click', async () => {
      try {
        const ids = this.selectedSessionIds();
        if (!ids.length) return;
        this.clearPreprocessingStateLocally(ids[0]);
        this.state.selectedJobId = null;
        this.state.logText = '';
        this.render(app);
        await GlioTwin.post('/api/processing/jobs/queue', { session_ids: ids });
        await GlioTwin.post('/api/processing/dispatch', {});
        this.state.focusedSessionId = ids[0];
        await this.refreshCase(ids[0]);
        await this.refreshOverview();
        this.render(app);
        GlioTwin.toast(`Queued and started ${ids.length} session(s)`, 'info');
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
        this.render(app);
      });
    });
    app.querySelectorAll('.import-table tbody tr').forEach(row => {
      row.addEventListener('click', async (event) => {
        if (event.target.closest('input,button,a')) return;
        const openBtn = row.querySelector('.workspace-open-case');
        if (!openBtn) return;
        const sessionId = parseInt(openBtn.dataset.sessionId, 10);
        await this.loadCase(sessionId);
        this.render(app);
      });
    });
    app.querySelectorAll('.workspace-open-case').forEach(el => {
      el.addEventListener('click', async () => {
        await this.loadCase(el.dataset.sessionId);
        this.render(app);
      });
    });
    app.querySelectorAll('.workspace-job-row[data-job-id]').forEach(el => {
      el.addEventListener('click', async () => {
        this.state.selectedJobId = parseInt(el.dataset.jobId, 10);
        const log = await GlioTwin.fetch(`/api/processing/jobs/${this.state.selectedJobId}/log?tail=16000`);
        this.state.logText = log.text || '';
        this.render(app);
      });
    });
    app.querySelectorAll('.workspace-stop-all').forEach(el => {
      el.addEventListener('click', async () => {
        try {
          await GlioTwin.post('/api/processing/stop-all', {});
          this.state.selectedJobId = null;
          this.state.logText = '';
          await this.refreshOverview();
          if (this.state.focusedSessionId) {
            await this.refreshCase(this.state.focusedSessionId);
          }
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
