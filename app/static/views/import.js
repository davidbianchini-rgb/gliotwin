/* ============================================================
   Import view — dataset selection + per-dataset import flow
   ============================================================ */

const IMPORT_DATASETS = [
  {
    key: 'irst_dicom_raw',
    label: 'IRST',
    kind: 'ui',
    description: 'Dati DICOM interni IRST. Flusso UI completo: scan, review, commit, import RT.',
  },
  {
    key: 'mu_glioma_post',
    label: 'MU-Glioma-Post',
    kind: 'pipeline',
    script: 'pipelines/import_mu.py',
    description: 'Dataset pubblico MU Glioma Post-operative. Pipeline server-side.',
  },
  {
    key: 'ucsd_ptgbm',
    label: 'UCSD-PTGBM',
    kind: 'pipeline',
    script: 'pipelines/import_ucsd.py',
    description: 'Dataset UCSD Post-Treatment GBM. Pipeline server-side.',
  },
  {
    key: 'lumiere',
    label: 'LUMIERE',
    kind: 'pipeline',
    script: 'pipelines/import_lumiere.py',
    description: 'Dataset LUMIERE (longitudinal MRI). Pipeline server-side da CSV.',
  },
  {
    key: 'rhuh_gbm',
    label: 'RHUH-GBM',
    kind: 'pending',
    description: 'Dataset RHUH GBM. Pipeline di import non ancora implementata.',
  },
  {
    key: 'qin_gbm',
    label: 'QIN-GBM',
    kind: 'pending',
    description: 'Dataset QIN-GBM-Treatment-Response. Pipeline di import non ancora implementata.',
  },
  {
    key: 'glis_rt',
    label: 'GLIS-RT',
    kind: 'pending',
    description: 'Dataset GLIS-RT. Pipeline di import non ancora implementata.',
  },
];

const ImportView = {
  state: {
    selectedDataset: 'irst_dicom_raw',
    roots: [],
    scan: null,
    rtAnalysis: null,
    selectedExamKey: null,
    selectedRoot: '',
    rtFilePath: '/mnt/dati/RT_GBM_MOSAIQ_IMORT.xlsx',
    rtDataset: 'irst_dicom_raw',
    loading: false,
    rtLoading: false,
    includeExam: {},
    includeSeries: {},
    coreChoice: {},
    lastCommit: null,
    lastRtCommit: null,
  },

  _currentDatasetInfo() {
    return IMPORT_DATASETS.find(d => d.key === this.state.selectedDataset) || IMPORT_DATASETS[0];
  },

  statusBadge(status) {
    const labels = {
      ready: 'Ready',
      review: 'Review',
      incomplete: 'Incomplete',
    };
    return `<span class="badge badge-import-${status}">${labels[status] || status}</span>`;
  },

  classBadge(label) {
    const names = {
      t1n: 'T1 native',
      t1c: 'T1 contrast',
      t2w: 'T2',
      t2f: 'FLAIR',
      apt: 'APT',
      other: 'Other',
    };
    const safeLabel = String(label || '').trim();
    return `<span class="badge badge-import-class badge-import-class-${safeLabel || 'other'}">${names[safeLabel] || safeLabel.toUpperCase() || 'OTHER'}</span>`;
  },

  examByKey() {
    const map = new Map();
    for (const exam of this.state.scan?.exams || []) map.set(exam.exam_key, exam);
    return map;
  },

  selectedExam() {
    if (!this.state.selectedExamKey) return null;
    return this.examByKey().get(this.state.selectedExamKey) || null;
  },

  selectionSummary() {
    const exams = this.state.scan?.exams || [];
    let examsIncluded = 0;
    let seriesIncluded = 0;
    for (const exam of exams) {
      if (this.state.includeExam[exam.exam_key] === false) continue;
      examsIncluded += 1;
      for (const series of exam.series) {
        const key = series.series_instance_uid || series.source_dir;
        if (this.state.includeSeries[key] !== false) {
          seriesIncluded += 1;
        }
      }
    }
    return { examsIncluded, seriesIncluded };
  },

  selectedCoreSeries(exam, label) {
    const manualKey = this.state.coreChoice[`${exam.exam_key}|${label}`];
    const candidates = exam.core_candidates?.[label] || [];
    if (manualKey) {
      return candidates.find(item => (item.series_instance_uid || item.source_dir) === manualKey) || null;
    }
    return exam.core_selection?.[label] || candidates[0] || null;
  },

  initSelection(scan) {
    this.state.includeExam = {};
    this.state.includeSeries = {};
    this.state.coreChoice = {};
    for (const exam of scan.exams || []) {
      this.state.includeExam[exam.exam_key] = exam.status !== 'incomplete';
      for (const series of exam.series) {
        const key = series.series_instance_uid || series.source_dir;
        const isCore = !!series.selected_for_core;
        const isPreferredExtra = series.class_label === 'apt';
        this.state.includeSeries[key] = isCore || isPreferredExtra;
      }
    }
    this.state.selectedExamKey = scan.exams?.[0]?.exam_key || null;
  },

  async loadRoots() {
    const data = await GlioTwin.fetch('/api/import/roots');
    this.state.roots = data.roots || [];
    if (!this.state.selectedRoot && this.state.roots.length) {
      this.state.selectedRoot = this.state.roots[0];
    }
  },

  async runScan() {
    const rootPath = document.getElementById('import-root')?.value?.trim();
    const limitRaw = document.getElementById('import-limit')?.value?.trim();
    if (!rootPath) {
      GlioTwin.toast('Select a root path first', 'error');
      return;
    }
    this.state.loading = true;
    this.render(document.getElementById('app'));
    try {
      const payload = {
        root_path: rootPath,
        limit_studies: limitRaw ? parseInt(limitRaw, 10) : null,
      };
      const scan = await GlioTwin.post('/api/import/scan', payload);
      this.state.selectedRoot = rootPath;
      this.state.scan = scan;
      this.state.lastCommit = null;
      this.initSelection(scan);
      GlioTwin.toast(`Scan completed: ${scan.summary.total_exams} exams`, 'info');
    } catch (error) {
      console.error('[import scan]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.loading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runCommit() {
    const scan = this.state.scan;
    if (!scan) {
      GlioTwin.toast('Run a scan first', 'error');
      return;
    }
    const examKeys = (scan.exams || [])
      .filter(exam => this.state.includeExam[exam.exam_key] !== false)
      .map(exam => exam.exam_key);
    if (!examKeys.length) {
      GlioTwin.toast('No exams selected for import', 'error');
      return;
    }

    this.state.loading = true;
    this.render(document.getElementById('app'));
    try {
      const result = await GlioTwin.post('/api/import/commit', {
        root_path: this.state.selectedRoot,
        exam_keys: examKeys,
        include_series: this.state.includeSeries,
        core_choice: this.state.coreChoice,
      });
      this.state.lastCommit = result;
      GlioTwin.toast(`Imported ${result.imported_exam_keys.length} exams`, 'info');
    } catch (error) {
      console.error('[import commit]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.loading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runRtAnalyze() {
    const filePath = document.getElementById('rt-import-path')?.value?.trim();
    const dataset = document.getElementById('rt-import-dataset')?.value?.trim() || 'irst_dicom_raw';
    if (!filePath) {
      GlioTwin.toast('Select an RT Excel file first', 'error');
      return;
    }
    this.state.rtLoading = true;
    this.render(document.getElementById('app'));
    try {
      const result = await GlioTwin.post('/api/import/rt/analyze', {
        file_path: filePath,
        dataset,
      });
      this.state.rtFilePath = filePath;
      this.state.rtDataset = dataset;
      this.state.rtAnalysis = result;
      this.state.lastRtCommit = null;
      GlioTwin.toast(`RT analysis completed: ${result.summary.matched_rows} matched`, 'info');
    } catch (error) {
      console.error('[rt analyze]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.rtLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runRtCommit() {
    const filePath = document.getElementById('rt-import-path')?.value?.trim();
    const dataset = document.getElementById('rt-import-dataset')?.value?.trim() || 'irst_dicom_raw';
    if (!filePath) {
      GlioTwin.toast('Select an RT Excel file first', 'error');
      return;
    }
    this.state.rtLoading = true;
    this.render(document.getElementById('app'));
    try {
      const result = await GlioTwin.post('/api/import/rt/commit', {
        file_path: filePath,
        dataset,
      });
      this.state.rtFilePath = filePath;
      this.state.rtDataset = dataset;
      this.state.rtAnalysis = result;
      this.state.lastRtCommit = result;
      GlioTwin.toast(`RT imported: ${result.summary.imported_rows} rows`, 'info');
    } catch (error) {
      console.error('[rt commit]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.rtLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  renderSummary(scan) {
    const s = scan.summary;
    const cards = [
      ['Exams', s.total_exams, 'All studies grouped by patient + study UID'],
      ['Ready', s.ready_exams, '4 core sequences selected with no ambiguity'],
      ['Review', s.review_exams, '4 core sequences found, but at least one class has multiple candidates'],
      ['Incomplete', s.incomplete_exams, 'One or more core sequences are missing'],
      ['Extras', s.extra_series, 'Recognized non-core MRI series'],
      ['Other', s.other_series, 'Unclassified or non-useful series'],
    ];
    return `
      <div class="import-summary-grid">
        ${cards.map(([label, value, hint]) => `
          <div class="import-summary-card">
            <div class="import-summary-label">${label}</div>
            <div class="import-summary-value">${value}</div>
            <div class="import-summary-hint">${hint}</div>
          </div>
        `).join('')}
      </div>
    `;
  },

  renderExamTable(scan) {
    const rows = scan.exams.map(exam => {
      const selected = this.state.selectedExamKey === exam.exam_key ? 'selected' : '';
      const enabled = this.state.includeExam[exam.exam_key] !== false;
      const cores = ['t1n', 't1c', 't2w', 't2f'].map(label => {
        const item = exam.core_selection[label];
        return `<span class="import-core-pill ${item ? 'ok' : 'missing'}">${String(label || '').toUpperCase() || 'OTHER'}</span>`;
      }).join('');
      return `
        <tr class="import-row ${selected}" data-exam-key="${exam.exam_key}">
          <td><input type="checkbox" class="exam-include" data-exam-key="${exam.exam_key}" ${enabled ? 'checked' : ''}></td>
          <td>
            <div class="import-cell-title">${GlioTwin.patientPrimary(exam)}</div>
            <div class="import-cell-sub">
              ${GlioTwin.state.showSensitive
                ? `${GlioTwin.patientSecondary(exam) || GlioTwin.fmt(exam.patient_id)} · ${GlioTwin.examDate(exam)}`
                : `${GlioTwin.fmt(exam.patient_id)} · ${GlioTwin.fmt(exam.timepoint_label)}`}
            </div>
          </td>
          <td>
            <div class="import-cell-title">${GlioTwin.fmt(exam.study_description)}</div>
            <div class="import-cell-sub">${exam.series_count} series</div>
          </td>
          <td>${this.statusBadge(exam.status)}</td>
          <td><div class="import-core-strip">${cores}</div></td>
          <td>${exam.extra_series_count}</td>
        </tr>
      `;
    }).join('');

    const allChecked = scan.exams.every(e => this.state.includeExam[e.exam_key] !== false);
    const someChecked = !allChecked && scan.exams.some(e => this.state.includeExam[e.exam_key] !== false);
    return `
      <div class="import-table-wrap">
        <table class="import-table">
          <thead>
            <tr>
              <th><input type="checkbox" id="exam-select-all" ${allChecked ? 'checked' : ''} ${someChecked ? 'data-indeterminate="true"' : ''}> All</th>
              <th>Exam</th>
              <th>Description</th>
              <th>Status</th>
              <th>Core</th>
              <th>Extras</th>
            </tr>
          </thead>
          <tbody>${rows || `<tr><td colspan="6" class="import-empty-cell">No exams found.</td></tr>`}</tbody>
        </table>
      </div>
    `;
  },

  renderExamDetail(exam) {
    if (!exam) {
      return `<div class="import-empty-panel">Select an exam to inspect its candidate series.</div>`;
    }

    const coreCards = ['t1n', 't1c', 't2w', 't2f'].map(label => {
      const item = this.selectedCoreSeries(exam, label);
      const conflict = (exam.core_candidates?.[label] || []).length > 1;
      const alternatives = (exam.core_candidates?.[label] || []).map(candidate => {
        const key = candidate.series_instance_uid || candidate.source_dir;
        const selectedKey = item ? (item.series_instance_uid || item.source_dir) : null;
        const isSelected = key === selectedKey;
        return `
          <button
            class="import-candidate ${isSelected ? 'selected' : ''}"
            data-core-label="${label}"
            data-candidate-key="${key}"
          >
            <span class="import-candidate-rank">#${candidate.candidate_rank}</span>
            <span class="import-candidate-text">
              <strong>${GlioTwin.fmt(candidate.series_description)}</strong>
              <small>${candidate.selection_note}</small>
            </span>
          </button>
        `;
      }).join('');
      return `
        <div class="import-core-card ${item ? 'ok' : 'missing'}">
          <div class="import-core-card-label">${String(label || '').toUpperCase() || 'OTHER'}</div>
          <div class="import-core-card-body">
            ${item ? `
              <div class="import-core-card-title">${GlioTwin.fmt(item.series_description)}</div>
              <div class="import-core-card-sub">${GlioTwin.fmt(item.protocol_name)} · ${item.n_files} files</div>
              ${conflict ? `<div class="import-warning-text">Multiple candidates found for ${label}. Suggested candidates are ordered by adherence, with the most probable already selected.</div>` : ''}
              ${alternatives ? `<div class="import-candidate-list">${alternatives}</div>` : ''}
            ` : `<div class="import-warning-text">Missing ${label}</div>`}
          </div>
        </div>
      `;
    }).join('');

    const seriesRows = exam.series.map(series => {
      const key = series.series_instance_uid || series.source_dir;
      const included = this.state.includeSeries[key] !== false;
      const assignedLabels = ['t1n', 't1c', 't2w', 't2f']
        .filter(label => {
          const selected = this.selectedCoreSeries(exam, label);
          if (!selected) return false;
          const selectedKey = selected.series_instance_uid || selected.source_dir;
          return selectedKey === key;
        })
        .map(label => `<span class="import-assigned-tag">${String(label || '').toUpperCase() || 'OTHER'}</span>`)
        .join('');
      return `
        <tr>
          <td><input type="checkbox" class="series-include" data-series-key="${key}" ${included ? 'checked' : ''}></td>
          <td>${this.classBadge(series.class_label)}</td>
          <td>
            <div class="import-cell-title">${GlioTwin.fmt(series.series_description)}</div>
            <div class="import-cell-sub">${GlioTwin.fmt(series.protocol_name)}</div>
            ${assignedLabels ? `<div class="import-assigned-wrap">${assignedLabels}</div>` : ''}
          </td>
          <td>${GlioTwin.fmt(series.modality)}</td>
          <td>${GlioTwin.fmt(series.n_files)}</td>
        </tr>
      `;
    }).join('');

    return `
      <div class="import-detail-head">
        <div>
          <div class="import-detail-title">${GlioTwin.patientPrimary(exam)}</div>
          <div class="import-detail-sub">
            ${GlioTwin.fmt(exam.study_description)} · ${GlioTwin.examDate(exam)} · ${GlioTwin.fmt(exam.timepoint_label)}
            ${GlioTwin.state.showSensitive && GlioTwin.patientSecondary(exam) ? ` · ${GlioTwin.patientSecondary(exam)}` : ''}
          </div>
        </div>
        ${this.statusBadge(exam.status)}
      </div>
      <div class="import-core-grid">${coreCards}</div>
      <div class="import-detail-block">
        <div class="import-detail-block-title">All series in this exam</div>
        <div class="import-series-wrap">
          <table class="import-table import-series-table">
            <thead>
              <tr>
                <th>Keep</th>
                <th>Class</th>
                <th>Series</th>
                <th>Modality</th>
                <th>Files</th>
              </tr>
            </thead>
            <tbody>${seriesRows}</tbody>
          </table>
        </div>
      </div>
    `;
  },

  renderRtSummary(analysis) {
    const s = analysis.summary || {};
    const cards = [
      ['Rows', s.rows_total || 0, 'Rows parsed from the Excel file'],
      ['Matched', s.matched_rows || 0, 'Single patient hit by normalized surname + name'],
      ['Ambiguous', s.ambiguous_rows || 0, 'More than one DB patient matches the same RT name'],
      ['Unmatched', s.unmatched_rows || 0, 'No patient in the selected dataset matches the RT name'],
    ];
    return `
      <div class="import-summary-grid">
        ${cards.map(([label, value, hint]) => `
          <div class="import-summary-card">
            <div class="import-summary-label">${label}</div>
            <div class="import-summary-value">${value}</div>
            <div class="import-summary-hint">${hint}</div>
          </div>
        `).join('')}
      </div>
    `;
  },

  renderRtRows(analysis) {
    const rows = (analysis.rows || []).slice(0, 40).map(row => `
      <tr>
        <td><span class="badge badge-import-${row.status}">${row.status}</span></td>
        <td>
          <div class="import-cell-title">${GlioTwin.fmt(row.patient_name_raw)}</div>
          <div class="import-cell-sub">row ${row.row_index}${row.ida ? ` · IDA ${row.ida}` : ''}</div>
        </td>
        <td>${GlioTwin.fmt(row.tax_code)}</td>
        <td>${GlioTwin.fmt(row.fractions_count)}</td>
        <td>${GlioTwin.fmt(row.diagnosis_date)}</td>
        <td>${GlioTwin.fmt(row.start_date)}</td>
        <td>
          ${(row.candidates || []).map(candidate => `
            <div class="import-cell-sub">
              ${GlioTwin.fmt(candidate.patient_name)} · ${GlioTwin.fmt(candidate.subject_id)}
              ${GlioTwin.state.showSensitive && candidate.patient_birth_date ? ` · DOB ${GlioTwin.dicomDate(candidate.patient_birth_date)}` : ''}
            </div>
          `).join('') || '<span class="text-muted">—</span>'}
        </td>
      </tr>
    `).join('');

    return `
      <div class="import-detail-block">
        <div class="import-detail-block-title">RT preview</div>
        <div class="import-series-wrap">
          <table class="import-table import-series-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>RT row</th>
                <th>Tax code</th>
                <th>Fractions</th>
                <th>Diagnosis</th>
                <th>RT start</th>
                <th>DB candidates</th>
              </tr>
            </thead>
            <tbody>${rows || `<tr><td colspan="7" class="import-empty-cell">No RT rows found.</td></tr>`}</tbody>
          </table>
        </div>
        ${(analysis.rows || []).length > 40 ? `
          <div class="import-helper-text">Showing first 40 rows only. The import still considers the full file.</div>
        ` : ''}
      </div>
    `;
  },

  bindEvents(app) {
    app.querySelectorAll('.import-ds-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        this.state.selectedDataset = btn.dataset.dsKey;
        this.state.rtDataset = btn.dataset.dsKey;
        this.render(app);
      });
    });

    app.querySelector('#import-run-scan')?.addEventListener('click', () => this.runScan());
    app.querySelector('#import-run-commit')?.addEventListener('click', () => this.runCommit());
    app.querySelector('#rt-import-analyze')?.addEventListener('click', () => this.runRtAnalyze());
    app.querySelector('#rt-import-commit')?.addEventListener('click', () => this.runRtCommit());
    app.querySelectorAll('.import-row').forEach(row => {
      row.addEventListener('click', (event) => {
        if (event.target.closest('input')) return;
        this.state.selectedExamKey = row.dataset.examKey;
        this.render(app);
      });
    });
    const selectAll = app.querySelector('#exam-select-all');
    if (selectAll?.dataset.indeterminate === 'true') selectAll.indeterminate = true;
    selectAll?.addEventListener('change', (event) => {
      const checked = event.target.checked;
      for (const exam of this.state.scan?.exams || []) {
        this.state.includeExam[exam.exam_key] = checked;
      }
      this.render(app);
    });
    app.querySelectorAll('.exam-include').forEach(input => {
      input.addEventListener('change', (event) => {
        this.state.includeExam[event.target.dataset.examKey] = event.target.checked;
        this.render(app);
      });
    });
    app.querySelectorAll('.series-include').forEach(input => {
      input.addEventListener('change', (event) => {
        this.state.includeSeries[event.target.dataset.seriesKey] = event.target.checked;
      });
    });
    app.querySelectorAll('.import-candidate').forEach(button => {
      button.addEventListener('click', (event) => {
        const label = event.currentTarget.dataset.coreLabel;
        const key = event.currentTarget.dataset.candidateKey;
        const exam = this.selectedExam();
        if (!exam) return;
        this.state.coreChoice[`${exam.exam_key}|${label}`] = key;
        this.state.includeSeries[key] = true;
        this.render(app);
      });
    });
  },

  renderDatasetSelector() {
    return `
      <div class="import-dataset-selector card">
        <div class="card-title">Sorgente dataset</div>
        <div class="import-ds-grid">
          ${IMPORT_DATASETS.map(d => `
            <button
              class="import-ds-btn ${d.kind} ${this.state.selectedDataset === d.key ? 'selected' : ''}"
              data-ds-key="${d.key}"
              title="${d.description}"
            >
              <span class="import-ds-label">${d.label}</span>
              <span class="import-ds-kind">${d.kind === 'ui' ? 'UI' : d.kind === 'pipeline' ? 'script' : 'da fare'}</span>
            </button>
          `).join('')}
        </div>
        <div class="import-ds-desc" id="import-ds-desc">${this._currentDatasetInfo().description}</div>
      </div>
    `;
  },

  renderIrstSection(scan, rtAnalysis, selection) {
    return `
      <div class="import-controls card">
        <div class="card-title">Scan Root</div>
        <div class="import-control-grid">
          <label class="import-field">
            <span>Server path</span>
            <input id="import-root" class="import-input" list="import-roots" value="${this.state.selectedRoot || ''}" placeholder="/mnt/dati/irst_data/irst_dicom_raw/DICOM GBM">
            <datalist id="import-roots">
              ${(this.state.roots || []).map(root => `<option value="${root}"></option>`).join('')}
            </datalist>
          </label>
          <label class="import-field import-field-small">
            <span>Study limit</span>
            <input id="import-limit" class="import-input" type="number" min="1" max="2000" placeholder="all">
          </label>
          <button id="import-run-scan" class="btn btn-primary" ${this.state.loading ? 'disabled' : ''}>
            ${this.state.loading ? 'Scanning…' : 'Scan'}
          </button>
        </div>
        <div class="import-action-row">
          <button id="import-run-commit" class="btn" ${(!scan || this.state.loading) ? 'disabled' : ''}>
            ${this.state.loading ? 'Working…' : 'Import Selected'}
          </button>
          <div class="import-action-hint">Importa le serie DICOM selezionate nel catalogo SQLite come dataset <code>irst_dicom_raw</code>. Nessun preprocessing ancora.</div>
        </div>
        <div class="import-helper-text">
          Prima fase: costruisce un manifest ispezionabile dal DICOM grezzo senza toccare il DB clinico. Il processo è auditabile e rieseguibile.
        </div>
      </div>

      <div class="import-controls card">
        <div class="card-title">Import RT</div>
        <div class="import-control-grid">
          <label class="import-field">
            <span>Excel path</span>
            <input id="rt-import-path" class="import-input" value="${this.state.rtFilePath || ''}" placeholder="/mnt/dati/RT_GBM_MOSAIQ_IMORT.xlsx">
          </label>
          <label class="import-field import-field-small">
            <span>Dataset</span>
            <input id="rt-import-dataset" class="import-input" value="${this.state.rtDataset || 'irst_dicom_raw'}" placeholder="irst_dicom_raw" readonly>
          </label>
          <button id="rt-import-analyze" class="btn btn-primary" ${this.state.rtLoading ? 'disabled' : ''}>
            ${this.state.rtLoading ? 'Working…' : 'Analyze RT'}
          </button>
        </div>
        <div class="import-action-row">
          <button id="rt-import-commit" class="btn" ${this.state.rtLoading ? 'disabled' : ''}>
            ${this.state.rtLoading ? 'Working…' : 'Import RT'}
          </button>
          <div class="import-action-hint">Importa solo i match non ambigui nel DB clinico. Righe ambigue e non trovate restano in attesa di revisione manuale.</div>
        </div>
        <div class="import-helper-text">
          Il matching usa <code>COGNOME, NOME</code> normalizzato. I campi importati sono: external refs RT, metadati corso RT, evento diagnosi, evento inizio radioterapia.
        </div>
      </div>

      ${this.state.lastCommit ? `
        <div class="card import-result-card">
          <div class="card-title">Ultimo import DICOM</div>
          <div class="import-result-grid">
            <div><strong>${this.state.lastCommit.subjects}</strong><span>subjects</span></div>
            <div><strong>${this.state.lastCommit.sessions}</strong><span>sessions</span></div>
            <div><strong>${this.state.lastCommit.sequences_inserted}</strong><span>seq insert</span></div>
            <div><strong>${this.state.lastCommit.sequences_updated}</strong><span>seq update</span></div>
            <div><strong>${this.state.lastCommit.sequences_skipped}</strong><span>seq skip</span></div>
          </div>
          <div class="import-result-sub">Dataset: <code>${this.state.lastCommit.dataset}</code></div>
          ${(this.state.lastCommit.imported_sessions || []).length ? `
            <div class="import-workspace-links">
              ${(this.state.lastCommit.imported_sessions || []).slice(0, 8).map(item => `
                <a class="btn btn-linklike" href="#/workspace/${item.session_id}">
                  Preprocessing ${item.patient_id} · ${item.timepoint_label}
                </a>
              `).join('')}
            </div>
          ` : ''}
        </div>
      ` : ''}

      ${this.state.lastRtCommit ? `
        <div class="card import-result-card">
          <div class="card-title">Ultimo import RT</div>
          <div class="import-result-grid">
            <div><strong>${this.state.lastRtCommit.summary.imported_rows}</strong><span>importate</span></div>
            <div><strong>${this.state.lastRtCommit.summary.skipped_ambiguous}</strong><span>ambigue</span></div>
            <div><strong>${this.state.lastRtCommit.summary.skipped_unmatched}</strong><span>non trovate</span></div>
            <div><strong>${this.state.lastRtCommit.summary.rows_total}</strong><span>totale righe</span></div>
          </div>
          <div class="import-result-sub">File: <code>${this.state.lastRtCommit.file_path}</code></div>
        </div>
      ` : ''}

      ${scan ? this.renderSummary(scan) : `
        <div class="import-placeholder card">
          <div class="import-placeholder-title">Nessun scan ancora</div>
          <div class="import-placeholder-copy">Scegli un path DICOM server-side e avvia la discovery. La pagina classificherà le serie, segnalerà gli esami incompleti e preselezionerà le 4 modalità core più gli extra riconosciuti.</div>
        </div>
      `}

      ${scan ? this.renderExamTable(scan) : ''}
      ${rtAnalysis ? this.renderRtSummary(rtAnalysis) : ''}
      ${rtAnalysis ? this.renderRtRows(rtAnalysis) : ''}
    `;
  },

  renderPipelineSection(info) {
    return `
      <div class="import-pipeline-card card">
        <div class="card-title">Pipeline server-side — ${info.label}</div>
        <div class="import-pipeline-desc">${info.description}</div>
        <div class="import-pipeline-cmd">
          <span class="import-pipeline-cmd-label">Comando</span>
          <code class="import-pipeline-cmd-code">cd /home/irst/gliotwin &amp;&amp; python ${info.script}</code>
        </div>
        <div class="import-helper-text">
          Questa pipeline viene eseguita direttamente sul server. Una volta completata, i soggetti e le sessioni compaiono automaticamente nel Viewer e nel Preprocessing.
        </div>
      </div>
    `;
  },

  renderPendingSection(info) {
    return `
      <div class="import-pending-card card">
        <div class="import-pending-icon">○</div>
        <div class="import-pending-title">Pipeline non ancora disponibile — ${info.label}</div>
        <div class="import-pending-desc">${info.description}</div>
      </div>
    `;
  },

  render(app) {
    const scan = this.state.scan;
    const rtAnalysis = this.state.rtAnalysis;
    const selection = this.selectionSummary();
    const dsInfo = this._currentDatasetInfo();

    app.innerHTML = `
      <div class="import-view">
        <div class="import-header">
          <div>
            <div class="import-title">Import</div>
            <div class="import-subtitle">Selezione sorgente e import dati per fase. Il viewer resta invariato.</div>
          </div>
          ${scan && dsInfo.kind === 'ui' ? `<div class="import-preview-chip">${selection.examsIncluded} exam selezionati · ${selection.seriesIncluded} serie</div>` : ''}
        </div>

        <div class="import-workspace">
          <section class="import-left">
            ${this.renderDatasetSelector()}

            ${dsInfo.kind === 'ui'      ? this.renderIrstSection(scan, rtAnalysis, selection) : ''}
            ${dsInfo.kind === 'pipeline' ? this.renderPipelineSection(dsInfo) : ''}
            ${dsInfo.kind === 'pending'  ? this.renderPendingSection(dsInfo) : ''}
          </section>

          <aside class="import-right card">
            ${dsInfo.kind === 'ui' && scan
              ? this.renderExamDetail(this.selectedExam())
              : `<div class="import-empty-panel">${
                  dsInfo.kind === 'ui'
                    ? 'Dopo il primo scan, qui appariranno serie candidate, conflitti, sequenze core mancanti e selezione keep/discard.'
                    : 'Il pannello di dettaglio è disponibile solo per il flusso IRST DICOM.'
                }</div>`
            }
          </aside>
        </div>
      </div>
    `;
    this.bindEvents(app);
  },
};

GlioTwin.register('import', async (app) => {
  if (!ImportView.state.roots.length) {
    try {
      await ImportView.loadRoots();
    } catch (error) {
      console.error('[import roots]', error);
      GlioTwin.toast(error.message, 'error');
    }
  }
  ImportView.render(app);
});
