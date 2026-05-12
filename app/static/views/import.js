/* ============================================================
   Import view — dataset selection + per-dataset import flow
   ============================================================ */

const IMPORT_DATASETS = [
  {
    key: 'irst_dicom_raw',
    label: 'DICOM',
    kind: 'ui',
    description: 'Dati DICOM interni. Flusso UI completo: scan, review, commit, import RT.',
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

const IMPORT_STRUCTURE_OPTIONS = [
  { key: 't1n', label: 'T1 native' },
  { key: 't1c', label: 'T1 contrast' },
  { key: 't2w', label: 'T2' },
  { key: 't2f', label: 'FLAIR' },
  { key: 'apt', label: 'APT' },
  { key: 'adc', label: 'ADC' },
  { key: 'swi', label: 'SWI' },
  { key: 'dwi', label: 'DWI' },
  { key: 'other', label: 'Other' },
];

const ImportView = {
  state: {
    selectedDataset: 'irst_dicom_raw',
    roots: [],
    scan: null,
    rtAnalysis: null,
    selectedExamKey: null,
    selectedSubjectKey: null,
    selectedRoot: '',
    rtFilePath: '/mnt/dati/RT_GBM_MOSAIQ_IMORT.xlsx',
    rtDataset: 'irst_dicom_raw',
    loading: false,
    rtLoading: false,
    includeExam: {},
    includeSeries: {},
    coreChoice: {},
    selectedStructures: ['t1n', 't1c', 't2w', 't2f', 'apt'],
    lastCommit: null,
    lastRtCommit: null,
    pipelineRootPath: '/mnt/dati/MU-Glioma-Post',
    pipelineStatus: null,
    pipelineLoading: false,
    lumiereRootPath: '/mnt/dati/lumiere',
    lumiereStatus: null,
    lumiereLoading: false,
  },

  _currentDatasetInfo() {
    return IMPORT_DATASETS.find(d => d.key === this.state.selectedDataset) || IMPORT_DATASETS[0];
  },

  _normText(value) {
    return String(value || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase()
      .replace(/[^A-Z0-9 ]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  },

  _scanPatientGroups(scan) {
    const groups = new Map();
    for (const exam of scan?.exams || []) {
      const family = this._normText(exam.patient_family_name);
      const given = this._normText(exam.patient_given_name);
      const name = this._normText(exam.patient_name);
      const birth = this._normText(exam.patient_birth_date);
      const personKey = (family || given)
        ? `${family}|${given}|${birth}`
        : `${name}|${birth}`;
      const key = personKey || `PID|${this._normText(exam.patient_id)}`;
      if (!groups.has(key)) {
        groups.set(key, {
          key,
          patientIdAliases: new Set(),
          patientName: exam.patient_name || [exam.patient_given_name, exam.patient_family_name].filter(Boolean).join(' ') || exam.patient_id,
          patientBirthDate: exam.patient_birth_date || '',
          exams: [],
        });
      }
      const group = groups.get(key);
      group.patientIdAliases.add(exam.patient_id);
      group.exams.push(exam);
    }
    return [...groups.values()].sort((a, b) => String(a.patientName).localeCompare(String(b.patientName)));
  },

  _matchRtToScan(scan, rtAnalysis) {
    const scanGroups = this._scanPatientGroups(scan);
    const nameIndex = new Map();
    for (const group of scanGroups) {
      const exam = group.exams[0] || {};
      const family = this._normText(exam.patient_family_name);
      const given = this._normText(exam.patient_given_name);
      const name = this._normText(group.patientName);
      const keys = [
        `${family} ${given}`.trim(),
        `${given} ${family}`.trim(),
        name,
      ].filter(Boolean);
      keys.forEach((key) => {
        if (!nameIndex.has(key)) nameIndex.set(key, []);
        nameIndex.get(key).push(group);
      });
    }

    const rows = (rtAnalysis?.rows || []).map((row) => {
      const family = this._normText(row.patient_family_name);
      const given = this._normText(row.patient_given_name);
      const raw = this._normText(row.patient_name_raw);
      const keys = [`${family} ${given}`.trim(), `${given} ${family}`.trim(), raw].filter(Boolean);
      const matches = [];
      const seen = new Set();
      keys.forEach((key) => {
        (nameIndex.get(key) || []).forEach((group) => {
          if (seen.has(group.key)) return;
          seen.add(group.key);
          matches.push(group);
        });
      });
      return {
        ...row,
        scan_matches: matches,
        scan_match_status: matches.length === 1 ? 'matched' : matches.length > 1 ? 'ambiguous' : 'unmatched',
      };
    });

    return {
      scanGroups,
      rows,
      summary: {
        scan_patients: scanGroups.length,
        matched_rows: rows.filter((row) => row.scan_match_status === 'matched').length,
        ambiguous_rows: rows.filter((row) => row.scan_match_status === 'ambiguous').length,
        unmatched_rows: rows.filter((row) => row.scan_match_status === 'unmatched').length,
      },
    };
  },

  _buildUnifiedMatchRows(scan, rtAnalysis) {
    const data = this._matchRtToScan(scan, rtAnalysis);
    const byGroup = new Map();

    data.scanGroups.forEach((group) => {
      byGroup.set(group.key, {
        group,
        rtRows: [],
      });
    });

    data.rows.forEach((row) => {
      if (row.scan_matches?.length) {
        row.scan_matches.forEach((group) => {
          if (!byGroup.has(group.key)) {
            byGroup.set(group.key, { group, rtRows: [] });
          }
          byGroup.get(group.key).rtRows.push(row);
        });
      }
    });

    const rows = [...byGroup.values()].map(({ group, rtRows }) => {
      const uniqueRtNames = [...new Set(rtRows.map((row) => row.patient_name_raw).filter(Boolean))];
      let matchStatus = 'unmatched';
      if (rtRows.some((row) => row.scan_match_status === 'ambiguous')) {
        matchStatus = 'ambiguous';
      } else if (rtRows.some((row) => row.scan_match_status === 'matched')) {
        matchStatus = 'matched';
      }
      if (!rtRows.length) matchStatus = 'missing_rt';

      return {
        key: group.key,
        group,
        patientName: group.patientName,
        patientBirthDate: group.patientBirthDate,
        patientIdAliases: [...group.patientIdAliases],
        examCount: group.exams.length,
        readyCount: group.exams.filter((exam) => exam.status === 'ready').length,
        reviewCount: group.exams.filter((exam) => exam.status === 'review').length,
        incompleteCount: group.exams.filter((exam) => exam.status === 'incomplete').length,
        rtCount: rtRows.length,
        rtNames: uniqueRtNames,
        matchStatus,
      };
    });

    return {
      rows: rows.sort((a, b) => String(a.patientName).localeCompare(String(b.patientName))),
      summary: {
        total: rows.length,
        matched: rows.filter((row) => row.matchStatus === 'matched').length,
        ambiguous: rows.filter((row) => row.matchStatus === 'ambiguous').length,
        missing_rt: rows.filter((row) => row.matchStatus === 'missing_rt').length,
      },
    };
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

  selectedSubject() {
    if (!this.state.selectedSubjectKey) return null;
    return this._scanPatientGroups(this.state.scan).find(group => group.key === this.state.selectedSubjectKey) || null;
  },

  _filteredExams(scan) {
    if (!this.state.selectedSubjectKey) return scan?.exams || [];
    const group = this._scanPatientGroups(scan).find(item => item.key === this.state.selectedSubjectKey);
    return group?.exams || [];
  },

  _subjectSelectionState(group) {
    const exams = group.exams || [];
    const selected = exams.filter(exam => this.state.includeExam[exam.exam_key] !== false).length;
    return {
      total: exams.length,
      selected,
      all: !!exams.length && selected === exams.length,
      some: selected > 0 && selected < exams.length,
    };
  },

  selectExam(examKey, app) {
    if (!examKey) return;
    this.state.selectedExamKey = examKey;
    this.render(app || document.getElementById('app'));
  },

  selectSubject(subjectKey, app) {
    this.state.selectedSubjectKey = subjectKey || null;
    const exams = this._filteredExams(this.state.scan);
    if (!exams.some(exam => exam.exam_key === this.state.selectedExamKey)) {
      this.state.selectedExamKey = exams[0]?.exam_key || null;
    }
    this.render(app || document.getElementById('app'));
  },

  toggleSubjectSelection(subjectKey, checked, app) {
    const group = this._scanPatientGroups(this.state.scan).find(item => item.key === subjectKey);
    if (!group) return;
    for (const exam of group.exams) {
      this.state.includeExam[exam.exam_key] = checked;
    }
    if (checked) {
      this.state.selectedSubjectKey = subjectKey;
      if (!group.exams.some(exam => exam.exam_key === this.state.selectedExamKey)) {
        this.state.selectedExamKey = group.exams[0]?.exam_key || null;
      }
    } else if (this.state.selectedSubjectKey === subjectKey && !this._subjectSelectionState(group).selected) {
      const fallback = this._scanPatientGroups(this.state.scan).find(item => this._subjectSelectionState(item).selected);
      this.state.selectedSubjectKey = fallback?.key || subjectKey;
      const exams = this._filteredExams(this.state.scan);
      this.state.selectedExamKey = exams[0]?.exam_key || null;
    }
    this.render(app || document.getElementById('app'));
  },

  toggleAllSubjectsSelection(checked, app) {
    for (const group of this._scanPatientGroups(this.state.scan)) {
      for (const exam of group.exams) {
        this.state.includeExam[exam.exam_key] = checked;
      }
    }
    if (checked) {
      const firstGroup = this._scanPatientGroups(this.state.scan)[0];
      this.state.selectedSubjectKey = firstGroup?.key || null;
      this.state.selectedExamKey = firstGroup?.exams?.[0]?.exam_key || null;
    }
    this.render(app || document.getElementById('app'));
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
    const groups = this._scanPatientGroups(scan);
    this.state.selectedSubjectKey = groups[0]?.key || null;
    this.state.selectedExamKey = (groups[0]?.exams || scan.exams || [])[0]?.exam_key || null;
  },

  async loadRoots() {
    const data = await GlioTwin.fetch('/api/import/roots');
    this.state.roots = data.roots || [];
    if (!this.state.selectedRoot && this.state.roots.length) {
      this.state.selectedRoot = this.state.roots[0];
    }
  },

  async loadLumiereStatus() {
    const rootPath = this.state.lumiereRootPath || '/mnt/dati/lumiere';
    this.state.lumiereLoading = true;
    this.render(document.getElementById('app'));
    try {
      const query = new URLSearchParams({ root_path: rootPath }).toString();
      this.state.lumiereStatus = await GlioTwin.fetch(`/api/import/lumiere/status?${query}`);
    } catch (error) {
      console.error('[lumiere status]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.lumiereLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runLumiereImport() {
    const rootPath = document.getElementById('lumiere-root-path')?.value?.trim() || '/mnt/dati/lumiere';
    this.state.lumiereRootPath = rootPath;
    this.state.lumiereLoading = true;
    this.render(document.getElementById('app'));
    try {
      const result = await GlioTwin.post('/api/import/lumiere/run', {
        root_path: rootPath,
        purge_selected: false,
        subjects: [],
      });
      if (result.status === 'already_running') {
        GlioTwin.toast('Import LUMIERE già in esecuzione', 'info');
      } else {
        GlioTwin.toast('Import LUMIERE avviato', 'info');
      }
      await this.loadLumiereStatus();
    } catch (error) {
      console.error('[lumiere run]', error);
      GlioTwin.toast(error.message, 'error');
      this.state.lumiereLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  async loadPipelineStatus() {
    const info = this._currentDatasetInfo();
    if (info.key !== 'mu_glioma_post') return;
    const rootPath = this.state.pipelineRootPath || '/mnt/dati/MU-Glioma-Post';
    this.state.pipelineLoading = true;
    this.render(document.getElementById('app'));
    try {
      const query = new URLSearchParams({ root_path: rootPath }).toString();
      this.state.pipelineStatus = await GlioTwin.fetch(`/api/import/mu/status?${query}`);
    } catch (error) {
      console.error('[mu status]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.pipelineLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runMuImport() {
    const rootPath = document.getElementById('mu-root-path')?.value?.trim() || '/mnt/dati/MU-Glioma-Post';
    this.state.pipelineRootPath = rootPath;
    this.state.pipelineLoading = true;
    this.render(document.getElementById('app'));
    try {
      const result = await GlioTwin.post('/api/import/mu/run', {
        root_path: rootPath,
        purge_selected: false,
        subjects: [],
      });
      if (result.status === 'already_running') {
        GlioTwin.toast('Import MU gia in esecuzione', 'info');
      } else {
        GlioTwin.toast('Import MU avviato', 'info');
      }
      await this.loadPipelineStatus();
    } catch (error) {
      console.error('[mu run]', error);
      GlioTwin.toast(error.message, 'error');
      this.state.pipelineLoading = false;
      this.render(document.getElementById('app'));
    }
  },

  async runScan() {
    const rootPath = document.getElementById('import-root')?.value?.trim();
    if (!rootPath) {
      GlioTwin.toast('Select a root path first', 'error');
      return;
    }
    this.state.loading = true;
    this.render(document.getElementById('app'));
    try {
      const payload = { root_path: rootPath };
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

  async runDiscover() {
    const rootPath = document.getElementById('import-root')?.value?.trim();
    const filePath = document.getElementById('rt-import-path')?.value?.trim();
    if (!rootPath && !filePath) {
      GlioTwin.toast('Select at least one source path', 'error');
      return;
    }
    this.state.loading = true;
    this.state.rtLoading = true;
    this.render(document.getElementById('app'));
    try {
      if (rootPath) {
        const scan = await GlioTwin.post('/api/import/scan', {
          root_path: rootPath,
          requested_structures: this.state.selectedStructures,
        });
        this.state.selectedRoot = rootPath;
        this.state.scan = scan;
        this.state.lastCommit = null;
        this.initSelection(scan);
      }
      if (filePath) {
        const result = await GlioTwin.post('/api/import/rt/analyze', {
          file_path: filePath,
          dataset: this.state.rtDataset || 'irst_dicom_raw',
        });
        this.state.rtFilePath = filePath;
        this.state.rtAnalysis = result;
        this.state.lastRtCommit = null;
      }
      GlioTwin.toast('Sources loaded', 'info');
    } catch (error) {
      console.error('[import discover]', error);
      GlioTwin.toast(error.message, 'error');
    } finally {
      this.state.loading = false;
      this.state.rtLoading = false;
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
    const dataset = this.state.rtDataset || 'irst_dicom_raw';
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
    const dataset = this.state.rtDataset || 'irst_dicom_raw';
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
      ['Exams', s.total_exams, 'studi trovati'],
      ['Ready', s.ready_exams, '4 core univoche'],
      ['Review', s.review_exams, '4 core trovate ma con scelte multiple'],
      ['Incomplete', s.incomplete_exams, 'manca almeno una core'],
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
    const exams = this._filteredExams(scan);
    const rows = exams.map(exam => {
      const selected = this.state.selectedExamKey === exam.exam_key ? 'selected' : '';
      const enabled = this.state.includeExam[exam.exam_key] !== false;
      const isReview = exam.status === 'review';
      const ambiguousCount = ['t1n', 't1c', 't2w', 't2f']
        .filter(label => (exam.core_candidates?.[label] || []).length > 1)
        .length;
      const cores = ['t1n', 't1c', 't2w', 't2f'].map(label => {
        const item = exam.core_selection[label];
        return `<span class="import-core-pill ${item ? 'ok' : 'missing'}">${String(label || '').toUpperCase() || 'OTHER'}</span>`;
      }).join('');
      return `
        <tr class="import-row ${selected}" data-exam-key="${exam.exam_key}" data-role="exam-row">
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
          <td>
            <div class="import-core-strip">${cores}</div>
            ${isReview ? `<div class="import-cell-sub">Ambiguous core classes: ${ambiguousCount}</div>` : ''}
          </td>
          <td>${exam.extra_series_count}</td>
        </tr>
      `;
    }).join('');

    const allChecked = exams.length ? exams.every(e => this.state.includeExam[e.exam_key] !== false) : false;
    const someChecked = !allChecked && exams.some(e => this.state.includeExam[e.exam_key] !== false);
    return `
      <div class="import-table-section card">
        <div class="import-table-head">
          <div>
            <div class="card-title">Exams</div>
            <div class="import-table-note">
              <strong>Review</strong> significa che l'esame e completo: le 4 sequenze core sono state trovate,
              ma almeno una classe ha piu candidati. Non vengono fuse assieme: il sistema preseleziona il candidato
              migliore e tu puoi cambiarlo nel pannello di destra.
            </div>
          </div>
          <div class="import-table-meta">
            <span>${exams.length} esami</span>
            <span>${exams.filter(e => e.status === 'review').length} ambigui</span>
          </div>
        </div>
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

  renderMatchSummary(scan, rtAnalysis) {
    const unified = this._buildUnifiedMatchRows(scan, rtAnalysis);
    const cards = [
      ['Found', unified.summary.total || 0, 'soggetti'],
      ['Matched', unified.summary.matched || 0, 'RT univoco'],
      ['Ambiguous', unified.summary.ambiguous || 0, 'da verificare'],
      ['No RT', unified.summary.missing_rt || 0, 'senza match'],
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

  renderUnifiedMatchTable(scan, rtAnalysis) {
    const data = this._buildUnifiedMatchRows(scan, rtAnalysis);
    const allSelected = data.rows.length ? data.rows.every(row => this._subjectSelectionState(row.group || { exams: [] }).all) : false;
    const someSelected = !allSelected && data.rows.some(row => this._subjectSelectionState(row.group || { exams: [] }).selected);
    return `
      <div class="import-cross-card">
        <div class="import-table-head">
          <div>
            <div class="card-title">Subjects</div>
            <div class="import-table-note">
              Una riga per soggetto DICOM accorpato. Qui vedi il match RT senza lasciare la schermata esami.
            </div>
          </div>
        </div>
        <div class="import-series-wrap">
          <table class="import-table import-series-table">
            <thead>
              <tr>
                <th><input type="checkbox" id="subject-select-all" ${allSelected ? 'checked' : ''} ${someSelected ? 'data-indeterminate="true"' : ''}> All</th>
                <th>Soggetto</th>
                <th>ID DICOM</th>
                <th>Studi</th>
                <th>Core status</th>
                <th>RT match</th>
                <th>Nome RT</th>
              </tr>
            </thead>
            <tbody>
              ${data.rows.length ? data.rows.map((row) => {
                const group = this._scanPatientGroups(scan).find(item => item.key === row.key);
                const state = this._subjectSelectionState(group || { exams: [] });
                const selectedRow = this.state.selectedSubjectKey === row.key ? 'selected' : '';
                return `
                <tr class="import-row ${selectedRow}" data-role="subject-row" data-subject-key="${row.key}">
                  <td><input type="checkbox" class="subject-include" data-subject-key="${row.key}" ${state.all ? 'checked' : ''} ${state.some ? 'data-indeterminate="true"' : ''}></td>
                  <td>
                    <div class="import-cell-title">${GlioTwin.fmt(row.patientName)}</div>
                    ${row.patientBirthDate ? `<div class="import-cell-sub">DOB ${GlioTwin.dicomDate(row.patientBirthDate)}</div>` : ''}
                  </td>
                  <td>
                    <div class="import-cell-sub">${row.patientIdAliases.join(', ')}</div>
                  </td>
                  <td>
                    <div class="import-cell-title">${row.examCount}</div>
                    <div class="import-cell-sub">timepoint/studi letti</div>
                  </td>
                  <td>
                    <div class="import-cell-sub">${row.readyCount} ready · ${row.reviewCount} review · ${row.incompleteCount} incomplete</div>
                  </td>
                  <td>
                    <span class="badge badge-import-${row.matchStatus}">${row.matchStatus}</span>
                  </td>
                  <td>
                    ${row.rtNames.length ? row.rtNames.map((name) => `
                      <div class="import-cell-sub">${GlioTwin.fmt(name)}</div>
                    `).join('') : '<span class="text-muted">—</span>'}
                  </td>
                </tr>
              `;
              }).join('') : `<tr><td colspan="7" class="import-empty-cell">Nessun dato disponibile.</td></tr>`}
            </tbody>
          </table>
        </div>
      </div>
    `;
  },

  renderStructureSelector() {
    return `
      <div class="import-structure-box">
        <div class="import-inline-title">Structures to search</div>
        <div class="import-structure-list">
          ${IMPORT_STRUCTURE_OPTIONS.map((option) => `
            <label class="import-structure-chip">
              <input
                type="checkbox"
                class="structure-include"
                data-structure-key="${option.key}"
                ${this.state.selectedStructures.includes(option.key) ? 'checked' : ''}
              >
              <span>${option.label}</span>
            </label>
          `).join('')}
        </div>
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

    app.querySelector('#import-run-discover')?.addEventListener('click', () => this.runDiscover());
    app.querySelector('#import-run-rt-commit')?.addEventListener('click', () => this.runRtCommit());
    app.querySelector('#import-run-commit')?.addEventListener('click', () => this.runCommit());
    app.querySelector('#mu-check-dataset')?.addEventListener('click', () => this.loadPipelineStatus());
    app.querySelector('#mu-run-import')?.addEventListener('click', () => this.runMuImport());
    app.querySelector('#mu-root-path')?.addEventListener('change', (event) => {
      this.state.pipelineRootPath = event.target.value.trim();
    });
    app.querySelector('#lumiere-check-dataset')?.addEventListener('click', () => this.loadLumiereStatus());
    app.querySelector('#lumiere-run-import')?.addEventListener('click', () => this.runLumiereImport());
    app.querySelector('#lumiere-root-path')?.addEventListener('change', (event) => {
      this.state.lumiereRootPath = event.target.value.trim();
    });
    app.querySelectorAll('.structure-include').forEach(input => {
      input.addEventListener('change', (event) => {
        const key = event.target.dataset.structureKey;
        if (event.target.checked) {
          if (!this.state.selectedStructures.includes(key)) this.state.selectedStructures.push(key);
        } else {
          this.state.selectedStructures = this.state.selectedStructures.filter(item => item !== key);
        }
      });
    });
    app.querySelector('.import-cross-card tbody')?.addEventListener('click', (event) => {
      if (event.target.closest('input')) return;
      const row = event.target.closest('[data-role="subject-row"]');
      if (!row) return;
      this.selectSubject(row.dataset.subjectKey, app);
    });
    const subjectSelectAll = app.querySelector('#subject-select-all');
    if (subjectSelectAll?.dataset.indeterminate === 'true') subjectSelectAll.indeterminate = true;
    subjectSelectAll?.addEventListener('change', (event) => {
      this.toggleAllSubjectsSelection(event.target.checked, app);
    });
    app.querySelectorAll('.subject-include').forEach(input => {
      if (input.dataset.indeterminate === 'true') input.indeterminate = true;
      input.addEventListener('change', (event) => {
        this.toggleSubjectSelection(event.target.dataset.subjectKey, event.target.checked, app);
      });
    });
    app.querySelector('.import-table-section tbody')?.addEventListener('click', (event) => {
      if (event.target.closest('input')) return;
      const row = event.target.closest('[data-role="exam-row"]');
      if (!row) return;
      this.selectExam(row.dataset.examKey, app);
    });
    const selectAll = app.querySelector('#exam-select-all');
    if (selectAll?.dataset.indeterminate === 'true') selectAll.indeterminate = true;
    selectAll?.addEventListener('change', (event) => {
      const checked = event.target.checked;
      for (const exam of this._filteredExams(this.state.scan)) {
        this.state.includeExam[exam.exam_key] = checked;
      }
      const exams = this._filteredExams(this.state.scan);
      if (checked && exams.length) this.state.selectedExamKey = exams[0].exam_key;
      this.render(app);
    });
    app.querySelectorAll('.exam-include').forEach(input => {
      input.addEventListener('change', (event) => {
        this.state.includeExam[event.target.dataset.examKey] = event.target.checked;
        const exams = this._filteredExams(this.state.scan).filter(exam => this.state.includeExam[exam.exam_key] !== false);
        if (!exams.some(exam => exam.exam_key === this.state.selectedExamKey)) {
          this.state.selectedExamKey = exams[0]?.exam_key || null;
        }
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
    const hasSelectedExams = selection.examsIncluded > 0;
    return `
      <div class="import-controls import-ops-card card">
        <div class="import-ops-head">
          <div>
            <div class="card-title">Read Sources And Import</div>
            <div class="import-ops-subtitle">Scegli sorgente DICOM e file RT, leggi entrambe le fonti, controlla il match e poi importa i selezionati.</div>
          </div>
          ${(scan || rtAnalysis) ? `<div class="import-preview-chip">${selection.examsIncluded} esami selezionati · ${selection.seriesIncluded} serie</div>` : ''}
        </div>

        <div class="import-dual-grid">
          <div class="import-inline-group">
            <div class="import-inline-title">DICOM source</div>
            <div class="import-control-grid import-control-grid-compact">
              <label class="import-field">
                <span>Cartella</span>
                <input id="import-root" class="import-input" list="import-roots" value="${this.state.selectedRoot || ''}" placeholder="/mnt/dati/irst_data/irst_dicom_raw/DICOM GBM">
                <datalist id="import-roots">
                  ${(this.state.roots || []).map(root => `<option value="${root}"></option>`).join('')}
                </datalist>
              </label>
            </div>
          </div>

          <div class="import-inline-group">
            <div class="import-inline-title">RT source</div>
            <div class="import-control-grid import-control-grid-compact">
              <label class="import-field">
                <span>Excel path</span>
                <input id="rt-import-path" class="import-input" value="${this.state.rtFilePath || ''}" placeholder="/mnt/dati/RT_GBM_MOSAIQ_IMORT.xlsx">
              </label>
            </div>
          </div>
        </div>

        ${this.renderStructureSelector()}

        <div class="import-primary-row">
          <button id="import-run-discover" class="btn" ${(this.state.loading || this.state.rtLoading) ? 'disabled' : ''}>
            ${(this.state.loading || this.state.rtLoading) ? 'Reading…' : 'Scan Sources'}
          </button>
          <button id="import-run-rt-commit" class="btn ${hasSelectedExams ? 'btn-secondary' : 'btn-primary import-primary-btn'}" ${(this.state.rtLoading || !this.state.rtFilePath) ? 'disabled' : ''}>
            ${(this.state.rtLoading) ? 'Working…' : 'Import Clinical RT'}
          </button>
          ${hasSelectedExams ? `
            <button id="import-run-commit" class="btn btn-primary import-primary-btn" ${(!scan || this.state.loading || this.state.rtLoading) ? 'disabled' : ''}>
              ${(this.state.loading || this.state.rtLoading) ? 'Working…' : 'Import Selected'}
            </button>
          ` : ''}
          <div class="import-action-hint"><code>Scan Sources</code> legge DICOM e/o file RT. <code>Import Clinical RT</code> carica solo i dati clinici RT sul dataset gia importato, senza reimportare le immagini. <code>Import Selected</code> importa solo gli studi DICOM selezionati.</div>
        </div>

        <div class="import-helper-text">
          Gli esami in <code>review</code> sono completi: le 4 core sono state trovate, ma almeno una classe ha piu candidati. Il sistema non li accorpa: sceglie il migliore per default e tu puoi cambiarlo nel pannello di destra.
        </div>
        ${!hasSelectedExams && this.state.rtFilePath ? `
          <div class="import-helper-text">
            Non serve selezionare nessuna serie per caricare il file clinico. Puoi usare direttamente <code>Import Clinical RT</code>.
          </div>
        ` : ''}
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

      ${scan && rtAnalysis ? `
        <div class="card import-match-strip">
          ${this.renderMatchSummary(scan, rtAnalysis)}
          ${this.renderUnifiedMatchTable(scan, rtAnalysis)}
        </div>
      ` : scan ? this.renderSummary(scan) : ''}
      ${scan ? this.renderExamTable(scan) : ''}
    `;
  },

  renderPipelineSection(info) {
    if (info.key === 'lumiere') {
      const status = this.state.lumiereStatus;
      const dataset = status?.dataset;
      const counts = status?.db_counts;
      const job = status?.job;
      const available = dataset?.available || {};
      const discovered = dataset?.counts || {};
      return `
        <div class="import-pipeline-card card">
          <div class="card-title">Import LUMIERE</div>
          <div class="import-pipeline-desc">
            Dataset longitudinale LUMIERE: MRI multi-contrasto, segmentazioni HD-GLIO-AUTO (solo T1ce) e dati clinici
            (demografia, IDH, MGMT, OS, RANO). Ogni paziente ha più timepoint settimanali.
          </div>
          <div class="import-form-grid">
            <div class="import-form-field import-form-field-wide">
              <label for="lumiere-root-path">Root dataset</label>
              <input id="lumiere-root-path" class="import-input" value="${this.state.lumiereRootPath || '/mnt/dati/lumiere'}" placeholder="/mnt/dati/lumiere">
            </div>
          </div>
          <div class="import-actions-row">
            <button id="lumiere-check-dataset" class="btn" ${this.state.lumiereLoading ? 'disabled' : ''}>Check Dataset</button>
            <button id="lumiere-run-import" class="btn btn-primary import-primary-btn" ${(this.state.lumiereLoading || job?.running) ? 'disabled' : ''}>Import LUMIERE</button>
          </div>
          ${job ? `
            <div class="import-helper-text">
              Stato: <strong>${job.running ? 'running' : 'idle'}</strong> · ${GlioTwin.fmt(job.last_msg || '—')}
              ${job.error ? `<div class="import-warning-text">${GlioTwin.fmt(job.error)}</div>` : ''}
            </div>
          ` : ''}
          ${dataset ? `
            <div class="import-summary-grid">
              <div class="import-summary-card"><div class="import-summary-label">Imaging</div><div class="import-summary-value">${available.imaging ? 'OK' : 'NO'}</div><div class="import-summary-hint">${discovered.patients || 0} pazienti</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Sessions</div><div class="import-summary-value">${discovered.sessions || 0}</div><div class="import-summary-hint">timepoint trovati</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Demographics</div><div class="import-summary-value">${available.demographics ? 'OK' : 'NO'}</div><div class="import-summary-hint">CSV clinico</div></div>
              <div class="import-summary-card"><div class="import-summary-label">T1ce segs</div><div class="import-summary-value">${discovered.seg_files || 0}</div><div class="import-summary-hint">segmentazioni</div></div>
            </div>
            <div class="import-helper-text">
              Imaging root: <code>${dataset.imaging}</code>
            </div>
          ` : ''}
          ${counts ? `
            <div class="import-summary-grid">
              <div class="import-summary-card"><div class="import-summary-label">Subjects</div><div class="import-summary-value">${counts.subjects}</div><div class="import-summary-hint">nel DB</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Sessions</div><div class="import-summary-value">${counts.sessions}</div><div class="import-summary-hint">timepoint importati</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Sequences</div><div class="import-summary-value">${counts.sequences}</div><div class="import-summary-hint">MRI</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Structures</div><div class="import-summary-value">${counts.structures}</div><div class="import-summary-hint">HD-GLIO-AUTO</div></div>
            </div>
          ` : ''}
        </div>
      `;
    }
    if (info.key === 'mu_glioma_post') {
      const status = this.state.pipelineStatus;
      const dataset = status?.dataset;
      const counts = status?.db_counts;
      const job = status?.job;
      const available = dataset?.available || {};
      const discovered = dataset?.counts || {};
      return `
        <div class="import-pipeline-card card">
          <div class="card-title">Import MU-Glioma-Post</div>
          <div class="import-pipeline-desc">
            MRI, strutture radiologiche e dati clinici vengono importati assieme e associati ai timepoint del dataset.
            Dopo l'import i casi devono essere visibili direttamente nel Viewer; eventuali nuove segmentazioni restano separate dalle strutture importate.
          </div>
          <div class="import-form-grid">
            <div class="import-form-field import-form-field-wide">
              <label for="mu-root-path">Root dataset</label>
              <input id="mu-root-path" class="import-input" value="${this.state.pipelineRootPath || '/mnt/dati/MU-Glioma-Post'}" placeholder="/mnt/dati/MU-Glioma-Post">
            </div>
          </div>
          <div class="import-actions-row">
            <button id="mu-check-dataset" class="btn" ${this.state.pipelineLoading ? 'disabled' : ''}>Check Dataset</button>
            <button id="mu-run-import" class="btn btn-primary import-primary-btn" ${(this.state.pipelineLoading || job?.running) ? 'disabled' : ''}>Import MU</button>
          </div>
          ${job ? `
            <div class="import-helper-text">
              Stato: <strong>${job.running ? 'running' : 'idle'}</strong> · ${GlioTwin.fmt(job.last_msg || '—')}
              ${job.error ? `<div class="import-warning-text">${GlioTwin.fmt(job.error)}</div>` : ''}
            </div>
          ` : ''}
          ${dataset ? `
            <div class="import-summary-grid">
              <div class="import-summary-card"><div class="import-summary-label">MRI</div><div class="import-summary-value">${available.mri ? 'OK' : 'NO'}</div><div class="import-summary-hint">${discovered.patients || 0} pazienti</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Structures</div><div class="import-summary-value">${available.structures ? 'OK' : 'NO'}</div><div class="import-summary-hint">${discovered.mask_files || 0} mask</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Clinical</div><div class="import-summary-value">${available.clinical ? 'OK' : 'NO'}</div><div class="import-summary-hint">excel presente</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Timepoints</div><div class="import-summary-value">${discovered.timepoints || 0}</div><div class="import-summary-hint">cartelle trovate</div></div>
            </div>
            <div class="import-helper-text">
              Data root: <code>${dataset.data_root}</code><br>
              Clinical file: <code>${dataset.clinical_xls}</code><br>
              Volumes file: <code>${dataset.volumes_xls}</code>
            </div>
          ` : ''}
          ${counts ? `
            <div class="import-summary-grid">
              <div class="import-summary-card"><div class="import-summary-label">Subjects</div><div class="import-summary-value">${counts.subjects}</div><div class="import-summary-hint">nel DB</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Sessions</div><div class="import-summary-value">${counts.sessions}</div><div class="import-summary-hint">timepoint importati</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Sequences</div><div class="import-summary-value">${counts.sequences}</div><div class="import-summary-hint">MRI</div></div>
              <div class="import-summary-card"><div class="import-summary-label">Structures</div><div class="import-summary-value">${counts.structures}</div><div class="import-summary-hint">radiologiche</div></div>
            </div>
          ` : ''}
        </div>
      `;
    }
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
                    : 'Il pannello di dettaglio è disponibile solo per il flusso DICOM.'
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
  if (ImportView.state.selectedDataset === 'mu_glioma_post') {
    try {
      await ImportView.loadPipelineStatus();
      return;
    } catch (error) {
      console.error('[mu preload]', error);
    }
  }
  if (ImportView.state.selectedDataset === 'lumiere') {
    try {
      await ImportView.loadLumiereStatus();
      return;
    } catch (error) {
      console.error('[lumiere preload]', error);
    }
  }
  ImportView.render(app);
});
