/* ============================================================
   GlioTwin SPA — router, global state, API helpers
   ============================================================ */

// ── Global state ──────────────────────────────────────────────
const GlioTwin = {
  state: {
    patients: [],          // cached list
    currentPatient: null,
    currentSession: null,
    showSensitive: localStorage.getItem('gliotwin_show_sensitive') === '1',
  },

  // ── API helpers ────────────────────────────────────────────
  async fetch(url) {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${url}`);
    return res.json();
  },

  async post(url, body) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `${res.status} ${res.statusText}`);
    }
    return res.json();
  },

  async put(url, body) {
    const res = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `${res.status} ${res.statusText}`);
    }
    return res.json();
  },

  // ── Toast notifications ────────────────────────────────────
  toast(msg, type = 'info', ms = 4000) {
    let c = document.getElementById('toast-container');
    if (!c) {
      c = document.createElement('div');
      c.id = 'toast-container';
      document.body.appendChild(c);
    }
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.textContent = msg;
    c.appendChild(t);
    setTimeout(() => t.remove(), ms);
  },

  // ── Routing ────────────────────────────────────────────────
  routes: {},

  register(name, handler) {
    this.routes[name] = handler;
  },

  async navigate(hash) {
    const app = document.getElementById('app');
    // normalise: '#/patient/1', 'patient/1', '#patient/1' → all work
    const raw   = (hash || '#/browser').replace(/^#\/?/, '');
    const parts = raw.split('/');
    const view  = parts[0] || 'dashboard';
    const arg   = parts[1] || null;

    // keep URL bar in sync so F5 / back work
    const target = arg ? `#/${view}/${arg}` : `#/${view}`;
    if (location.hash !== target) {
      history.replaceState(null, '', target);
    }

    // update nav active state
    document.querySelectorAll('.nav-item').forEach(el => {
      el.classList.toggle('active', el.dataset.view === view);
    });

    const handler = this.routes[view];
    if (!handler) {
      app.innerHTML = `<div class="empty-state">View not found: ${view}</div>`;
      return;
    }
    try {
      await handler(app, arg);
    } catch (e) {
      console.error('[navigate error]', e);
      // show error inside app without swallowing useful HTML
      app.innerHTML = `<div class="empty-state" style="flex-direction:column;gap:12px">
        <strong style="color:var(--red)">Error loading view</strong>
        <code style="font-size:11px;color:var(--text-dim)">${e.message}</code>
      </div>`;
      this.toast(e.message, 'error');
    }
  },

  // ── Format helpers ────────────────────────────────────────
  humanizeDataset(ds) {
    const labels = {
      irst_dicom_raw:  'IRST',
      lumiere:         'LUMIERE',
      mu_glioma_post:  'MU-Glioma-Post',
      ucsd_ptgbm:      'UCSD-PTGBM',
      rhuh_gbm:        'RHUH-GBM',
      qin_gbm:         'QIN-GBM',
      glis_rt:         'GLIS-RT',
    };
    return labels[ds] || ds || 'Unknown';
  },

  friendlySequenceType(sequenceType) {
    const map = {
      T1ce: 'T1+C',
      CT1: 'T1+C',
      APT: 'APT',
    };
    return map[sequenceType] || sequenceType || 'Series';
  },

  datasetBadge(ds) {
    const map = {
      irst_dicom_raw: 'irst',
      lumiere:        'lumiere',
      mu_glioma_post: 'mu',
      ucsd_ptgbm:     'ucsd',
      rhuh_gbm:       'rhuh',
      qin_gbm:        'qin',
      glis_rt:        'glis',
    };
    const k = map[ds] || 'neutral';
    return `<span class="badge badge-${k}">${this.humanizeDataset(ds)}</span>`;
  },

  statusBadge(s) {
    return `<span class="badge badge-status-${s}">${s}</span>`;
  },

  fmt(val, fallback = '—') {
    if (val === null || val === undefined || val === '') return fallback;
    return val;
  },

  fmtVol(ml) {
    if (ml === null || ml === undefined) return '—';
    return ml.toFixed(1) + ' mL';
  },

  fmtDays(d) {
    if (d === null || d === undefined) return '—';
    if (d === 0) return 'T0';
    return (d > 0 ? '+' : '') + d + 'd';
  },

  dicomDate(raw, fallback = '—') {
    if (!raw) return fallback;
    const text = String(raw).trim();
    if (/^\d{8}$/.test(text)) {
      return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    }
    return text || fallback;
  },

  fmtDate(raw, fallback = '—') {
    if (!raw) return fallback;
    const text = String(raw).trim();
    if (!text) return fallback;
    if (/^\d{8}$/.test(text)) {
      return `${text.slice(6, 8)}-${text.slice(4, 6)}-${text.slice(0, 4)}`;
    }
    const match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (match) {
      return `${match[3]}-${match[2]}-${match[1]}`;
    }
    return text;
  },

  personName(item) {
    if (!item) return '';
    const direct = this.fmt(item.patient_name, '').trim();
    if (direct) return direct;
    const parts = [this.fmt(item.patient_given_name, ''), this.fmt(item.patient_family_name, '')]
      .map(value => value.trim())
      .filter(Boolean);
    return parts.join(' ');
  },

  patientPrimary(item) {
    if (!item) return '—';
    const code = item.subject_id || item.patient_code || item.patient_id;
    if (this.state.showSensitive) {
      return this.personName(item) || this.fmt(code);
    }
    return this.fmt(code);
  },

  patientSecondary(item) {
    if (!item || !this.state.showSensitive) return '';
    const details = [];
    const subjectId = this.fmt(item.subject_id || item.patient_code || item.patient_id, '').trim();
    if (subjectId) details.push(subjectId);
    if (item.patient_birth_date) details.push(`DOB ${this.fmtDate(item.patient_birth_date)}`);
    if (item.sex) details.push(item.sex);
    return details.join(' · ');
  },

  examDate(item) {
    return item?.study_date ? this.fmtDate(item.study_date) : '—';
  },

  sessionMeta(item) {
    if (!this.state.showSensitive) return '';
    const details = [];
    if (item?.study_date) details.push(this.fmtDate(item.study_date));
    return details.join(' · ');
  },

  updateSensitiveToggle() {
    const btn = document.getElementById('phi-toggle');
    if (!btn) return;
    btn.textContent = this.state.showSensitive ? 'PHI On' : 'PHI Off';
    btn.classList.toggle('active', this.state.showSensitive);
  },

  setSensitiveVisible(show) {
    this.state.showSensitive = !!show;
    localStorage.setItem('gliotwin_show_sensitive', this.state.showSensitive ? '1' : '0');
    this.updateSensitiveToggle();
    this.navigate(location.hash || '#/browser');
  },

  idh(s) {
    if (!s || s === 'unknown') return '<span class="text-muted">IDH ?</span>';
    const ok = s === 'mutated';
    return `<span class="bm-chip ${ok ? 'positive' : 'negative'}">IDH ${s}</span>`;
  },

  mgmt(s) {
    if (!s || s === 'unknown') return '<span class="text-muted">MGMT ?</span>';
    const ok = s === 'methylated';
    return `<span class="bm-chip ${ok ? 'positive' : 'negative'}">MGMT ${s}</span>`;
  },
};

// ── Bootstrap ──────────────────────────────────────────────────

// alias: #/analysis → global-metrics (phase rename, keep old hash for back-compat)
GlioTwin.register('analysis', async (app, arg) => {
  const handler = GlioTwin.routes['global-metrics'];
  if (handler) return handler(app, arg);
});

window.addEventListener('hashchange', () => GlioTwin.navigate(location.hash));

document.addEventListener('DOMContentLoaded', async () => {
  GlioTwin.updateSensitiveToggle();
  document.getElementById('phi-toggle')?.addEventListener('click', () => {
    GlioTwin.setSensitiveVisible(!GlioTwin.state.showSensitive);
  });

  // wire nav clicks
  document.querySelectorAll('.nav-item').forEach(el => {
    el.addEventListener('click', e => {
      e.preventDefault();
      location.hash = `#/${el.dataset.view}`;
    });
  });

  // initial route
  await GlioTwin.navigate(location.hash || '#/browser');
});
