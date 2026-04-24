'use strict';

/* ============================================================
   Analysis view
   Layout identico al viewer:
     LEFT   : ricerca, lista pazienti, lista sessioni, mini-info
     RIGHT  : tab Patient Data / Longitudinal Metrics / Cohort / Status
   ============================================================ */

// ── Stato modulo ─────────────────────────────────────────────
let _AN = {
  patients:    null,
  sessions:    [],
  selPid:      null,
  selSid:      null,
  patDetail:   null,
  activeTab:   'patient',
  // longitudinal
  timeline:    null,
  tlLoading:   false,
  metricStatus: null,
  selLabel:    '',
  selSequence: 'APT',
  selSource:   'preferred',
  tlReqSeq:    0,
  // cohort
  cohortRows:  null,
  labelFilter: 'all',
  sourceFilter:'computed',
  colorBy:     'patient',
  // status
  statusData:  null,
};
let _anCharts  = [];
let _anPollTimer = null;

// ─── Costanti coorte ──────────────────────────────────────────
const _AN_PALETTE = [
  '#3b7ef8','#f97316','#22d3ee','#a78bfa','#34d399','#fb923c',
  '#f472b6','#facc15','#60a5fa','#4ade80','#e879f9','#94a3b8',
  '#ff6b6b','#48cae4','#ffb703','#06d6a0','#ef476f','#118ab2',
];
const _AN_LABEL_COLORS = {
  enhancing_tumor:'#f97316', edema:'#3b7ef8',
  necrotic_core:'#ef4444',   resection_cavity:'#a78bfa',
};
const _AN_LABEL_NAMES = {
  enhancing_tumor:'Enhancing Tumor', edema:'Edema',
  necrotic_core:'Necrotic Core',     resection_cavity:'Resection Cavity',
};
const _AN_LABEL_SHORT  = { enhancing_tumor:'ET', edema:'ED', necrotic_core:'NC', resection_cavity:'RC' };
const _AN_LABEL_ORDER  = ['resection_cavity','necrotic_core','edema','enhancing_tumor'];
const _anPatColorMap   = new Map();

// ─── Helpers ──────────────────────────────────────────────────
function _anDisposeCharts() {
  for (const c of _anCharts) { try { c.dispose(); } catch (_) {} }
  _anCharts = [];
}

function _anParseDate(v) {
  if (!v) return null;
  const m = String(v).trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const d = new Date(Date.UTC(+m[1], +m[2]-1, +m[3]));
  return isNaN(d.getTime()) ? null : d;
}
const _anPointDate = p => _anParseDate(p?.study_date);
const _anEventDate = e => _anParseDate(e?.event_date);
const _anFmtMetric = v => Number.isFinite(v) ? (Math.abs(v)>=100 ? v.toFixed(1) : v.toFixed(2)) : '—';

function _anFriendlyLabel(label) {
  const map = {
    enhancing_tumor:'Enhancing Tumor', edema:'Edema',
    necrotic_core:'Necrotic Core',     necrosis:'Necrotic Core',
    resection_cavity:'Resection Cavity', tumor_core:'Tumor Core',
    whole_tumor:'Whole Tumor', tumor_mask:'Tumor Mask',
    et:'Enhancing Tumor', snfh:'Edema', netc:'Necrotic Core', rc:'Resection Cavity',
  };
  const k = String(label||'').trim().toLowerCase();
  return map[k] || String(label||'Structure').replaceAll('_',' ').replace(/\b\w/g,m=>m.toUpperCase());
}

function _anPatColor(sid) {
  if (!_anPatColorMap.has(sid))
    _anPatColorMap.set(sid, _AN_PALETTE[_anPatColorMap.size % _AN_PALETTE.length]);
  return _anPatColorMap.get(sid);
}
function _anPointColor(row, colorBy) {
  if (colorBy === 'label')     return _AN_LABEL_COLORS[row.label] || '#8395b0';
  if (colorBy === 'timepoint') {
    const t = Math.min((parseInt((row.session_label||'').replace(/\D/g,''))||1-1)/6, 1);
    return `hsl(${220-t*140},80%,${55+t*15}%)`;
  }
  return _anPatColor(row.subject_id);
}
function _anJitter() { return (Math.random()-0.5)*0.42; }
function _anBoxStats(vals) {
  if (!vals.length) return null;
  const s=[...vals].sort((a,b)=>a-b);
  const q=p=>{const pos=(s.length-1)*p,lo=Math.floor(pos),hi=Math.ceil(pos);return s[lo]+(s[hi]-s[lo])*(pos-lo);};
  const q1=q(.25),q3=q(.75),iqr=q3-q1;
  return [Math.max(s[0],q1-1.5*iqr),q1,q(.5),q3,Math.min(s[s.length-1],q3+1.5*iqr)];
}

// ─── Statistiche ──────────────────────────────────────────────
function _anNormalCDF(x){const t=1/(1+0.2316419*Math.abs(x)),d=0.3989423*Math.exp(-x*x/2);const p=d*t*(0.3193815+t*(-0.3565638+t*(1.7814779+t*(-1.8212560+t*1.3302744))));return x>0?1-p:p;}
function _anLogGamma(x){const c=[0.99999999999980993,676.5203681218851,-1259.1392167224028,771.32342877765313,-176.61502916214059,12.507343278686905,-0.13857109526572012,9.9843695780195716e-6,1.5056327351493116e-7];if(x<0.5)return Math.log(Math.PI/Math.sin(Math.PI*x))-_anLogGamma(1-x);x-=1;let a=c[0];const t=x+7.5;for(let i=1;i<9;i++)a+=c[i]/(x+i);return 0.5*Math.log(2*Math.PI)+(x+0.5)*Math.log(t)-t+Math.log(a);}
function _anGammaRegP(a,x){if(x<0)return 0;if(x<a+1){let term=1/a,sum=term;for(let n=1;n<200;n++){term*=x/(a+n);sum+=term;if(Math.abs(term)<1e-10*Math.abs(sum))break;}return Math.exp(-x+a*Math.log(x)-_anLogGamma(a))*sum;}let b=x+1-a,c=1e30,d=1/b,h=d;for(let i=1;i<=200;i++){const an=-i*(i-a);b+=2;d=an*d+b;if(Math.abs(d)<1e-30)d=1e-30;c=b+an/c;if(Math.abs(c)<1e-30)c=1e-30;d=1/d;const del=d*c;h*=del;if(Math.abs(del-1)<1e-10)break;}return 1-Math.exp(-x+a*Math.log(x)-_anLogGamma(a))*h;}
function _anChi2p(H,df){return(H<=0||df<=0)?1:1-_anGammaRegP(df/2,H/2);}
function _anMannWhitney(a,b){if(a.length<2||b.length<2)return null;const tagged=[...a.map(v=>({v,g:0})),...b.map(v=>({v,g:1}))].sort((x,y)=>x.v-y.v);const ranks=new Array(tagged.length);let i=0;while(i<tagged.length){let j=i;while(j<tagged.length-1&&tagged[j+1].v===tagged[i].v)j++;const avg=(i+j)/2+1;for(let k=i;k<=j;k++)ranks[k]=avg;i=j+1;}let R1=0;tagged.forEach((item,idx)=>{if(item.g===0)R1+=ranks[idx];});const n1=a.length,n2=b.length,U1=R1-n1*(n1+1)/2,U2=n1*n2-U1,U=Math.min(U1,U2);const mu=n1*n2/2,sigma=Math.sqrt(n1*n2*(n1+n2+1)/12),z=(U-mu)/sigma;return{u:U,z,p:2*_anNormalCDF(-Math.abs(z)),r:(U1-U2)/(n1*n2),n1,n2};}
function _anKruskalWallis(groupsMap,labels){const groups=labels.map(l=>groupsMap[l]||[]).filter(g=>g.length>1);if(groups.length<2)return null;const N=groups.reduce((s,g)=>s+g.length,0);if(N<4)return null;const tagged=groups.flatMap((g,gi)=>g.map(v=>({v,gi}))).sort((a,b)=>a.v-b.v);const ranks=new Array(tagged.length);let i=0;while(i<tagged.length){let j=i;while(j<tagged.length-1&&tagged[j+1].v===tagged[i].v)j++;const avg=(i+j)/2+1;for(let k=i;k<=j;k++)ranks[k]=avg;i=j+1;}const rankSums=new Array(groups.length).fill(0);tagged.forEach((item,idx)=>{rankSums[item.gi]+=ranks[idx];});const H=(12/(N*(N+1)))*groups.reduce((s,g,gi)=>s+rankSums[gi]**2/g.length,0)-3*(N+1);const df=groups.length-1;return{H,df,p:_anChi2p(H,df),N};}

// ═══════════════════════════════════════════════════════════════
// SHELL: renderizza il layout intero
// ═══════════════════════════════════════════════════════════════
function _anRenderShell(app) {
  const tab = _AN.activeTab;
  app.innerHTML = `
  <div class="app-layout">

    <!-- ══ SINISTRA ══ -->
    <div class="data-panel">
      <div class="data-panel-scroll">

        <div class="data-sec">
          <div class="data-sec-title">Filters</div>
          <div class="filter-grid">
            <div class="filter-row">
              <select class="f-sel" id="an-f-ds">
                <option value="">All datasets</option>
                <option value="irst_dicom_raw">DICOM</option>
                <option value="mu_glioma_post">MU-Glioma-Post</option>
                <option value="lumiere">LUMIERE</option>
                <option value="ucsd_ptgbm">UCSD-PTGBM</option>
                <option value="rhuh_gbm">RHUH-GBM</option>
                <option value="qin_gbm">QIN-GBM</option>
                <option value="glis_rt">GLIS-RT</option>
              </select>
              <select class="f-sel" id="an-f-vital">
                <option value="">All outcomes</option>
                <option value="alive">Alive</option>
                <option value="deceased">Deceased</option>
              </select>
            </div>
            <div class="filter-row">
              <select class="f-sel" id="an-f-idh">
                <option value="">IDH all</option>
                <option value="mutated">IDH mutated</option>
                <option value="wildtype">IDH wildtype</option>
              </select>
              <select class="f-sel" id="an-f-mgmt">
                <option value="">MGMT all</option>
                <option value="methylated">Methylated</option>
                <option value="unmethylated">Unmethylated</option>
              </select>
            </div>
            <input class="f-search" id="an-f-q" placeholder="Search patient ID…" autocomplete="off">
          </div>
        </div>

        <div class="data-sec">
          <div class="data-sec-title" id="an-pt-title">Patients</div>
          <div class="tree-scroll" id="an-patient-list">
            <div class="loading-screen" style="height:60px">
              <div class="spinner" style="width:18px;height:18px;border-width:2px"></div>
            </div>
          </div>
        </div>

      </div>
    </div>

    <!-- ══ DESTRA ══ -->
    <div class="right-col">
      <div class="right-tabs">
        <button class="right-tab ${tab==='patient'?'active':''}"      id="an-tab-patient">Patient Data</button>
        <button class="right-tab ${tab==='longitudinal'?'active':''}" id="an-tab-longitudinal">Longitudinal Metrics</button>
        <button class="right-tab ${tab==='cohort'?'active':''}"        id="an-tab-cohort">Cohort Analysis</button>
        <button class="right-tab ${tab==='status'?'active':''}"        id="an-tab-status">Stato Dati</button>
      </div>
      <div class="signal-panel" id="an-panel-patient"      style="${tab==='patient'     ?'':'display:none'}"></div>
      <div class="signal-panel" id="an-panel-longitudinal" style="${tab==='longitudinal'?'':'display:none'}"></div>
      <div class="signal-panel" id="an-panel-cohort"       style="${tab==='cohort'      ?'':'display:none'}"></div>
      <div class="signal-panel" id="an-panel-status"       style="${tab==='status'      ?'':'display:none'}"></div>
    </div>

  </div>`;
}

// ─── Rendering lista pazienti ─────────────────────────────────
function _anRenderPtList() {
  const el    = document.getElementById('an-patient-list');
  const ti    = document.getElementById('an-pt-title');
  if (!el) return;
  const all  = _AN.patients || [];
  const list = _anFilteredPatients();
  if (ti) ti.textContent = `Patients (${list.length}${list.length < all.length ? '/' + all.length : ''})`;
  el.innerHTML = list.length
    ? list.map(p => `
        <div class="tree-item ${_AN.selPid===p.id?'selected':''}" data-pid="${p.id}"
             onclick="anSelectPt(${p.id})">
          <div class="tree-item-main">
            <span class="tree-item-id">${GlioTwin.patientPrimary(p)}</span>
            ${GlioTwin.patientSecondary(p) ? `<span class="tree-item-sub">${GlioTwin.patientSecondary(p)}</span>` : ''}
          </div>
          <span class="tree-item-meta">${p.n_sessions||0}×</span>
          ${GlioTwin.datasetBadge(p.dataset)}
        </div>`).join('')
    : '<div class="tree-hint">No results</div>';
}

function _anFilteredPatients() {
  const q     = (document.getElementById('an-f-q')?.value    || '').toLowerCase();
  const ds    = document.getElementById('an-f-ds')?.value    || '';
  const vital = document.getElementById('an-f-vital')?.value || '';
  const idh   = document.getElementById('an-f-idh')?.value   || '';
  const mgmt  = document.getElementById('an-f-mgmt')?.value  || '';
  const all   = _AN.patients || [];
  return all.filter(p => {
    if (ds    && p.dataset       !== ds)    return false;
    if (vital && p.vital_status  !== vital) return false;
    if (idh   && p.idh_status    !== idh)   return false;
    if (mgmt  && p.mgmt_status   !== mgmt)  return false;
    if (q) {
      const hay = [p.subject_id, p.patient_name, p.patient_given_name, p.patient_family_name]
        .filter(Boolean).join(' ').toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
}

// ─── Selezione paziente ───────────────────────────────────────
window.anSelectPt = async (pid) => {
  clearTimeout(_anPollTimer);
  _anDisposeCharts();
  _AN.selPid    = pid;
  _AN.patDetail = null;
  _AN.timeline  = null;
  _AN.metricStatus = null;
  GlioTwin.state.currentPatient = pid;

  _anRenderPtList();

  // Carica dettaglio paziente in background (serve per Patient Data e timeline)
  try {
    _AN.patDetail = await GlioTwin.fetch(`/api/patients/${pid}`);
  } catch(_) {}

  _anRenderActiveTab();
  if (_AN.activeTab === 'longitudinal') await _anLoadTimeline();
};


// ═══════════════════════════════════════════════════════════════
// TAB SWITCHER
// ═══════════════════════════════════════════════════════════════
function _anSwitchTab(tab) {
  _anDisposeCharts();
  _AN.activeTab = tab;
  ['longitudinal','patient','cohort','status'].forEach(t => {
    document.getElementById(`an-panel-${t}`)?.style.setProperty('display', t===tab ? '' : 'none');
    document.getElementById(`an-tab-${t}`)?.classList.toggle('active', t===tab);
  });
  _anRenderActiveTab();
}

async function _anRenderActiveTab() {
  const tab = _AN.activeTab;
  if (tab === 'longitudinal') { _anRenderLongPanel(); if (_AN.selPid && !_AN.timeline && !_AN.tlLoading) await _anLoadTimeline(); }
  if (tab === 'patient')      _anRenderPatientEditor();
  if (tab === 'cohort')       await _anRenderOrLoadCohort();
  if (tab === 'status')       await _anLoadStatus();
}

// ═══════════════════════════════════════════════════════════════
// TAB: LONGITUDINAL METRICS
// ═══════════════════════════════════════════════════════════════
async function _anLoadTimeline() {
  if (!_AN.selPid) return;
  const seq = ++_AN.tlReqSeq;
  _AN.tlLoading = true;
  _anRenderLongPanel();
  try {
    const params = new URLSearchParams({ _ts: Date.now() });
    if (_AN.selLabel)    params.set('label', _AN.selLabel);
    if (_AN.selSequence) params.set('sequence_type', _AN.selSequence);
    if (_AN.selSource)   params.set('structure_source', _AN.selSource);
    const [tl, ms] = await Promise.all([
      GlioTwin.fetch(`/api/patients/${_AN.selPid}/signal-timeline?${params}`),
      GlioTwin.fetch(`/api/signal-metrics/status?_ts=${Date.now()}`).catch(()=>null),
    ]);
    if (seq !== _AN.tlReqSeq) return;
    _AN.timeline     = tl;
    _AN.metricStatus = ms;
    _AN.selLabel     = tl.selected_label            || '';
    _AN.selSequence  = tl.selected_sequence_type    || 'APT';
    _AN.selSource    = tl.selected_source           || 'preferred';
  } catch(e) {
    if (seq !== _AN.tlReqSeq) return;
    _AN.timeline = { points:[], clinical_events:[], available_labels:[],
      available_sequence_types:[], available_sources:['preferred'], error:e.message };
  } finally {
    if (seq === _AN.tlReqSeq) _AN.tlLoading = false;
  }
  _anRenderLongPanel();
}

function _anRenderLongPanel() {
  const host = document.getElementById('an-panel-longitudinal');
  if (!host) return;
  _anDisposeCharts();

  if (!_AN.selPid) {
    host.innerHTML = `<div class="signal-panel-head"><div><div class="signal-panel-title">Longitudinal Metrics</div><div class="signal-panel-sub" style="margin-top:8px;color:var(--text-muted)">Seleziona un paziente dalla lista per vedere le metriche longitudinali.</div></div></div>`;
    return;
  }
  if (_AN.tlLoading) {
    host.innerHTML = `<div class="gm-loading"><div class="spinner"></div><span>Caricamento metriche longitudinali…</span></div>`;
    return;
  }
  const tl = _AN.timeline;
  if (!tl) { host.innerHTML = `<div class="gm-loading"><div class="spinner"></div></div>`; return; }
  if (tl.error) {
    host.innerHTML = `<div class="signal-panel-head"><div class="signal-panel-title">Longitudinal Metrics</div><div style="color:var(--red);padding:16px">Errore: ${tl.error}</div></div>`;
    return;
  }

  const labels    = tl.available_labels          || [];
  const sequences = tl.available_sequence_types  || [];
  const sources   = tl.available_sources         || ['preferred','radiological','computed'];
  const points    = (tl.points||[]).filter(p => p.signal && Number.isFinite(p.signal.median));
  const volPts    = points.filter(p => Number.isFinite(p.volume_ml));
  const sigPts    = points.map(p => ({...p, median:p.signal.median, q1:p.signal.q1, q3:p.signal.q3}));

  const job     = _AN.metricStatus?.latest_job;
  const running = ['queued','running'].includes(job?.status);
  const progress= running && job?.total_tasks>0 ? ` (${job.completed_tasks??0}/${job.total_tasks})` : '';

  host.innerHTML = `
    <div class="signal-panel-head">
      <div>
        <div class="signal-panel-title">Longitudinal Metrics</div>
        <div class="signal-panel-status">
          <span class="signal-status-chip ${points.length?'ok':'warn'}">${points.length} timepoint con metriche</span>
          ${running ? `<span class="signal-status-chip warn">Calcolo in corso${progress}…</span>` : ''}
          ${job && !running ? `<span class="signal-status-chip">Job #${job.id}: ${job.status}</span>` : ''}
        </div>
      </div>
      <div class="signal-panel-actions">
        <button class="btn signal-calc-btn ${running?'signal-calc-btn--running':''}"
                id="an-calc-btn" ${running?'disabled':''}>
          ${running ? `<span class="signal-calc-spinner"></span> Calcolo${progress}…` : 'Aggiorna metriche'}
        </button>
        <button class="btn btn-secondary signal-calc-btn-force"
                id="an-calc-force-btn" ${running?'disabled':''}>Ricalcola tutto</button>
      </div>
    </div>
    <div class="signal-metric-section">
      <div class="signal-metric-head">
        <div>
          <div class="signal-chart-title">Volume struttura</div>
          <div class="signal-metric-sub">Andamento del volume nel tempo.</div>
        </div>
        <div class="signal-panel-controls">
          <label><span>Struttura</span>
            <select id="an-sel-label">
              ${labels.map(l=>`<option value="${l}" ${l===_AN.selLabel?'selected':''}>${_anFriendlyLabel(l)}</option>`).join('')}
            </select>
          </label>
          <label><span>Sorgente</span>
            <select id="an-sel-source">
              ${sources.map(s=>`<option value="${s}" ${s===_AN.selSource?'selected':''}>${s}</option>`).join('')}
            </select>
          </label>
        </div>
      </div>
      <div class="signal-chart-wrap">
        <div class="signal-chart-host" id="an-vol-chart"></div>
        <div class="signal-empty" id="an-vol-chart-empty" style="display:none">Nessun dato di volume disponibile.</div>
      </div>
    </div>
    <div class="signal-metric-section">
      <div class="signal-metric-head">
        <div>
          <div class="signal-chart-title">Segnale voxel</div>
          <div class="signal-metric-sub">Segnale mediano ± IQR nella struttura, sulla serie scelta.</div>
        </div>
        <div class="signal-panel-controls">
          <label><span>Serie</span>
            <select id="an-sel-sequence">
              ${sequences.map(s=>`<option value="${s}" ${s===_AN.selSequence?'selected':''}>${GlioTwin.friendlySequenceType(s)}</option>`).join('')}
            </select>
          </label>
        </div>
      </div>
      <div class="signal-chart-wrap">
        <div class="signal-chart-host" id="an-sig-chart"></div>
        <div class="signal-empty" id="an-sig-chart-empty" style="display:none">Nessun dato segnale disponibile.</div>
      </div>
    </div>
  `;

  _anBuildTimeChart({ cid:'an-vol-chart', points:volPts, events:tl.clinical_events||[], vk:'volume_ml', yLabel:'mL', title:'Volume' });
  _anBuildTimeChart({ cid:'an-sig-chart', points:sigPts, events:tl.clinical_events||[], vk:'median', yLabel:'signal', title:'Segnale', lk:'q1', uk:'q3' });

  host.querySelector('#an-calc-btn')?.addEventListener('click', () => _anQueueMetrics(false));
  host.querySelector('#an-calc-force-btn')?.addEventListener('click', () => _anQueueMetrics(true));
  host.querySelector('#an-sel-label')?.addEventListener('change', e => { _AN.selLabel=e.target.value; _anLoadTimeline(); });
  host.querySelector('#an-sel-source')?.addEventListener('change', e => { _AN.selSource=e.target.value; _anLoadTimeline(); });
  host.querySelector('#an-sel-sequence')?.addEventListener('change', e => { _AN.selSequence=e.target.value; _anLoadTimeline(); });
}

function _anBuildTimeChart({ cid, points, events=[], title, yLabel, vk, lk=null, uk=null }) {
  const host  = document.getElementById(cid);
  const empty = document.getElementById(`${cid}-empty`);
  if (!host || !empty) return;
  if (!points.length) { host.style.display='none'; empty.style.display='block'; return; }
  host.style.display='block'; empty.style.display='none';

  const seriesData = points.map(p => {
    const dt=_anPointDate(p);
    return dt && Number.isFinite(p[vk]) ? {value:[dt.getTime(),p[vk]], point:p} : null;
  }).filter(Boolean);

  const evMarkers = events.map(e => {
    const dt=_anEventDate(e);
    return dt ? {xAxis:dt.getTime(), lineStyle:{color:'#f59e0b',width:2,type:'dashed'},
      label:{show:true, formatter:`${e.event_type||'Event'}\n${GlioTwin.fmtDate(e.event_date)}`,
             color:'#fbbf24', fontWeight:700, position:'insideEndTop', distance:6}} : null;
  }).filter(Boolean);

  const allTimes = [...seriesData.map(d=>d.value[0]), ...evMarkers.map(m=>m.xAxis)].sort((a,b)=>a-b);
  const upperData = uk ? points.map(p=>{const dt=_anPointDate(p);return dt&&Number.isFinite(p[uk])?[dt.getTime(),p[uk]]:null;}).filter(Boolean) : [];
  const lowerData = lk ? points.map(p=>{const dt=_anPointDate(p);return dt&&Number.isFinite(p[lk])?[dt.getTime(),p[lk]]:null;}).filter(Boolean) : [];

  const chart = echarts.init(host, null, {renderer:'canvas'});
  _anCharts.push(chart);
  chart.setOption({
    animation:false, backgroundColor:'transparent',
    grid:{left:58,right:20,top:40,bottom:48},
    tooltip:{trigger:'axis', backgroundColor:'rgba(11,13,15,0.95)', borderColor:'#2a3342',
      textStyle:{color:'#e5edf7'},
      formatter:items=>{const first=Array.isArray(items)?items[0]:items;const p=first?.data?.point;if(!p)return '';return [`<strong>RM ${GlioTwin.fmtDate(p.study_date,p.session_label||'')}</strong>`,`${title}: ${_anFmtMetric(p[vk])}`].join('<br>');}},
    xAxis:{type:'time', min:allTimes[0], max:allTimes[allTimes.length-1],
      axisLabel:{color:'#9aa6b8',formatter:v=>echarts.format.formatTime('dd-MM-yyyy',v)},
      axisLine:{lineStyle:{color:'#445063'}}, splitLine:{lineStyle:{color:'rgba(154,166,184,0.14)'}}},
    yAxis:{type:'value', name:yLabel, nameTextStyle:{color:'#9aa6b8',padding:[0,0,0,-8]},
      axisLabel:{color:'#9aa6b8'}, axisLine:{lineStyle:{color:'#445063'}},
      splitLine:{lineStyle:{color:'rgba(154,166,184,0.14)'}}},
    series:[
      ...(uk&&lk?[
        {type:'line',data:upperData,lineStyle:{opacity:0},symbol:'none',stack:'iqr',areaStyle:{color:'rgba(59,126,248,0.14)'}},
        {type:'line',data:lowerData.map((item,i)=>[item[0],(upperData[i]?.[1]??0)-item[1]]),lineStyle:{opacity:0},symbol:'none',stack:'iqr',areaStyle:{color:'rgba(59,126,248,0.14)'}},
      ]:[]),
      {name:title, type:'line', smooth:false, symbol:'circle', symbolSize:8,
        itemStyle:{color:'#3b7ef8',borderColor:'#0f1623',borderWidth:2},
        lineStyle:{color:'#3b7ef8',width:3},
        label:{show:true,position:'top',color:'#e5edf7',fontWeight:700,formatter:item=>_anFmtMetric(item.data.value[1])},
        data:seriesData, markLine:{symbol:['none','none'],silent:true,data:evMarkers}},
    ],
  });
  chart.resize();
}

async function _anQueueMetrics(force) {
  try {
    const job = await GlioTwin.post('/api/signal-metrics/jobs/queue-missing', {patient_id:null, force});
    _AN.metricStatus = {latest_job:job, cached_rows:_AN.metricStatus?.cached_rows||0};
    GlioTwin.toast(`${force?'Ricalcolo completo':'Calcolo metriche mancanti'} avviato (job #${job.id})`, 'info');
  } catch(e) { GlioTwin.toast(`Errore avvio job: ${e.message}`, 'error'); }
  _anRenderLongPanel();
  _anScheduleMetricPoll();
}

function _anScheduleMetricPoll() {
  clearTimeout(_anPollTimer);
  const job = _AN.metricStatus?.latest_job;
  if (!job || !['queued','running'].includes(job.status)) return;
  const prevId = job.id;
  _anPollTimer = setTimeout(async () => {
    if (!location.hash.startsWith('#/analysis')) return;
    try { _AN.metricStatus = await GlioTwin.fetch(`/api/signal-metrics/status?_ts=${Date.now()}`); } catch(_) {}
    if (_AN.activeTab === 'longitudinal') _anRenderLongPanel();
    if (_AN.activeTab === 'status')       _anRenderStatusPanel();
    const newJob = _AN.metricStatus?.latest_job;
    if (newJob?.id === prevId && !['queued','running'].includes(newJob.status)) {
      if (newJob.status === 'completed') {
        const n=newJob.completed_tasks??0, f=newJob.failed_tasks??0;
        GlioTwin.toast(n===0?'Nessuna metrica mancante.':`Completato: ${n} calcolate${f>0?`, ${f} vuote`:''}`, n===0?'info':'success');
      } else if (newJob.status === 'failed') {
        GlioTwin.toast(`Calcolo fallito: ${newJob.error_message||'errore sconosciuto'}`, 'error');
      }
    } else { _anScheduleMetricPoll(); }
  }, 2500);
}

// ═══════════════════════════════════════════════════════════════
// TAB: PATIENT DATA
// ═══════════════════════════════════════════════════════════════
function _anRenderPatientEditor() {
  const host = document.getElementById('an-panel-patient');
  if (!host) return;
  const p = _AN.patDetail;
  if (!p) {
    host.innerHTML = `<div class="signal-panel-head"><div><div class="signal-panel-title">Patient Data</div><div class="signal-panel-sub" style="margin-top:8px;color:var(--text-muted)">Seleziona un paziente dalla lista.</div></div></div>`;
    return;
  }
  const latestRt = p.latest_radiotherapy_course || {};
  const refs = p.external_ref_map || {};
  host.innerHTML = `
    <div class="signal-panel-head">
      <div>
        <div class="signal-panel-title">Patient Data</div>
        <div class="signal-panel-sub">Modifica manuale dei campi anagrafici e clinici principali.</div>
      </div>
    </div>
    <form class="patient-edit-form" id="an-patient-form">
      <label><span>Patient Name</span>    <input class="patient-edit-input" name="patient_name"        value="${GlioTwin.fmt(p.patient_name,'')}"></label>
      <label><span>Given Name</span>      <input class="patient-edit-input" name="patient_given_name"  value="${GlioTwin.fmt(p.patient_given_name,'')}"></label>
      <label><span>Family Name</span>     <input class="patient-edit-input" name="patient_family_name" value="${GlioTwin.fmt(p.patient_family_name,'')}"></label>
      <label><span>Birth Date</span>      <input class="patient-edit-input" name="patient_birth_date"  value="${GlioTwin.fmt(p.patient_birth_date,'')}" placeholder="YYYYMMDD"></label>
      <label><span>Sex</span>             <input class="patient-edit-input" name="sex"                 value="${GlioTwin.fmt(p.sex,'')}"></label>
      <label><span>Diagnosis</span>       <input class="patient-edit-input" name="diagnosis"           value="${GlioTwin.fmt(p.diagnosis,'')}"></label>
      <label><span>Diagnosis Date</span>  <input type="date" class="patient-edit-input" name="diagnosis_date" value="${GlioTwin.fmt(p.diagnosis_date,'')}"></label>
      <label><span>Death Date</span>      <input type="date" class="patient-edit-input" name="death_date" value="${GlioTwin.fmt(p.death_date,'')}"></label>
      <label><span>IDH</span>             <input class="patient-edit-input" name="idh_status"          value="${GlioTwin.fmt(p.idh_status,'')}"></label>
      <label><span>MGMT</span>            <input class="patient-edit-input" name="mgmt_status"         value="${GlioTwin.fmt(p.mgmt_status,'')}"></label>
      <label><span>Age At Dx</span>       <input type="number" step="0.1" class="patient-edit-input" name="age_at_diagnosis" value="${p.age_at_diagnosis??''}"></label>
      <label><span>OS Days</span>         <input type="number" class="patient-edit-input" name="os_days" value="${p.os_days??''}"></label>
      <label><span>Vital Status</span>    <input class="patient-edit-input" name="vital_status"        value="${GlioTwin.fmt(p.vital_status,'')}"></label>
      <label><span>IDA</span>             <input class="patient-edit-input" name="ida"                 value="${GlioTwin.fmt(refs.ida||latestRt.external_course_id,'')}"></label>
      <label><span>Tax Code</span>        <input class="patient-edit-input" name="tax_code"            value="${GlioTwin.fmt(refs.tax_code||latestRt.tax_code,'')}"></label>
      <label><span>RT Start</span>        <input type="date" class="patient-edit-input" name="radiotherapy_start_date" value="${GlioTwin.fmt(p.radiotherapy_start_date||latestRt.start_date,'')}"></label>
      <label><span>Fractions</span>       <input type="number" class="patient-edit-input" name="fractions_count" value="${latestRt.fractions_count??''}"></label>
      <label class="patient-edit-notes"><span>Notes</span><textarea class="patient-edit-input" name="notes" rows="4">${GlioTwin.fmt(p.notes,'')}</textarea></label>
      <div class="patient-edit-actions">
        <button type="submit" class="btn btn-primary">Salva dati paziente</button>
      </div>
    </form>
  `;
  host.querySelector('#an-patient-form')?.addEventListener('submit', _anSubmitPatientForm);
}

async function _anSubmitPatientForm(event) {
  event.preventDefault();
  if (!_AN.selPid) return;
  const data = new FormData(event.currentTarget);
  const g = k => data.get(k)||null;
  const gn = k => data.get(k) ? Number(data.get(k)) : null;
  try {
    await GlioTwin.put(`/api/patients/${_AN.selPid}`, {
      patient_name:          g('patient_name'),
      patient_given_name:    g('patient_given_name'),
      patient_family_name:   g('patient_family_name'),
      patient_birth_date:    g('patient_birth_date'),
      sex:                   g('sex'),
      diagnosis:             g('diagnosis'),
      diagnosis_date:        g('diagnosis_date'),
      death_date:            g('death_date'),
      idh_status:            g('idh_status'),
      mgmt_status:           g('mgmt_status'),
      age_at_diagnosis:      gn('age_at_diagnosis'),
      os_days:               gn('os_days'),
      vital_status:          g('vital_status'),
      ida:                   g('ida'),
      tax_code:              g('tax_code'),
      radiotherapy_start_date: g('radiotherapy_start_date'),
      fractions_count:       gn('fractions_count'),
      notes:                 g('notes'),
    });
    GlioTwin.toast('Dati paziente salvati', 'success');
    _AN.patDetail = await GlioTwin.fetch(`/api/patients/${_AN.selPid}`);
    _anRenderMiniInfo(_AN.patDetail);
    const idx = (_AN.patients||[]).findIndex(p=>p.id===_AN.selPid);
    if (idx>=0) { _AN.patients[idx] = {..._AN.patients[idx], ..._AN.patDetail}; _anRenderPtList(); }
  } catch(e) { GlioTwin.toast(e.message, 'error'); }
}

// ═══════════════════════════════════════════════════════════════
// TAB: COHORT ANALYSIS
// ═══════════════════════════════════════════════════════════════
async function _anRenderOrLoadCohort() {
  const host = document.getElementById('an-panel-cohort');
  if (!host) return;
  if (_AN.cohortRows !== null) { _anRenderCohortPanel(); return; }
  host.innerHTML = `<div class="gm-loading"><div class="spinner"></div><span>Caricamento dati coorte…</span></div>`;
  try {
    const data = await GlioTwin.fetch('/api/global-metrics');
    _AN.cohortRows = data.rows || [];
    _anRenderCohortPanel();
  } catch(e) {
    host.innerHTML = `<div class="gm-loading"><span style="color:var(--red)">Errore: ${e.message}</span></div>`;
  }
}

function _anCohortFiltered() {
  const selectedPatients = new Set(_anFilteredPatients().map(p => p.id));
  return (_AN.cohortRows||[]).filter(r =>
    selectedPatients.has(r.patient_id) &&
    (_AN.labelFilter==='all' || r.label===_AN.labelFilter) &&
    r.structure_source===_AN.sourceFilter && r.signal_error===null
  );
}

function _anRenderCohortPanel() {
  const host = document.getElementById('an-panel-cohort');
  if (!host) return;
  _anDisposeCharts();

  const selectedPatients = _anFilteredPatients();
  const selectedPatientIds = new Set(selectedPatients.map(p => p.id));
  const allRows  = (_AN.cohortRows || []).filter(r => selectedPatientIds.has(r.patient_id));
  const baseRows = _anCohortFiltered();
  const aptRows  = baseRows.filter(r => r.sequence_type === 'APT');
  const volSeen  = new Set();
  const volRows  = baseRows.filter(r=>r.volume_ml!=null).filter(r=>{const k=`${r.session_id}|${r.label}`;if(volSeen.has(k))return false;volSeen.add(k);return true;});
  const nPat = selectedPatients.length;
  const nSes = new Set(allRows.map(r=>r.session_id)).size;

  host.innerHTML = `
    <div class="signal-panel-head" style="flex-wrap:wrap;gap:12px">
      <div>
        <div class="signal-panel-title">Cohort Analysis</div>
        <div class="signal-panel-status">
          <span class="signal-status-chip ok">${nPat} pazienti</span>
          <span class="signal-status-chip">${nSes} timepoint</span>
        </div>
      </div>
      <div class="signal-panel-controls" style="align-items:flex-start">
        <label><span>Struttura</span>
          <select id="an-c-label">
            <option value="all" ${_AN.labelFilter==='all'?'selected':''}>Tutte</option>
            <option value="enhancing_tumor"  ${_AN.labelFilter==='enhancing_tumor' ?'selected':''}>Enhancing Tumor</option>
            <option value="edema"            ${_AN.labelFilter==='edema'           ?'selected':''}>Edema</option>
            <option value="necrotic_core"    ${_AN.labelFilter==='necrotic_core'   ?'selected':''}>Necrotic Core</option>
            <option value="resection_cavity" ${_AN.labelFilter==='resection_cavity'?'selected':''}>Resection Cavity</option>
          </select>
        </label>
        <label><span>Sorgente</span>
          <select id="an-c-source">
            <option value="computed"    ${_AN.sourceFilter==='computed'   ?'selected':''}>Computed (FeTS)</option>
            <option value="radiological"${_AN.sourceFilter==='radiological'?'selected':''}>Radiological</option>
          </select>
        </label>
        <label><span>Colora per</span>
          <select id="an-c-color">
            <option value="patient"   ${_AN.colorBy==='patient'   ?'selected':''}>Paziente</option>
            <option value="label"     ${_AN.colorBy==='label'     ?'selected':''}>Struttura</option>
            <option value="timepoint" ${_AN.colorBy==='timepoint' ?'selected':''}>Timepoint</option>
          </select>
        </label>
      </div>
    </div>
    <div class="gm-charts-grid">
      <div class="gm-chart-card">
        <div class="gm-chart-title">Volume per struttura <span class="gm-chart-unit">(mL)</span></div>
        <div class="gm-chart-sub">Boxplot + punti individuali. Clic su un punto per aprire il paziente.</div>
        <div id="an-vol-strip" class="gm-chart-container"></div>
      </div>
      <div class="gm-chart-card">
        <div class="gm-chart-title">Segnale APT per struttura <span class="gm-chart-unit">(%MTR<sub>asym</sub>)</span></div>
        <div class="gm-chart-sub">Solo timepoint con APT disponibile.</div>
        <div id="an-apt-strip" class="gm-chart-container"></div>
        <div id="an-stats-panel" class="gm-stats-panel"></div>
      </div>
    </div>
  `;

  const onClickRow = r => {
    GlioTwin.state.currentPatient = r.patient_id;
    GlioTwin.state.currentSession = r.session_id;
    location.hash = '#/browser';
  };
  _anMakeStripChart('an-vol-strip', volRows, 'volume_ml', 'Volume (mL)', onClickRow);
  _anMakeStripChart('an-apt-strip', aptRows, 'median',    '% MTRasym',   onClickRow);
  _anRenderStatsPanel(aptRows);

  host.querySelector('#an-c-label' )?.addEventListener('change', e=>{ _AN.labelFilter =e.target.value; _anRenderCohortPanel(); });
  host.querySelector('#an-c-source')?.addEventListener('change', e=>{ _AN.sourceFilter=e.target.value; _anRenderCohortPanel(); });
  host.querySelector('#an-c-color' )?.addEventListener('change', e=>{ _AN.colorBy     =e.target.value; _anRenderCohortPanel(); });
}

function _anMakeStripChart(cid, rows, vk, xLabel, onClickRow) {
  const el = document.getElementById(cid);
  if (!el) return;
  const colorBy  = _AN.colorBy;
  const present  = new Set(rows.map(r=>r.label));
  const labels   = _AN_LABEL_ORDER.filter(l=>present.has(l));
  const catIndex = Object.fromEntries(labels.map((l,i)=>[l,i]));
  const N        = labels.length;
  const isApt    = vk==='median';

  const scatterSeries = labels.map(label=>({
    name: _AN_LABEL_NAMES[label]||label, type:'scatter', symbolSize:8,
    data: rows.filter(r=>r.label===label&&r[vk]!=null).map(r=>({
      value:[r[vk], catIndex[label]+_anJitter()],
      itemStyle:{color:_anPointColor(r,colorBy), opacity:0.88}, _row:r,
    })),
    emphasis:{scale:1.7}, z:10,
  }));

  const boxCustomData = labels.map((label,i)=>{
    const vals=rows.filter(r=>r.label===label&&r[vk]!=null).map(r=>r[vk]);
    const stats=_anBoxStats(vals); if(!stats)return null;
    const [wLo,q1,med,q3,wHi]=stats;
    return {i,q1,med,q3,wLo,wHi,col:_AN_LABEL_COLORS[label]||'#8395b0',label,n:vals.length};
  }).filter(Boolean);

  const boxCustomSeries={
    type:'custom', name:'_box', silent:true, z:2,
    renderItem(params,api){
      const d=boxCustomData[params.dataIndex]; if(!d)return{type:'group',children:[]};
      const [xGL,yC]=api.coord([0,d.i]);
      const xQ1=api.coord([d.q1,d.i])[0], xMed=api.coord([d.med,d.i])[0];
      const xQ3=api.coord([d.q3,d.i])[0], xWL=api.coord([d.wLo,d.i])[0], xWH=api.coord([d.wHi,d.i])[0];
      const h=13, ls={stroke:d.col,lineWidth:1.5,opacity:0.6};
      return{type:'group',children:[
        {type:'rect',shape:{x:xGL,y:yC-h-2,width:3,height:(h+2)*2},style:{fill:d.col,opacity:0.85},z2:3},
        {type:'text',style:{x:xGL-8,y:yC,text:_AN_LABEL_NAMES[d.label]||d.label,fill:d.col,opacity:0.95,fontSize:11,fontWeight:700,textAlign:'right',textVerticalAlign:'middle'},z2:20},
        {type:'line',shape:{x1:xWL,y1:yC,x2:xWH,y2:yC},style:ls},
        {type:'line',shape:{x1:xWL,y1:yC-h*.5,x2:xWL,y2:yC+h*.5},style:ls},
        {type:'line',shape:{x1:xWH,y1:yC-h*.5,x2:xWH,y2:yC+h*.5},style:ls},
        {type:'rect',shape:{x:xQ1,y:yC-h,width:xQ3-xQ1,height:h*2},style:{fill:d.col,opacity:0.22}},
        {type:'rect',shape:{x:xQ1,y:yC-h,width:xQ3-xQ1,height:h*2},style:{fill:'none',stroke:d.col,lineWidth:2,opacity:0.75}},
        {type:'line',shape:{x1:xMed,y1:yC-h,x2:xMed,y2:yC+h},style:{stroke:'#ffffff',lineWidth:2.5,opacity:0.95}},
        {type:'text',style:{x:xMed+5,y:yC-h-2,text:d.med.toFixed(isApt?2:1),fill:'#ffffff',opacity:0.75,fontSize:10,fontWeight:600,textAlign:'left',textVerticalAlign:'bottom'},z2:25},
        {type:'text',style:{x:xWH+4,y:yC,text:`n=${d.n}`,fill:'#8395b0',opacity:0.7,fontSize:9,textAlign:'left',textVerticalAlign:'middle'},z2:20},
      ]};
    },
    data:boxCustomData.map((_,i)=>[i]), encode:{x:0},
  };

  const chart=echarts.init(el,'dark');
  _anCharts.push(chart);
  chart.setOption({
    backgroundColor:'transparent',
    grid:{left:140,right:40,top:12,bottom:44},
    xAxis:{type:'value',name:xLabel,nameLocation:'middle',nameGap:28,nameTextStyle:{color:'#8395b0',fontSize:11},axisLabel:{color:'#8395b0',fontSize:11},splitLine:{lineStyle:{color:'#1e2a40'}},axisLine:{lineStyle:{color:'#2d4a7a'}},min:0},
    yAxis:{type:'value',min:-0.5,max:N-0.5,interval:1,axisLabel:{show:false},axisTick:{show:false},axisLine:{lineStyle:{color:'#2d4a7a'}},splitLine:{show:true,lineStyle:{color:'#172033',type:'dashed'}}},
    tooltip:{trigger:'item',backgroundColor:'#0f1623',borderColor:'#2d4a7a',textStyle:{color:'#dde4f0',fontSize:12},
      formatter:params=>{const r=params.data?._row;if(!r)return '';const val=r[vk];return[`<div style="font-weight:700;margin-bottom:3px">${r.subject_id}</div>`,`<div style="color:#8395b0;font-size:11px">${r.session_label}${r.study_date?' · '+r.study_date:''}</div>`,`<div style="margin-top:5px"><span style="color:${_AN_LABEL_COLORS[r.label]||'#8395b0'}">■</span> ${_AN_LABEL_NAMES[r.label]||r.label}</div>`,`<div>${xLabel}: <b>${val!=null?val.toFixed(isApt?3:1):'—'}</b></div>`,`<div style="color:#3b7ef8;margin-top:3px;font-size:11px">Clic per aprire →</div>`].join('');}},
    series:[boxCustomSeries,...scatterSeries],
  });
  chart.on('click',params=>{const r=params.data?._row;if(r)onClickRow(r);});
}

function _anRenderStatsPanel(aptRows) {
  const el = document.getElementById('an-stats-panel');
  if (!el) return;
  const present = _AN_LABEL_ORDER.filter(l=>aptRows.some(r=>r.label===l&&r.median!=null));
  if (present.length < 2) { el.innerHTML='<div class="gm-stats-note">Dati APT insufficienti.</div>'; return; }
  const groups = Object.fromEntries(present.map(l=>[l,aptRows.filter(r=>r.label===l&&r.median!=null).map(r=>r.median)]));
  const kw = _anKruskalWallis(groups, present);
  const pairs=[];
  for(let i=0;i<present.length;i++) for(let j=i+1;j<present.length;j++){const res=_anMannWhitney(groups[present[i]],groups[present[j]]);if(res)pairs.push({la:present[i],lb:present[j],...res});}
  const m=pairs.length||1;
  const corrected=pairs.map(p=>({...p,p_corr:Math.min(p.p*m,1)}));
  const pMatrix={};
  corrected.forEach(({la,lb,p_corr,r})=>{pMatrix[`${la}|${lb}`]={p:p_corr,r};pMatrix[`${lb}|${la}`]={p:p_corr,r:-r};});
  const pCell=(la,lb)=>{if(la===lb)return'<td class="gm-sc-diag">—</td>';const e=pMatrix[`${la}|${lb}`];if(!e)return'<td class="gm-sc-na">—</td>';const{p,r}=e;const cls=p<0.001?'gm-sc-sig3':p<0.01?'gm-sc-sig2':p<0.05?'gm-sc-sig1':'gm-sc-ns';const pStr=p<0.001?'<0.001':p<0.01?p.toFixed(3):p.toFixed(2);const rStr=(r>=0?'+':'')+r.toFixed(2);const rColor=Math.abs(r)>0.5?'#22d3ee':Math.abs(r)>0.3?'#a78bfa':'#8395b0';return`<td class="gm-sc-cell ${cls}"><div class="gm-sc-p">${pStr}</div><div class="gm-sc-r" style="color:${rColor}">${rStr}</div></td>`;};
  const kwLine=kw?`<div class="gm-stats-kw">Kruskal-Wallis: H=${kw.H.toFixed(2)}, df=${kw.df}, <span class="${kw.p<0.05?'gm-stats-sig':'gm-stats-ns'}">p ${kw.p<0.001?'< 0.001':'= '+kw.p.toFixed(3)}</span> &nbsp;(N=${kw.N})</div>`:'';
  el.innerHTML=`<div class="gm-stats-title">Differenziazione APT tra strutture</div>${kwLine}<div class="gm-stats-subtitle">Mann-Whitney U, Bonferroni (×${m}) — p corretto / r ranghi</div><div class="gm-stats-wrap"><table class="gm-stats-table"><thead><tr><th></th>${present.map(l=>`<th style="color:${_AN_LABEL_COLORS[l]||'#8395b0'}">${_AN_LABEL_SHORT[l]||l}</th>`).join('')}</tr></thead><tbody>${present.map(la=>`<tr><th style="color:${_AN_LABEL_COLORS[la]||'#8395b0'};white-space:nowrap">${_AN_LABEL_SHORT[la]}</th>${present.map(lb=>pCell(la,lb)).join('')}</tr>`).join('')}</tbody></table><div class="gm-stats-legend"><span class="gm-sc-cell gm-sc-sig3">p&lt;0.001</span><span class="gm-sc-cell gm-sc-sig2">p&lt;0.01</span><span class="gm-sc-cell gm-sc-sig1">p&lt;0.05</span><span class="gm-sc-cell gm-sc-ns">n.s.</span></div></div>`;
}

// ═══════════════════════════════════════════════════════════════
// TAB: STATO DATI
// ═══════════════════════════════════════════════════════════════
async function _anLoadStatus() {
  const host = document.getElementById('an-panel-status');
  if (!host) return;
  host.innerHTML=`<div class="gm-loading"><div class="spinner"></div><span>Caricamento stato dati…</span></div>`;
  try {
    const [patientsResp, sessionsResp, ms] = await Promise.all([
      GlioTwin.fetch('/api/patients'),
      GlioTwin.fetch('/api/segmentation/sessions'),
      GlioTwin.fetch(`/api/signal-metrics/status?_ts=${Date.now()}`).catch(()=>null),
    ]);
    const patients = Array.isArray(patientsResp) ? patientsResp : (patientsResp.patients||[]);
    const sessions = sessionsResp.sessions || [];
    _AN.statusData = {
      nPat:    patients.length,
      nSes:    sessions.length,
      nPrep:   sessions.filter(s=>s.preprocessing_ready).length,
      nSeg:    sessions.filter(s=>(s.segmented_models||[]).length>0).length,
      ms,
    };
    _AN.metricStatus = ms;
  } catch(e) {
    if(host) host.innerHTML=`<div class="gm-loading"><span style="color:var(--red)">Errore: ${e.message}</span></div>`;
    return;
  }
  _anRenderStatusPanel();
}

function _anRenderStatusPanel() {
  const host = document.getElementById('an-panel-status');
  if (!host || !_AN.statusData) return;
  const d   = _AN.statusData;
  const job = d.ms?.latest_job;
  const run = ['queued','running'].includes(job?.status);
  const prg = run && job?.total_tasks>0 ? ` (${job.completed_tasks??0}/${job.total_tasks})` : '';
  const pct = (n,t) => t ? `${n} / ${t} (${Math.round(100*n/t)}%)` : `${n} / ${t}`;

  const cards = [
    {title:'Pazienti',       value:d.nPat,                         sub:'importati nel database',              color:'var(--blue)'},
    {title:'Sessioni',       value:d.nSes,                         sub:'timepoint totali',                    color:'var(--text-muted)'},
    {title:'Preprocessing',  value:pct(d.nPrep, d.nSes),           sub:'sessioni con NIfTI pronti',            color:'var(--green)'},
    {title:'Segmentazione',  value:pct(d.nSeg,  d.nSes),           sub:'sessioni con almeno un modello',      color:'#a78bfa'},
    {title:'Cache metriche', value:d.ms?.cached_rows ?? '—',        sub:'righe signal_metric_cache',           color:'#22d3ee'},
  ];

  host.innerHTML=`
    <div class="signal-panel-head">
      <div>
        <div class="signal-panel-title">Stato Dati &amp; Metriche</div>
        <div class="signal-panel-sub">Riepilogo della completezza del database per fase.</div>
      </div>
      <div class="signal-panel-actions">
        <button class="btn signal-calc-btn ${run?'signal-calc-btn--running':''}" id="an-st-calc" ${run?'disabled':''}>
          ${run?`<span class="signal-calc-spinner"></span> Calcolo${prg}…`:'Calcola metriche mancanti'}
        </button>
        <button class="btn btn-secondary" id="an-st-force" ${run?'disabled':''}>Ricalcola tutto</button>
        <button class="btn btn-secondary" id="an-st-refresh">Aggiorna</button>
      </div>
    </div>
    ${job?`<div class="an-job-status ${run?'an-job-running':''}">
      ${run?'<div class="rh-status-spinner"></div>':''}
      <div><strong>Job #${job.id}</strong> — ${job.status}
        ${job.completed_tasks!=null?` · ${job.completed_tasks} calcolati`:''}
        ${job.failed_tasks?` · ${job.failed_tasks} vuoti`:''}
        ${job.error_message?` · ✗ ${job.error_message}`:''}
      </div>
    </div>`:''}
    <div class="an-status-grid">
      ${cards.map(c=>`
        <div class="an-status-card">
          <div class="an-status-title">${c.title}</div>
          <div class="an-status-value" style="color:${c.color}">${c.value}</div>
          <div class="an-status-sub">${c.sub}</div>
        </div>`).join('')}
    </div>`;

  host.querySelector('#an-st-calc')?.addEventListener('click', async()=>{await _anQueueMetrics(false); await _anLoadStatus();});
  host.querySelector('#an-st-force')?.addEventListener('click', async()=>{if(!confirm('Ricalcolare tutto?'))return; await _anQueueMetrics(true); await _anLoadStatus();});
  host.querySelector('#an-st-refresh')?.addEventListener('click', ()=>{_AN.statusData=null; _anLoadStatus();});
}

// ═══════════════════════════════════════════════════════════════
// ENTRY POINT
// ═══════════════════════════════════════════════════════════════
GlioTwin.register('analysis', async (app) => {
  clearTimeout(_anPollTimer);
  _anDisposeCharts();

  // Il tab di default è 'patient' se non è già impostato
  if (!['patient','longitudinal','cohort','status'].includes(_AN.activeTab))
    _AN.activeTab = 'patient';

  _anRenderShell(app);

  // Carica pazienti (con cache)
  if (!_AN.patients) {
    try {
      const d = await GlioTwin.fetch('/api/patients');
      _AN.patients = Array.isArray(d) ? d : (d.patients||[]);
    } catch(e) {
      const el = document.getElementById('an-patient-list');
      if (el) el.innerHTML=`<div class="tree-hint" style="color:var(--red)">${e.message}</div>`;
    }
  }
  _anRenderPtList();

  // Ripristina paziente selezionato globalmente (es. da Viewer)
  if (!_AN.selPid && GlioTwin.state.currentPatient) {
    _AN.selPid = GlioTwin.state.currentPatient;
  }

  // Se c'è un paziente selezionato e non abbiamo ancora il dettaglio, caricalo
  if (_AN.selPid && !_AN.patDetail) {
    try { _AN.patDetail = await GlioTwin.fetch(`/api/patients/${_AN.selPid}`); } catch(_) {}
  }

  // Bind filtri
  const _anOnFilterChange = () => {
    _anRenderPtList();
    if (_AN.activeTab === 'cohort' && _AN.cohortRows !== null) _anRenderCohortPanel();
  };
  document.getElementById('an-f-q')?.addEventListener('input', _anOnFilterChange);
  ['an-f-ds','an-f-vital','an-f-idh','an-f-mgmt'].forEach(id =>
    document.getElementById(id)?.addEventListener('change', _anOnFilterChange)
  );

  // Bind tab buttons
  ['patient','longitudinal','cohort','status'].forEach(t =>
    document.getElementById(`an-tab-${t}`)?.addEventListener('click', () => _anSwitchTab(t))
  );

  await _anRenderActiveTab();
});
