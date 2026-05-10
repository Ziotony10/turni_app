
const MESI_IT    = ['Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'];
const GIORNI_SHORT = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom'];

let anno = new Date().getFullYear();
let mese = new Date().getMonth() + 1;
let isEditor = false;
let operatori = [];
let datiMese = null;
let setupOperatorCount = 0;

// Stato picker
let pickerTarget = null;   // { data, opId, col }
let pickerFlags  = { smart:false, rep:false, F:false, M:false };
let pickerTurnoSelected = undefined;
let pickerMode = 'table';          // 'table' | 'template'
let pickerTemplateTarget = null;   // { dow, pos }
let tplData = {};                  // tplData[dow][pos] = { turno_base, flags }
let tplRepData = {};               // tplRepData[dow] = { rep1_pos, rep2_pos, rep3_pos, fest_m1_pos, fest_m2_pos, fest_p1_pos, fest_p2_pos }
let tplSlotCount = 13;
let teamContext = { linked_operatore_id:null, linked_operatore_nome:'', can_request_ferie:false, ferie_pending_count:0 };
let ferieMode = false;
let ferieAdds = new Set();
let ferieRemovals = new Set();
let teamFerieLogData = { pending: [], logs: [] };
let teamFerieFilters = { name: '', status: 'all' };
let teamTableResizeBound = false;
let teamTableResizeObserver = null;

// Stato Swap
let swapMode = false;
let swapPhase = 1; // 1: seleziona tuo turno, 2: seleziona turno collega
let swapSource = null; // { data, opId, col, turno }
let swapTarget = null; // { data, opId, col, turno }
let pendingBadgeCounts = { ferie: 0, scambi: 0 };

// Stato Notifiche
let notifications = [];

function getOperatorSlotCount(extraPositions = []) {
  const opMax = operatori.reduce((max, op) => Math.max(max, Number(op.posizione) || 0), 0);
  const extraMax = extraPositions.reduce((max, pos) => Math.max(max, Number(pos) || 0), 0);
  return Math.max(opMax, extraMax, 1);
}

function updateSetupHelpText() {
  const el = document.getElementById('setupHelpText');
  if (el) el.textContent = `Inserisci i nomi degli operatori nell'ordine delle colonne della tabella (${setupOperatorCount} posizioni).`;
}

function buildTeamTableColgroup(operatorCount = 0) {
  let html = '<colgroup><col class="col-date"><col class="col-day">';
  for (let i = 0; i < operatorCount; i += 1) html += '<col class="col-slot"><col class="col-slot">';
  html += '<col class="col-sep">';
  for (let i = 0; i < 3; i += 1) html += '<col class="col-side">';
  html += '<col class="col-sep">';
  for (let i = 0; i < 4; i += 1) html += '<col class="col-side">';
  html += '</colgroup>';
  return html;
}

function applyResponsiveTeamLayout() {
  const wrap = document.getElementById('tableWrap');
  const table = wrap?.querySelector('.team-table');
  const operatorCount = datiMese?.operatori?.length || operatori.length || 0;
  if (!wrap || !table || !operatorCount) return;

  const containerWidth = Math.max(Math.floor(wrap.clientWidth || 0), 320);
  const slotCount = operatorCount * 2;
  const fluidBreakpoint = 1800;
  const layoutWidth = Math.max(containerWidth, fluidBreakpoint);
  const isFluidMode = containerWidth >= fluidBreakpoint;
  const dateW = 50;
  const dayW = 32;
  const sepW = 2;
  const sideW = layoutWidth >= 2200 ? 28 : layoutWidth >= 2000 ? 26 : 24;
  const fixedWidth = dateW + dayW + (sepW * 2) + (sideW * 7);
  const slotW = Math.max(28, Math.floor((layoutWidth - fixedWidth) / Math.max(slotCount, 1)));

  let slotFont = 0.72;
  let slotVarFont = 0.80;
  let opNameFont = 0.62;
  if (slotW <= 38) {
    slotFont = 0.68;
    slotVarFont = 0.75;
    opNameFont = 0.58;
  }
  if (slotW <= 34) {
    slotFont = 0.62;
    slotVarFont = 0.69;
    opNameFont = 0.54;
  }
  if (slotW <= 30) {
    slotFont = 0.58;
    slotVarFont = 0.64;
    opNameFont = 0.50;
  }

  wrap.style.setProperty('--date-w', `${dateW}px`);
  wrap.style.setProperty('--day-w', `${dayW}px`);
  wrap.style.setProperty('--sep-w', `${sepW}px`);
  wrap.style.setProperty('--side-w', `${sideW}px`);
  wrap.style.setProperty('--slot-w', `${slotW}px`);
  wrap.style.setProperty('--slot-font', `${slotFont}rem`);
  wrap.style.setProperty('--slot-var-font', `${slotVarFont}rem`);
  wrap.style.setProperty('--op-name-font', `${opNameFont}rem`);
  wrap.style.setProperty('--team-table-min-width', `${Math.max(fluidBreakpoint, fixedWidth + (slotCount * slotW))}px`);
  wrap.classList.toggle('is-scroll-mode', !isFluidMode);
}

// ── AUTH ──────────────────────────────────────────────────────────────────────
function getToken() { return localStorage.getItem('token'); }
function logout()   {
  localStorage.removeItem('token');
  localStorage.removeItem('username');
  localStorage.removeItem('nome');
  localStorage.removeItem('is_admin');
  window.location.href='/login.html';
}
async function fetchCurrentUserOrNull() {
  const res = await fetch('/api/auth/me');
  if (res.status === 401) { logout(); return null; }
  if (!res.ok) throw new Error(`Auth check failed: ${res.status}`);
  return res.json();
}

const _origFetch = window.fetch.bind(window);
window.fetch = function(url, opts={}) {
  const token = getToken();
  if (token && typeof url==='string' && url.startsWith('/api/')) {
    opts.headers = { ...(opts.headers||{}), 'Authorization': 'Bearer '+token };
  }
  return _origFetch(url, opts).then(res => {
    if (res.status===401 && typeof url==='string' && url.startsWith('/api/')) { logout(); }
    return res;
  });
};

// ── USER MENU ─────────────────────────────────────────────────────────────────
function toggleUserMenu(e) { e.stopPropagation(); document.getElementById('userDropdown').classList.toggle('open'); }
document.addEventListener('click', () => document.getElementById('userDropdown')?.classList.remove('open'));
document.addEventListener('click', e => {
  const p = document.getElementById('turnoPicker');
  if (p && p.style.display !== 'none' && !p.contains(e.target)) closePicker();
  const d = document.getElementById('notifDropdown');
  if (d && d.classList.contains('open') && !document.getElementById('notifBellBtn').contains(e.target) && !d.contains(e.target)) d.classList.remove('open');
});

// ── INIT ──────────────────────────────────────────────────────────────────────
(async function() {
  if (!getToken()) { window.location.href='/login.html'; return; }
  try {
    const me = await fetchCurrentUserOrNull();
    if (!me) return;
    if (!me.id) throw new Error('Auth payload non valido');
    const nome = me.nome || me.username || '';
    const initiali = nome.split(' ').map(p=>p[0]||'').join('').slice(0,2).toUpperCase();
    document.getElementById('userNameLabel').textContent = nome.split(' ')[0];
    document.getElementById('userAvatar').textContent   = initiali || '?';
    document.getElementById('udName').textContent       = nome;
    document.getElementById('udUsername').textContent   = me.username;

    const teamMe = await fetch('/api/team/me').then(r=>r.json());
    isEditor = teamMe.is_editor;
    teamContext = { ...(teamContext || {}), ...(teamMe || {}) };
    if (isEditor) document.getElementById('editorBar').style.display = 'flex';
    if (me.is_admin) {
      const lnk = document.getElementById('adminNavLink');
      if (lnk) lnk.style.setProperty('display','inline-block','important');
      const mob = document.getElementById('adminMobileBtn');
      if (mob) mob.style.display = 'flex';
    }

    const sel = document.getElementById('selAnno');
    for (let y=2024; y<=2030; y++) {
      const o = document.createElement('option');
      o.value=y; o.textContent=y; if(y===anno) o.selected=true;
      sel.appendChild(o);
    }
    buildMonthTabs();
    await loadOperatori();
    updateFerieUi();
    load();
  } catch(e) { console.error(e); }
})();

function changeAnno(d) {
  anno += d;
  document.getElementById('selAnno').value = anno;
  buildMonthTabs(); load();
}

function buildMonthTabs() {
  const c = document.getElementById('monthTabs');
  c.innerHTML = '';
  MESI_IT.forEach((m, i) => {
    const d = document.createElement('div');
    d.className = 'month-tab' + (i+1===mese ? ' active' : '');
    d.textContent = m;
    d.onclick = () => { mese=i+1; document.querySelectorAll('.month-tab').forEach((t,j)=>t.classList.toggle('active',j===i)); load(); };
    c.appendChild(d);
  });
}

function getTeamPrintStyle(pageWidthMm, pageHeightMm, pageMarginMm) {
  const appStyle = Array.from(document.querySelectorAll('style')).map(s => s.textContent || '').join('\n');
  return `
    ${appStyle}
    * {
      -webkit-print-color-adjust: exact !important;
      print-color-adjust: exact !important;
      forced-color-adjust: none !important;
    }
    html, body {
      background:#ffffff !important;
      color:#0f172a !important;
      margin:0;
      padding:0;
      font-family:'Segoe UI', system-ui, sans-serif;
      width:${pageWidthMm}mm;
      height:${pageHeightMm}mm;
    }
    body { padding:${pageMarginMm}mm; overflow:visible; }
    .print-page {
      width:calc(${pageWidthMm}mm - ${pageMarginMm * 2}mm);
      min-height:calc(${pageHeightMm}mm - ${pageMarginMm * 2}mm);
      overflow:visible;
    }
    .print-shell {
      display:block;
      width:max-content;
    }
    .print-head { margin-bottom:14px; }
    .print-title {
      font-size:1.15rem;
      font-weight:800;
      color:#0f172a;
      margin-bottom:4px;
    }
    .print-meta {
      font-size:0.78rem;
      color:#475569;
    }
    .table-wrap {
      overflow:visible !important;
      background:#ffffff !important;
      height:auto !important;
      max-height:none !important;
    }
    .team-table {
      font-size:0.78rem !important;
      border-collapse:collapse !important;
      width:auto !important;
    }
    .team-table th,
    .team-table td {
      position:static !important;
      filter:none !important;
      box-shadow:none !important;
      -webkit-print-color-adjust: exact !important;
      print-color-adjust: exact !important;
    }
    .team-table th {
      top:auto !important;
      left:auto !important;
    }
    .team-table td.td-data,
    .team-table td.td-giorno,
    .team-table th.sticky-left,
    .team-table th.col-giorno {
      left:auto !important;
    }
    .team-table tr:hover td { filter:none !important; }
    .cella,
    .tpl-cella {
      min-width:56px !important;
      min-height:22px !important;
      font-size:0.76rem !important;
      -webkit-print-color-adjust: exact !important;
      print-color-adjust: exact !important;
    }
    .cella.var-cell.filled {
      font-size:0.82rem !important;
      color:#FF0000 !important;
    }
    .cella.var-cell.filled .mf-inline,
    .cella.var-cell.filled .cella-rep-icon,
    .cella.var-cell.filled .smart-inline {
      color:#FF0000 !important;
    }
    .th-op-nome {
      max-width:none !important;
      overflow:visible !important;
      text-overflow:clip !important;
    }
    .print-legend {
      margin-top:18px;
      font-size:0.78rem;
      color:#475569;
    }
    @page {
      size: ${pageWidthMm}mm ${pageHeightMm}mm;
      margin: ${pageMarginMm}mm;
    }
    @media print {
      html, body {
        width:${pageWidthMm}mm;
        height:${pageHeightMm}mm;
        overflow:visible;
      }
      body { padding:0; }
      .print-page { width:auto; min-height:auto; }
    }
  `;
}

function buildTeamPrintHtml() {
  const wrap = document.getElementById('tableWrap');
  const table = wrap?.querySelector('.team-table');
  if (!wrap || !table) {
    throw new Error('Tabella team non disponibile');
  }
  const pxToMm = 25.4 / 96;
  const contentWidthPx = Math.ceil(table.scrollWidth);
  const contentHeightPx = Math.ceil(wrap.scrollHeight);
  const pageMarginMm = 8;
  const pageWidthMm = Math.max(297, Math.ceil(contentWidthPx * pxToMm) + (pageMarginMm * 2));
  const pageHeightMm = Math.max(210, Math.ceil(contentHeightPx * pxToMm) + (pageMarginMm * 2));
  const now = new Date();
  const generated = now.toLocaleDateString('it-IT') + ' ' + now.toLocaleTimeString('it-IT', { hour:'2-digit', minute:'2-digit' });
  const title = `Turni team - ${MESI_IT[mese - 1]} ${anno}`;
  const meta = `Generato ${generated} | Operatori ${operatori.length}`;
  return `<!DOCTYPE html>
  <html lang="it">
  <head>
    <meta charset="UTF-8">
    <title>${title}</title>
    <style>${getTeamPrintStyle(pageWidthMm, pageHeightMm, pageMarginMm)}</style>
  </head>
  <body>
    <div class="print-page">
      <div class="print-shell" id="printShell">
        <div class="print-head">
          <div class="print-title">${title}</div>
          <div class="print-meta">${meta}</div>
        </div>
        <div class="table-wrap">${wrap.innerHTML}</div>
        <div class="print-legend">R=rep&nbsp;&nbsp; #=smart&nbsp;&nbsp; m/f=mod</div>
      </div>
    </div>
    <script>
      window.addEventListener('load', () => {
        setTimeout(() => {
          window.print();
        }, 300);
      });
    <\/script>
  </body>
  </html>`;
}

async function openTeamPrintView() {
  if (!datiMese?.giorni?.length) {
    await load();
  }
  let frame = document.getElementById('teamPrintFrame');
  if (frame) frame.remove();
  frame = document.createElement('iframe');
  frame.id = 'teamPrintFrame';
  frame.style.position = 'fixed';
  frame.style.left = '-20000px';
  frame.style.top = '0';
  frame.style.width = `${Math.max(window.innerWidth, 2200)}px`;
  frame.style.height = `${Math.max(window.innerHeight, 1400)}px`;
  frame.style.opacity = '0';
  frame.style.pointerEvents = 'none';
  frame.style.border = '0';
  document.body.appendChild(frame);

  const html = buildTeamPrintHtml();
  await new Promise((resolve, reject) => {
    const cleanup = () => {
      frame.onload = null;
    };
    frame.onload = () => {
      cleanup();
      resolve();
    };
    try {
      const doc = frame.contentWindow?.document;
      if (!doc) throw new Error('Frame di stampa non disponibile');
      doc.open();
      doc.write(html);
      doc.close();
    } catch (err) {
      cleanup();
      reject(err);
    }
  });

  const printWindow = frame.contentWindow;
  if (!printWindow) throw new Error('Finestra di stampa non disponibile');
  printWindow.focus();
  printWindow.print();

  window.setTimeout(() => {
    const existing = document.getElementById('teamPrintFrame');
    if (existing) existing.remove();
  }, 60000);
}

async function loadOperatori() {
  operatori = await fetch('/api/team/operatori').then(r=>r.json()).catch(()=>[]);
}

// ── RENDER TABELLA ────────────────────────────────────────────────────────────
async function load() {
  const wrap = document.getElementById('tableWrap');
  wrap.innerHTML = '<div style="text-align:center;padding:60px;color:var(--muted)">Caricamento…</div>';

  if (operatori.length === 0) {
    wrap.innerHTML = `<div style="text-align:center;padding:60px;color:var(--muted)">
      <p style="font-size:1rem;margin-bottom:12px">👥 Nessun operatore configurato</p>
      ${isEditor ? '<button class="btn-editor primary" onclick="openSetupOperatori()" style="margin:0 auto">Imposta operatori</button>' : '<p>Contatta l\'editor per configurare la tabella.</p>'}
    </div>`;
    return;
  }

  const response = await fetch(`/api/team/turni/${anno}/${mese}`).catch(()=>({ok:false,status:0}));
  if (!response || response.ok===false && response.status===0) {
    wrap.innerHTML = `<div style="text-align:center;padding:60px;color:#fca5a5">Errore caricamento: impossibile contattare il server.</div>`;
    return;
  }
  if (!response.ok) {
    if (response.status===401) { logout(); return; }
    const text = await response.text();
    wrap.innerHTML = `<div style="text-align:center;padding:60px;color:#fca5a5">Errore ${response.status}: ${text}</div>`;
    return;
  }

  const data = await response.json();
  datiMese = data;
  const ops = data.operatori;
  if (!ops || ops.length===0) {
    wrap.innerHTML = '<div style="text-align:center;padding:60px;color:var(--muted)">Nessun operatore configurato</div>';
    return;
  }

  let html = '<table class="team-table"><thead><tr>';
  html += '<th class="col-data sticky-left">Data</th>';
  html += '<th class="col-giorno">Gg</th>';
  ops.forEach(op => {
    html += `<th colspan="2" class="col-op" title="${op.nome}">
      <div style="font-size:0.62rem;color:#6b7280;font-weight:800;background:#e5e7eb;border-radius:3px;padding:0 4px;display:inline-block;margin-bottom:1px">${op.posizione}</div>
      <div class="th-op-nome">${op.nome}</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;margin-top:2px;font-size:0.55rem;color:#9ca3af">
        <span>TAB</span><span>VAR</span>
      </div>
    </th>`;
  });
  html += '<th class="td-sep-destra" style="width:2px;padding:0"></th>';
  html += '<th class="col-destra" style="font-size:0.6rem">Rep<br>1°</th>';
  html += '<th class="col-destra" style="font-size:0.6rem">Rep<br>2°</th>';
  html += '<th class="col-destra" style="font-size:0.6rem">Rep<br>3°</th>';
  html += '<th class="td-sep-destra td-sep-fest" style="width:2px;padding:0"></th>';
  html += '<th class="col-destra" style="font-size:0.6rem;text-align:center">FEST<br><span style="font-size:0.62rem;font-weight:900">M</span></th>';
  html += '<th class="col-destra" style="font-size:0.6rem;text-align:center">FEST<br><span style="font-size:0.62rem;font-weight:900">M</span></th>';
  html += '<th class="col-destra" style="font-size:0.6rem;text-align:center">FEST<br><span style="font-size:0.62rem;font-weight:900">P</span></th>';
  html += '<th class="col-destra" style="font-size:0.6rem;text-align:center">FEST<br><span style="font-size:0.62rem;font-weight:900">P</span></th>';
  html += '</tr></thead><tbody>';

  data.giorni.forEach((g, idx) => {
    const d = new Date(g.data + 'T12:00:00');
    const dayName = GIORNI_SHORT[g.dow];
    const dayNum  = d.getDate();
    const monthShort = MESI_IT[mese-1].slice(0,3);

    let rowClass = 'week-block';
    if (g.is_domenica) rowClass += ' domenica';
    else if (g.is_sabato) rowClass += ' sabato';
    if (g.is_festivo) rowClass += ' festivo';
    if (g.dow === 0 || idx === 0) rowClass += ' week-start';
    if (g.dow === 6 || idx === data.giorni.length - 1) rowClass += ' week-end';

    html += `<tr class="${rowClass}" data-data="${g.data}">`;
    html += `<td class="td-data">${dayNum} ${monthShort}</td>`;
    html += `<td class="td-giorno">${dayName}</td>`;

    g.turni.forEach(t => {
      html += `<td class="td-turno">${renderCella(t.turno_base, t.flags_base, g.data, t.operatore_id, 'base', t.turno_var)}</td>`;
      html += `<td class="td-turno td-turno-var">${renderCella(t.turno_var, t.flags_var, g.data, t.operatore_id, 'var')}</td>`;
    });

    const cd = g.colonne_destra;
    html += '<td class="td-sep-destra td-sep-fest"></td>';
    // rep1, rep2 e rep3 identificano la gerarchia di reperibilità:
    // rep1: primo reperibile da chiamare
    // rep2: secondo reperibile
    // rep3: terzo reperibile
    ['rep1','rep2','rep3'].forEach(k => {
      html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'—'}</td>`;
    });
    html += '<td class="td-sep-destra"></td>';
    ['fest_m1','fest_m2','fest_p1','fest_p2'].forEach(k => {
      html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'—'}</td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  wrap.innerHTML = html;
  applySpecialTurnoStyles(wrap);
}

function buildTeamRowHtml(g, idx, totalRows = (datiMese?.giorni?.length || 0)) {
  const d = new Date(g.data + 'T12:00:00');
  const dayName = GIORNI_SHORT[g.dow];
  const dayNum  = d.getDate();
  const monthShort = MESI_IT[mese-1].slice(0,3);

  let rowClass = 'week-block';
  if (g.is_domenica) rowClass += ' domenica';
  else if (g.is_sabato) rowClass += ' sabato';
  if (g.is_festivo) rowClass += ' festivo';
  if (g.dow === 0 || idx === 0) rowClass += ' week-start';
  if (g.dow === 6 || idx === totalRows - 1) rowClass += ' week-end';

  let html = `<tr class="${rowClass}" data-data="${g.data}">`;
  html += `<td class="td-data">${dayNum} ${monthShort}</td>`;
  html += `<td class="td-giorno">${dayName}</td>`;

  g.turni.forEach(t => {
    html += `<td class="td-turno">${renderCella(t.turno_base, t.flags_base, g.data, t.operatore_id, 'base', t.turno_var)}</td>`;
    html += `<td class="td-turno td-turno-var">${renderCella(t.turno_var, t.flags_var, g.data, t.operatore_id, 'var')}</td>`;
  });

  const cd = g.colonne_destra;
  html += '<td class="td-sep-destra td-sep-fest"></td>';
  ['rep1','rep2','rep3'].forEach(k => {
    html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'â€”'}</td>`;
  });
  html += '<td class="td-sep-destra"></td>';
  ['fest_m1','fest_m2','fest_p1','fest_p2'].forEach(k => {
    html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'â€”'}</td>`;
  });
  html += '</tr>';
  return html;
}

function refreshTeamRow(dataStr) {
  const idx = datiMese?.giorni?.findIndex(g => g.data === dataStr);
  if (idx == null || idx < 0) return;
  const rowEl = document.querySelector(`tr[data-data="${dataStr}"]`);
  if (!rowEl) return;
  const tmp = document.createElement('tbody');
  tmp.innerHTML = buildTeamRowHtml(datiMese.giorni[idx], idx, datiMese.giorni.length);
  if (tmp.firstElementChild) {
    rowEl.replaceWith(tmp.firstElementChild);
    applySpecialTurnoStyles(document.getElementById('tableWrap'));
  }
}

// ── RENDER CELLA ──────────────────────────────────────────────────────────────
function renderCella(turno, flags, data, opId, col, turnoVar) {
  const isEmpty  = !turno;
  const edClass  = isEditor ? ' editable' : '';
  const colClass = col === 'var' ? ' var-cell' : '';
  const clickEvt = isEditor ? `onclick="openPicker('${data}',${opId},'${col}',event)"` : '';
  const turnoNorm = (turno || '').trim().toUpperCase();
  const isRep    = flags && flags.includes('rep');
  const isSmart  = flags && flags.includes('smart');
  const isF      = flags && flags.includes('F');
  const isM      = flags && flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';

  // Ogni colonna mostra il proprio simbolo di reperibilità in base ai flag dedicati
  const showRep = isRep;

  // Ordine: turno → m → f → ® → #
  const mfHtml   = (isM ? '<span class="mf-inline">m</span>' : '') +
                   (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon  = showRep ? '<span class="cella-rep-icon">®</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  const extras   = mfHtml + repIcon + smartHtml;
  const hasExtras = !!extras;

  // BASE sbarrata quando c'è una variazione
  if (col === 'base' && turnoVar) {
    if (isEmpty) return `<div class="cella-base-sbarrata${edClass}" ${clickEvt}>—${extras}</div>`;
    return `<div class="cella-base-sbarrata${edClass}" ${clickEvt}>${turno}${extras}</div>`;
  }

  if (isEmpty) {
    const repClass = showRep ? ' rep' : '';
    const smartClass = isSmart ? ' smart' : '';
    const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');
    const filledClass = hasExtras ? ' filled' : ' empty';
    return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${filledClass}" ${clickEvt}>—${extras}</div>`;
  }

  // Background giallo reperibilità solo se rep è attivo su questa colonna
  const repClass    = (showRep) ? ' rep' : '';
  const smartClass  = isSmart ? ' smart' : '';
  const filledClass = ' filled';
  return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${filledClass}" ${clickEvt}>${turno}${extras}</div>`;
}

function renderCella(turno, flags, data, opId, col, turnoVar) {
  const isEmpty  = !turno;
  const edClass  = isEditor ? ' editable' : '';
  const colClass = col === 'var' ? ' var-cell' : '';
  const clickEvt = isEditor ? `onclick="openPicker('${data}',${opId},'${col}',event)"` : '';
  const turnoNorm = (turno || '').trim().toUpperCase();
  const isRep    = flags && flags.includes('rep');
  const isSmart  = flags && flags.includes('smart');
  const isF      = flags && flags.includes('F');
  const isM      = flags && flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';
  const showRep = isRep;
  const mfHtml   = (isM ? '<span class="mf-inline">m</span>' : '') +
                   (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon  = showRep ? '<span class="cella-rep-icon">Â®</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  const extras   = mfHtml + repIcon + smartHtml;
  const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');

  if (col === 'base' && turnoVar) {
    const repClass = showRep ? ' rep' : '';
    const smartClass = isSmart ? ' smart' : '';
    const filledClass = isEmpty && !extras ? ' empty' : ' filled';
    const value = isEmpty ? 'â€”' : turno;
    return `<div class="cella cella-base-sbarrata${edClass}${repClass}${smartClass}${specialClass}${filledClass}" ${clickEvt}>${value}${extras}</div>`;
  }

  if (isEmpty) {
    const repClass = showRep ? ' rep' : '';
    const smartClass = isSmart ? ' smart' : '';
    const filledClass = extras ? ' filled' : ' empty';
    return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass}${filledClass}" ${clickEvt}>â€”${extras}</div>`;
  }

  const repClass = showRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass} filled" ${clickEvt}>${turno}${extras}</div>`;
}

function applySpecialTurnoStyles(root = document) {
  const festTurns = new Set(['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO']);
  root.querySelectorAll('.cella.filled, .cella-base-sbarrata, .tpl-cella.filled').forEach(el => {
    // Legge solo i text node diretti della cella, ignorando span figli
    // (ferie-marker, mf-inline, cella-rep-icon, smart-inline) per evitare falsi match
    // es: cella "R" + ferie-marker "F" non deve diventare "RF" = special-fest
    let rawText = '';
    el.childNodes.forEach(node => {
      if (node.nodeType === Node.TEXT_NODE) rawText += node.textContent;
    });
    const raw = rawText.trim().toUpperCase();
    const turno = raw.split(/\s+/)[0] || '';
    el.classList.remove('special-fest', 'special-mal');
    if (raw.startsWith('EX FEST') || raw.startsWith('EX F.') || festTurns.has(turno)) {
      el.classList.add('special-fest');
      return;
    }
    if (turno === 'MAL') el.classList.add('special-mal');
  });
}

// ── PICKER ────────────────────────────────────────────────────────────────────
function openPicker(data, opId, col, evt) {
  closePicker();
  pickerMode = 'table';
  pickerTarget = { data, opId, col };

  // Carica flags correnti dalla riga
  const giorno  = datiMese?.giorni.find(g => g.data === data);
  const turnoData = giorno?.turni.find(t => t.operatore_id === opId);
  const curFlags  = col === 'base' ? (turnoData?.flags_base || '') : (turnoData?.flags_var || '');
  pickerFlags = { smart: curFlags.includes('smart'), rep: curFlags.includes('rep'), F: curFlags.includes('F'), M: curFlags.includes('M') };

  // Pre-carica il turno attuale della cella (così non serve riselezionarlo)
  const curTurno = col === 'base' ? (turnoData?.turno_base ?? '') : (turnoData?.turno_var ?? '');
  pickerTurnoSelected = curTurno !== '' ? curTurno : undefined;

  // Aggiorna label colonna
  const colLabel = document.getElementById('pickerColLabel');
  if (colLabel) {
    colLabel.textContent = col === 'var' ? 'VAR' : 'BASE';
    colLabel.style.color = col === 'var' ? '#60a5fa' : '#94a3b8';
  }

  // Reset e aggiorna preview con turno attuale
  const preview = document.getElementById('pickerPreview');
  if (preview) {
    if (curTurno) {
      preview.textContent = curTurno;
      preview.style.color = '#60a5fa';
    } else {
      preview.textContent = '—';
      preview.style.color = '#475569';
    }
  }

  // Marca il bottone attivo se il turno corrisponde
  document.querySelectorAll('.turno-picker .sb').forEach(b => {
    b.classList.toggle('active', b.dataset.t === curTurno);
  });

  updatePickerFlagBtns();

  const picker = document.getElementById('turnoPicker');
  picker.style.display = 'flex';

  const rect = evt.currentTarget.getBoundingClientRect();
  let top = rect.bottom + 6, left = rect.left;
  if (left + 310 > window.innerWidth) left = window.innerWidth - 314;
  if (top + 440 > window.innerHeight) top = Math.max(10, rect.top - 446);
  picker.style.top  = top  + window.scrollY + 'px';
  picker.style.left = left + 'px';

  evt.stopPropagation();
}

function closePicker() {
  const picker = document.getElementById('turnoPicker');
  picker.style.display = 'none';
  picker.style.zIndex = '500';
  pickerTarget = null;
  pickerMode = 'table';
}

function updatePickerFlagBtns() {
  const map = { smart:'modSmart', rep:'modRep', F:'modF', M:'modM' };
  const cls = { smart:'chip-smart', rep:'chip-rep', F:'chip-F', M:'chip-M' };
  Object.entries(map).forEach(([flag, id]) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    Object.values(cls).forEach(c => btn.classList.remove(c));
    if (pickerFlags[flag]) btn.classList.add(cls[flag]);
  });
}

function pickerToggleFlag(flag, event) {
  pickerFlags[flag] = !pickerFlags[flag];
  updatePickerFlagBtns();
  if (event) event.stopPropagation();
}

function pickerSetTurno(turno, btn) {
  pickerTurnoSelected = turno;
  document.querySelectorAll('.turno-picker .sb').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  const preview = document.getElementById('pickerPreview');
  if (preview) {
    preview.textContent = turno !== '' ? (turno || '—') : '(vuoto)';
    preview.style.color = turno ? '#ffffff' : '#64748b';
  }
  if (event) event.stopPropagation();
}

function pickerAnnulla() {
  closePicker();
}

function pickerCancella() {
  // Cancella: svuota la cella corrente (base o var)
  pickerFlags = { smart:false, rep:false, F:false, M:false };
  updatePickerFlagBtns();
  pickerTurnoSelected = '';
  pickerConferma();
}

async function pickerConferma() {
  const hasFlags = Object.values(pickerFlags).some(Boolean);
  const turno = pickerTurnoSelected === undefined ? (hasFlags ? '' : undefined) : pickerTurnoSelected;
  if (turno === undefined) { alert('Seleziona prima un turno o almeno un indicatore'); return; }

  // ── MODALITÀ TEMPLATE ──
  if (pickerMode === 'template') {
    const { dow, pos } = pickerTemplateTarget;
    const flags = Object.entries(pickerFlags).filter(([,v])=>v).map(([k])=>k).join(',');
    if (!tplData[dow]) tplData[dow] = {};
    tplData[dow][pos] = { turno_base: turno, flags };
    closePicker();
    const tdEl = document.getElementById(`tpl_td_${dow}_${pos}`);
    if (tdEl) tdEl.innerHTML = renderTplCella(dow, pos);
    return;
  }

  if (!pickerTarget) return;

  const { data, opId, col } = pickerTarget;
  const flags = Object.entries(pickerFlags).filter(([,v])=>v).map(([k])=>k).join(',');

  // Recupera turno esistente per preservare l'altro campo
  const giorno    = datiMese?.giorni.find(g => g.data === data);
  const turnoData = giorno?.turni.find(t => t.operatore_id === opId);

  // ── FIX CRITICO: preserva la colonna non modificata ──
  const payload = {
    data,
    operatore_id: opId,
    col,
    flags_base: col === 'base' ? flags : (turnoData?.flags_base ?? ''),
    flags_var:  col === 'var'  ? flags : (turnoData?.flags_var  ?? ''),
    turno_base: col === 'base' ? turno : (turnoData?.turno_base ?? ''),
    turno_var:  col === 'var'  ? turno : (turnoData?.turno_var  ?? ''),
  };
  const prevState = turnoData ? {
    turno_base: turnoData.turno_base ?? '',
    turno_var: turnoData.turno_var ?? '',
    flags_base: turnoData.flags_base ?? '',
    flags_var: turnoData.flags_var ?? '',
  } : null;

  // Aggiornamento locale immediato (ottimistico)
  if (turnoData) {
    if (col === 'base') { turnoData.turno_base = turno; turnoData.flags_base = flags; }
    else                { turnoData.turno_var  = turno; turnoData.flags_var  = flags; }
  }
  refreshTeamRow(data);

  closePicker();

  const saveRes = await fetch('/api/team/turni', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).then(r=>r.json()).catch(()=>({ok:false}));

  const ts = new Date().toLocaleTimeString('it-IT',{hour:'2-digit',minute:'2-digit'});
  const lbl = document.getElementById('lastSaveLabel');
  if (!saveRes.ok) {
    if (turnoData && prevState) {
      turnoData.turno_base = prevState.turno_base;
      turnoData.turno_var = prevState.turno_var;
      turnoData.flags_base = prevState.flags_base;
      turnoData.flags_var = prevState.flags_var;
      refreshTeamRow(data);
    }
    if (lbl) lbl.textContent = '⚠️ Errore salvataggio ' + ts;
    return;
  }
  if (lbl) lbl.textContent = '💾 Salvato ' + ts;

}

// ── COLONNE DESTRA ────────────────────────────────────────────────────────────
async function editColonnaDestra(data, campo, td) {
  const giorno = datiMese?.giorni?.find(g => g.data === data);
  const current = td.textContent.trim() === '—' ? '' : td.textContent.trim();
  const val = prompt(`Valore per ${campo} del ${data}:`, current);
  if (val === null) return;
  td.textContent = val || '—';

  const row = td.closest('tr');
  const tds = row.querySelectorAll('.td-destra');
  const keys = ['rep1','rep2','rep3','fest_m1','fest_m2','fest_p1','fest_p2'];
  const payload = { data };
  tds.forEach((cell, i) => {
    if (keys[i]) payload[keys[i]] = cell.textContent.trim() === '—' ? '' : cell.textContent.trim();
  });
  payload[campo] = val || '';

  await fetch('/api/team/colonne-destra', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
}

// ── SETUP OPERATORI ───────────────────────────────────────────────────────────
function renderSetupOperatorRows(values = []) {
  const rows = document.getElementById('opRows');
  rows.innerHTML = '';
  updateSetupHelpText();
  for (let i=0; i<setupOperatorCount; i++) {
    const nome = (values[i] ?? operatori.find(op => Number(op.posizione) === i + 1)?.nome) || '';
    rows.innerHTML += `<div class="op-row">
      <span class="op-pos">${i+1}</span>
      <input type="text" placeholder="Nome operatore ${i+1}" value="${nome}" id="op_${i}"
        oninput="this.value=this.value.toUpperCase()" style="text-transform:uppercase">
    </div>`;
  }
}

function getSetupValues() {
  return Array.from({length: setupOperatorCount}, (_, i) => document.getElementById(`op_${i}`)?.value || '');
}

function openSetupOperatori() {
  setupOperatorCount = Math.max(getOperatorSlotCount(), 13);
  renderSetupOperatorRows();
  document.getElementById('setupOverlay').classList.add('open');
}

function addOperatoreRow() {
  const values = getSetupValues();
  setupOperatorCount = Math.max(setupOperatorCount + 1, 1);
  renderSetupOperatorRows(values);
}

function removeOperatoreRow() {
  if (setupOperatorCount <= 1) return;
  const values = getSetupValues();
  setupOperatorCount -= 1;
  renderSetupOperatorRows(values);
}

async function saveOperatori() {
  const ops = [];
  for (let i=0; i<setupOperatorCount; i++) {
    const v = document.getElementById(`op_${i}`)?.value.trim();
    if (v) ops.push({ nome: v, posizione: i+1 });
  }
  await fetch('/api/team/operatori', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ operatori: ops })
  });
  document.getElementById('setupOverlay').classList.remove('open');
  await loadOperatori();
  load();
}

// ── TEMPLATE SETTIMANA ────────────────────────────────────────────────────────

function renderTplCella(dow, pos) {
  const d = (tplData[dow] && tplData[dow][pos]) || { turno_base: '', flags: '' };
  const flags = d.flags || '';
  const turno = d.turno_base || '';
  const isRep   = flags.includes('rep');
  const isSmart = flags.includes('smart');
  const isF     = flags.includes('F');
  const isM     = flags.includes('M');
  const mfHtml  = (isM ? '<span class="mf-inline">m</span>' : '') + (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon  = isRep ? '<span class="cella-rep-icon">®</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  const repClass  = isRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  const stateClass = turno ? ' filled' : ' empty';
  return `<div class="cella tpl-cella${repClass}${smartClass}${stateClass}" onclick="openPickerForTemplate(${dow},${pos},event)">${turno||'—'}${mfHtml}${repIcon}${smartHtml}</div>`;
}

function openPickerForTemplate(dow, pos, evt) {
  closePicker();
  pickerMode = 'template';
  pickerTemplateTarget = { dow, pos };

  const d = (tplData[dow] && tplData[dow][pos]) || { turno_base: '', flags: '' };
  const flags = d.flags || '';
  pickerFlags = { smart: flags.includes('smart'), rep: flags.includes('rep'), F: flags.includes('F'), M: flags.includes('M') };
  const curTurno = d.turno_base || '';
  pickerTurnoSelected = curTurno !== '' ? curTurno : undefined;

  const colLabel = document.getElementById('pickerColLabel');
  if (colLabel) { colLabel.textContent = 'TPL'; colLabel.style.color = '#a78bfa'; }

  const preview = document.getElementById('pickerPreview');
  if (preview) {
    preview.textContent = curTurno || '—';
    preview.style.color = curTurno ? '#ffffff' : '#475569';
  }

  document.querySelectorAll('.turno-picker .sb').forEach(b => b.classList.toggle('active', b.dataset.t === curTurno));
  updatePickerFlagBtns();

  const picker = document.getElementById('turnoPicker');
  picker.style.display = 'flex';
  picker.style.zIndex  = '600';

  const rect = evt.currentTarget.getBoundingClientRect();
  let top = rect.bottom + 6, left = rect.left;
  if (left + 310 > window.innerWidth) left = window.innerWidth - 314;
  if (top + 440 > window.innerHeight) top = Math.max(10, rect.top - 446);
  picker.style.top  = top  + window.scrollY + 'px';
  picker.style.left = left + 'px';
  evt.stopPropagation();
}

async function openTemplate() {
  tplData = {};
  tplRepData = {};
  const template = await fetch('/api/team/template-week').then(r=>r.json()).catch(()=>({posizioni:{}, reperibili:{}}));
  const posizioni = template.posizioni || {};
  const reperibili = template.reperibili || {};
  const slotCount = getOperatorSlotCount();
  tplSlotCount = slotCount;

  // Carica i dati nel tplData
  for (let dow=0; dow<7; dow++) {
    tplData[dow] = {};
    tplRepData[dow] = {
      rep1_pos: Number(reperibili[dow]?.rep1_pos) <= slotCount ? (reperibili[dow]?.rep1_pos ?? '') : '',
      rep2_pos: Number(reperibili[dow]?.rep2_pos) <= slotCount ? (reperibili[dow]?.rep2_pos ?? '') : '',
      rep3_pos: Number(reperibili[dow]?.rep3_pos) <= slotCount ? (reperibili[dow]?.rep3_pos ?? '') : '',
      fest_m1_pos: Number(reperibili[dow]?.fest_m1_pos) <= slotCount ? (reperibili[dow]?.fest_m1_pos ?? '') : '',
      fest_m2_pos: Number(reperibili[dow]?.fest_m2_pos) <= slotCount ? (reperibili[dow]?.fest_m2_pos ?? '') : '',
      fest_p1_pos: Number(reperibili[dow]?.fest_p1_pos) <= slotCount ? (reperibili[dow]?.fest_p1_pos ?? '') : '',
      fest_p2_pos: Number(reperibili[dow]?.fest_p2_pos) <= slotCount ? (reperibili[dow]?.fest_p2_pos ?? '') : '',
    };
    for (let pos=1; pos<=slotCount; pos++) {
      const ex = posizioni[dow]?.find(p=>p.posizione===pos) || { turno_base:'', flags:'' };
      tplData[dow][pos] = { turno_base: ex.turno_base || '', flags: ex.flags || '' };
    }
  }

  // Renderizza la tabella
  const GIORNI = ['Lunedì','Martedì','Mercoledì','Giovedì','Venerdì','Sabato','Domenica'];
  const tbody = document.getElementById('templateTableBody');
  tbody.innerHTML = '';
  for (let pos=1; pos<=slotCount; pos++) {
    let row = `<tr><td class="template-pos-label">${pos}</td>`;
    for (let dow=0; dow<7; dow++) {
      row += `<td id="tpl_td_${dow}_${pos}">${renderTplCella(dow, pos)}</td>`;
    }
    row += '</tr>';
    tbody.innerHTML += row;
  }

  const repBody = document.getElementById('templateRepTableBody');
  repBody.innerHTML = '';
  [
    ['rep1_pos', '1° rep'],
    ['rep2_pos', '2° rep'],
    ['rep3_pos', '3° rep'],
  ].forEach(([key, label]) => {
    let row = `<tr><td class="template-rep-label">${label}</td>`;
    for (let dow=0; dow<7; dow++) {
      row += `<td><input type="number" min="1" max="${slotCount}" value="${tplRepData[dow]?.[key] ?? ''}" oninput="updateTemplateRep(${dow},'${key}',this.value,${slotCount})"></td>`;
    }
    row += '</tr>';
    repBody.innerHTML += row;
  });

  const festBody = document.getElementById('templateFestTableBody');
  festBody.innerHTML = '';
  [
    ['fest_m1_pos', 'Fest M1'],
    ['fest_m2_pos', 'Fest M2'],
    ['fest_p1_pos', 'Fest P1'],
    ['fest_p2_pos', 'Fest P2'],
  ].forEach(([key, label]) => {
    let row = `<tr><td class="template-rep-label">${label}</td>`;
    for (let dow=0; dow<7; dow++) {
      row += `<td><input type="number" min="1" max="${slotCount}" value="${tplRepData[dow]?.[key] ?? ''}" oninput="updateTemplateRep(${dow},'${key}',this.value,${slotCount})"></td>`;
    }
    row += '</tr>';
    festBody.innerHTML += row;
  });

  document.getElementById('tpl_start_date').value = template.start_date || '';
  document.getElementById('tpl_end_date').value   = template.end_date || '';

  applySpecialTurnoStyles(document.getElementById('templateOverlay'));
  document.getElementById('templateOverlay').classList.add('open');
}

function updateTemplateRep(dow, key, value, maxPos) {
  if (!tplRepData[dow]) tplRepData[dow] = {
    rep1_pos:'', rep2_pos:'', rep3_pos:'',
    fest_m1_pos:'', fest_m2_pos:'', fest_p1_pos:'', fest_p2_pos:'',
  };
  const num = parseInt(value, 10);
  tplRepData[dow][key] = Number.isFinite(num) && num >= 1 && num <= maxPos ? num : '';
}

async function saveTemplate() {
  const slotCount = tplSlotCount || getOperatorSlotCount();
  const posizioni = {};
  for (let dow=0; dow<7; dow++) {
    posizioni[dow] = [];
    for (let pos=1; pos<=slotCount; pos++) {
      const d = (tplData[dow] && tplData[dow][pos]) || { turno_base:'', flags:'' };
      posizioni[dow].push({
        posizione:  pos,
        turno_base: d.turno_base || '',
        turno_var:  '',
        flags:      d.flags || '',
      });
    }
  }
  const reperibili = {};
  for (let dow=0; dow<7; dow++) {
    const rep = tplRepData[dow] || {};
    reperibili[dow] = {
      rep1_pos: rep.rep1_pos || null,
      rep2_pos: rep.rep2_pos || null,
      rep3_pos: rep.rep3_pos || null,
      fest_m1_pos: rep.fest_m1_pos || null,
      fest_m2_pos: rep.fest_m2_pos || null,
      fest_p1_pos: rep.fest_p1_pos || null,
      fest_p2_pos: rep.fest_p2_pos || null,
    };
  }

  const newStart = document.getElementById('tpl_start_date').value;
  const newEnd   = document.getElementById('tpl_end_date').value;
  if (newStart && newEnd && newStart > newEnd) { alert('La data inizio non può essere successiva alla data fine.'); return; }

  const res = await fetch('/api/team/template-week', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ posizioni, reperibili, start_date: newStart || null, end_date: newEnd || null })
  }).then(r=>r.json());

  if (res.ok) {
    document.getElementById('templateResult').textContent = '✅ Template salvato!';
    document.getElementById('templateOverlay').classList.remove('open');
    await load();
  } else {
    document.getElementById('templateResult').textContent = '❌ Errore nel salvataggio.';
  }
}

function clearTemplateDateRange() {
  document.getElementById('tpl_start_date').value = '';
  document.getElementById('tpl_end_date').value   = '';
  document.getElementById('templateResult').textContent = '🧹 Intervallo date resettato.';
}

// ── LOG ───────────────────────────────────────────────────────────────────────
let logOpen = false;
async function toggleLog() {
  logOpen = !logOpen;
  const panel = document.getElementById('logPanel');
  panel.style.display = logOpen ? 'block' : 'none';
  if (logOpen) await loadLog();
}

async function loadLog() {
  const logs = await fetch('/api/team/log?limit=200').then(r=>r.json()).catch(()=>[]);
  const el = document.getElementById('logContent');
  if (!logs.length) { el.innerHTML = '<p style="color:var(--muted)">Nessuna modifica registrata</p>'; return; }
  el.innerHTML = logs.map(l => {
    const color = l.nuovo_valore ? '#34D399' : '#EF4444';
    return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
      <div style="display:flex;justify-content:space-between;margin-bottom:3px">
        <span style="font-weight:700;color:var(--text)">${l.operatore_nome||''}</span>
        <span style="color:var(--muted);font-size:0.7rem">${(l.data_modifica||'').slice(0,10)}</span>
      </div>
      <div style="font-size:0.72rem">
        📅 ${l.data_turno||''} &nbsp;·&nbsp; ${l.campo||''}
        &nbsp;<span style="text-decoration:line-through;color:#EF4444">${l.vecchio_valore||'—'}</span>
        → <span style="color:${color};font-weight:700">${l.nuovo_valore||'(vuoto)'}</span>
      </div>
      <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">da: ${l.utente||'?'}</div>
    </div>`;
  }).join('');
}

// Override pulito dei renderer: evita simboli corrotti e mantiene TAB invariata salvo la diagonale
function renderCella(turno, flags, data, opId, col, turnoVar, ferieRequest) {
  const isEmpty = !turno;
  const edClass = isEditor ? ' editable' : '';
  const colClass = col === 'var' ? ' var-cell' : '';
  const clickEvt = isEditor ? `onclick="openPicker('${data}',${opId},'${col}',event)"` : '';
  const contextEvt = isEditor ? `oncontextmenu="applyFerieRangeFromCell('${data}',${opId},event)"` : '';
  
  // Gestione click per Swap/Ferie
  let customClick = '';
  if (ferieMode) customClick = `onclick="toggleFerieDate('${data}')"`;
  else if (swapMode) customClick = `onclick="handleSwapSelection('${data}',${opId},'${col}',event)"`;

  const ferieClass = getFerieRequestCellClass(ferieRequest, data, opId, col);
  
  // Classi per Swap
  let swapClass = '';
  if (swapMode) {
    const isOwn = Number(opId) == Number(teamContext?.linked_operatore_id);
    if (swapPhase === 1 && isOwn) swapClass = ' swap-target-valid';
    else if (swapSource && swapSource.data === data && Number(swapSource.opId) == Number(opId) && swapSource.col === col) swapClass = ' swap-source-selected';
    else if (swapPhase === 2 && swapSource && swapSource.data === data && Number(opId) != Number(swapSource.opId)) swapClass = ' swap-target-valid';
  }

  const turnoNorm = (turno || '').trim().toUpperCase();
  const isRep = flags && flags.includes('rep');
  const isSmart = flags && flags.includes('smart');
  const isF = flags && flags.includes('F');
  const isM = flags && flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';

  const repClass = isRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');
  const mfHtml = (isM ? '<span class="mf-inline">m</span>' : '') +
                 (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon = isRep ? '<span class="cella-rep-icon">&reg;</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  const extras = mfHtml + repIcon + smartHtml;
  const value = isEmpty ? '' : turno;

  if (col === 'base' && turnoVar) {
    const filledClass = isEmpty && !extras ? ' empty' : ' filled';
    return `<div class="cella cella-base-sbarrata${edClass}${repClass}${smartClass}${specialClass}${ferieClass}${swapClass}${filledClass}" ${customClick || clickEvt} ${contextEvt}>${value}${extras}</div>`;
  }

  if (isEmpty) {
    const filledClass = extras ? ' filled' : ' empty';
    return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass}${ferieClass}${swapClass}${filledClass}" ${customClick || clickEvt} ${contextEvt}>${value}${extras}</div>`;
  }

  return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass}${ferieClass}${swapClass} filled" ${customClick || clickEvt} ${contextEvt}>${turno}${extras}</div>`;
}

function renderTplCella(dow, pos) {
  const d = (tplData[dow] && tplData[dow][pos]) || { turno_base: '', flags: '' };
  const flags = d.flags || '';
  const turno = d.turno_base || '';
  const turnoNorm = turno.trim().toUpperCase();
  const isRep = flags.includes('rep');
  const isSmart = flags.includes('smart');
  const isF = flags.includes('F');
  const isM = flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';
  const repClass = isRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');
  const stateClass = turno || isRep || isSmart || isF || isM ? ' filled' : ' empty';
  const mfHtml = (isM ? '<span class="mf-inline">m</span>' : '') + (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon = isRep ? '<span class="cella-rep-icon">&reg;</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  return `<div class="cella tpl-cella${repClass}${smartClass}${specialClass}${stateClass}" onclick="openPickerForTemplate(${dow},${pos},event)">${turno || ''}${mfHtml}${repIcon}${smartHtml}</div>`;
}

function updateFerieUi() {
  const hint      = document.getElementById('ferieModeHint');
  const toggleBtn = document.getElementById('ferieToggleBtn');
  const submitBtn = document.getElementById('ferieSubmitBtn');
  const cancelBtn = document.getElementById('ferieCancelBtn');
  const extraFields = document.getElementById('ferieExtraFields');
  if (!hint || !toggleBtn || !submitBtn || !cancelBtn || !extraFields) return;
  const canRequest   = !!teamContext?.can_request_ferie;
  const linkedName   = teamContext?.linked_operatore_nome || '';
  const pendingCount = Number(teamContext?.ferie_pending_count || 0);
  updatePendingBadges(pendingCount, null);

  if (ferieMode) {
    toggleBtn.style.display = 'none';
    submitBtn.style.display = 'inline-flex';
    cancelBtn.style.display = 'inline-flex';
    extraFields.classList.add('show');
    hint.textContent = `${ferieAdds.size} giorni selezionati`;
    hint.style.display = 'inline';
    if (document.getElementById('swapToggleBtn')) document.getElementById('swapToggleBtn').style.display = 'none';
  } else {
    toggleBtn.style.display = 'inline-flex';
    toggleBtn.disabled = !canRequest;
    toggleBtn.textContent = '📅 Ferie';
    submitBtn.style.display = 'none';
    cancelBtn.style.display = 'none';
    extraFields.classList.remove('show');
    hint.style.display = 'none';
    if (document.getElementById('swapToggleBtn')) document.getElementById('swapToggleBtn').style.display = canRequest ? 'inline-flex' : 'none';
  }

  badge.textContent = pendingCount > 99 ? '99+' : String(pendingCount || '');
  badge.classList.toggle('show', pendingCount > 0);
}

function toggleFerieMode() {
  if (!teamContext?.can_request_ferie) return;
  ferieMode = !ferieMode;
  if (!ferieMode) {
    ferieAdds.clear();
    ferieRemovals.clear();
  }
  updateFerieUi();
  if (datiMese) load();
}

function cancelFerieMode() {
  ferieMode = false;
  ferieAdds.clear();
  ferieRemovals.clear();
  updateFerieUi();
  if (datiMese) load();
}

function toggleFerieDate(dataStr) {
  const giorno = datiMese?.giorni?.find(g => g.data === dataStr);
  const row = giorno?.turni?.find(t => t.operatore_id === teamContext?.linked_operatore_id);
  const hasExisting = !!row?.ferie_request;
  if (hasExisting) {
    if (ferieRemovals.has(dataStr)) ferieRemovals.delete(dataStr); else ferieRemovals.add(dataStr);
    ferieAdds.delete(dataStr);
  } else {
    if (ferieAdds.has(dataStr)) ferieAdds.delete(dataStr); else ferieAdds.add(dataStr);
    ferieRemovals.delete(dataStr);
  }
  updateFerieUi();
  refreshTeamRow(dataStr);
}

async function submitFerieRequest() {
  if (!teamContext?.can_request_ferie) return;
  if (!ferieAdds.size && !ferieRemovals.size) {
    alert('Seleziona almeno un giorno.');
    return;
  }
  const tipo = document.getElementById('ferieRequestType').value;
  const note = document.getElementById('ferieRequestNote').value;
  
  const res = await fetch('/api/team/ferie/request-batch', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ 
      add_dates: Array.from(ferieAdds), 
      remove_dates: Array.from(ferieRemovals),
      tipo: tipo,
      note: note
    })
  }).then(r=>r.json()).catch(()=>({ok:false}));
  if (!res.ok) {
    alert(res.detail || 'Errore durante il salvataggio della richiesta.');
    return;
  }
  ferieMode = false;
  ferieAdds.clear();
  ferieRemovals.clear();
  document.getElementById('ferieRequestNote').value = '';
  const teamMe = await fetch('/api/team/me').then(r=>r.json()).catch(()=>null);
  if (teamMe) teamContext = { ...(teamContext || {}), ...(teamMe || {}) };
  updateFerieUi();
  await load();
}

async function applyFerieRangeFromCell(dataStr, opId, event) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  if (!isEditor) return false;
  const daysRaw = prompt('Quanti giorni consecutivi di ferie vuoi applicare da questa data?', '5');
  if (daysRaw === null) return false;
  const giorni = parseInt(daysRaw, 10);
  if (!Number.isFinite(giorni) || giorni < 1 || giorni > 31) {
    alert('Inserisci un numero di giorni tra 1 e 31.');
    return false;
  }
  const res = await fetch('/api/team/ferie/apply-range', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ operatore_id: opId, start_date: dataStr, giorni })
  }).then(r=>r.json()).catch(()=>({ok:false}));
  if (!res.ok) {
    alert(res.detail || 'Errore durante l applicazione delle ferie.');
    return false;
  }
  await load();
  if (logOpen) await loadLog();
  alert(`Ferie applicate: ${res.applied || 0}. Celle gia' modificate non sovrascritte: ${res.skipped || 0}.`);
  return false;
}

function renderFerieMarker(request, dataStr, opId) {
  if (isEditor) return '';
  const isOwnColumn = opId === teamContext?.linked_operatore_id;
  if (!isOwnColumn) return '';
  if (ferieAdds.has(dataStr) || ferieRemovals.has(dataStr)) {
    return '<span class="ferie-marker-pending" title="Richiesta ferie in attesa"></span>';
  }
  const status = normalizeTeamFerieText(request?.stato || '');
  if (status === 'pending') {
    return '<span class="ferie-marker-pending" title="Richiesta ferie in attesa"></span>';
  }
  return '';
}

function getFerieRequestCellClass(request, dataStr, opId, col) {
  if (col !== 'base') return '';
  const isOwnColumn = opId === teamContext?.linked_operatore_id;
  if (isOwnColumn && ferieRemovals.has(dataStr)) return ' ferie-request-rejected';
  if (isOwnColumn && ferieAdds.has(dataStr)) return ' ferie-request-pending';
  if (!request) return '';
  const status = (request?.stato || 'pending').toLowerCase();
  if (status === 'approved') return ' ferie-request-approved';
  if (status === 'rejected') return ' ferie-request-rejected';
  return ' ferie-request-pending';
}

buildTeamRowHtml = function(g, idx, totalRows = (datiMese?.giorni?.length || 0)) {
  const d = new Date(g.data + 'T12:00:00');
  const dayName = GIORNI_SHORT[g.dow];
  const dayNum  = d.getDate();
  const monthShort = MESI_IT[mese-1].slice(0,3);
  let rowClass = 'week-block';
  if (g.is_domenica) rowClass += ' domenica';
  else if (g.is_sabato) rowClass += ' sabato';
  if (g.is_festivo) rowClass += ' festivo';
  if (g.dow === 0 || idx === 0) rowClass += ' week-start';
  if (g.dow === 6 || idx === totalRows - 1) rowClass += ' week-end';
  let html = `<tr class="${rowClass}" data-data="${g.data}">`;
  html += `<td class="td-data">${dayNum} ${monthShort}</td>`;
  html += `<td class="td-giorno">${dayName}</td>`;
  g.turni.forEach(t => {
    html += `<td class="td-turno">${renderCella(t.turno_base, t.flags_base, g.data, t.operatore_id, 'base', t.turno_var, t.ferie_request)}</td>`;
    html += `<td class="td-turno td-turno-var">${renderCella(t.turno_var, t.flags_var, g.data, t.operatore_id, 'var', '', t.ferie_request)}</td>`;
  });
  const cd = g.colonne_destra;
  html += '<td class="td-sep-destra td-sep-fest"></td>';
  ['rep1','rep2','rep3'].forEach(k => {
    html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'—'}</td>`;
  });
  html += '<td class="td-sep-destra"></td>';
  ['fest_m1','fest_m2','fest_p1','fest_p2'].forEach(k => {
    html += `<td class="td-destra${isEditor?' editable':''}" onclick="${isEditor?`editColonnaDestra('${g.data}','${k}',this)`:''}">${cd[k]||'—'}</td>`;
  });
  html += '</tr>';
  return html;
};

refreshTeamRow = function(dataStr) {
  const idx = datiMese?.giorni?.findIndex(g => g.data === dataStr);
  if (idx == null || idx < 0) return;
  const rowEl = document.querySelector(`tr[data-data="${dataStr}"]`);
  if (!rowEl) return;
  const tmp = document.createElement('tbody');
  tmp.innerHTML = buildTeamRowHtml(datiMese.giorni[idx], idx, datiMese.giorni.length);
  if (tmp.firstElementChild) {
    rowEl.replaceWith(tmp.firstElementChild);
    applySpecialTurnoStyles(document.getElementById('tableWrap'));
  }
};

load = async function() {
  const wrap = document.getElementById('tableWrap');
  wrap.innerHTML = '<div style="text-align:center;padding:60px;color:var(--muted)">Caricamento...</div>';
  if (operatori.length === 0) {
    wrap.innerHTML = `<div style="text-align:center;padding:60px;color:var(--muted)"><p style="font-size:1rem;margin-bottom:12px">Nessun operatore configurato</p>${isEditor ? '<button class="btn-editor primary" onclick="openSetupOperatori()" style="margin:0 auto">Imposta operatori</button>' : "<p>Contatta l'editor per configurare la tabella.</p>"}</div>`;
    return;
  }
  const response = await fetch(`/api/team/turni/${anno}/${mese}`).catch(()=>({ok:false,status:0}));
  if (!response || response.ok===false && response.status===0) {
    wrap.innerHTML = '<div style="text-align:center;padding:60px;color:#fca5a5">Errore caricamento: impossibile contattare il server.</div>';
    return;
  }
  if (!response.ok) {
    const text = await response.text();
    wrap.innerHTML = `<div style="text-align:center;padding:60px;color:#fca5a5">Errore ${response.status}: ${text}</div>`;
    return;
  }
  const data = await response.json();
  datiMese = data;
  let html = `<table class="team-table">${buildTeamTableColgroup(data.operatori.length)}<thead><tr>`;
  html += '<th class="col-data sticky-left">Data</th><th class="col-giorno">Gg</th>';
  data.operatori.forEach(op => {
    html += `<th colspan="2" class="col-op" title="${op.nome}"><div style="font-size:0.58rem;color:#6b7280;font-weight:800;background:#e5e7eb;border-radius:3px;padding:0 3px;display:inline-block;margin-bottom:1px">${op.posizione}</div><div class="th-op-nome">${op.nome}</div><div style="display:grid;grid-template-columns:1fr 1fr;gap:0;margin-top:1px;font-size:0.5rem;color:#9ca3af"><span>TAB</span><span>VAR</span></div></th>`;
  });
  html += '<th class="td-sep-destra" style="width:2px;padding:0"></th><th class="col-destra" style="font-size:0.56rem">Rep<br>1°</th><th class="col-destra" style="font-size:0.56rem">Rep<br>2°</th><th class="col-destra" style="font-size:0.56rem">Rep<br>3°</th><th class="td-sep-destra" style="width:2px;padding:0"></th><th class="col-destra" style="font-size:0.56rem;text-align:center">FEST<br><span style="font-size:0.58rem;font-weight:900">M</span></th><th class="col-destra" style="font-size:0.56rem;text-align:center">FEST<br><span style="font-size:0.58rem;font-weight:900">M</span></th><th class="col-destra" style="font-size:0.56rem;text-align:center">FEST<br><span style="font-size:0.58rem;font-weight:900">P</span></th><th class="col-destra" style="font-size:0.56rem;text-align:center">FEST<br><span style="font-size:0.58rem;font-weight:900">P</span></th></tr></thead><tbody>';
  data.giorni.forEach((g, idx) => { html += buildTeamRowHtml(g, idx, data.giorni.length); });
  html += '</tbody></table>';
  wrap.innerHTML = html;
  applyResponsiveTeamLayout();
  if (!teamTableResizeBound) {
    window.addEventListener('resize', applyResponsiveTeamLayout);
    teamTableResizeBound = true;
  }
  if (window.ResizeObserver) {
    if (teamTableResizeObserver) teamTableResizeObserver.disconnect();
    teamTableResizeObserver = new ResizeObserver(() => applyResponsiveTeamLayout());
    teamTableResizeObserver.observe(wrap);
  }
  applySpecialTurnoStyles(wrap);
};

function normalizeTeamFerieText(value) {
  return String(value || '').toLowerCase();
}

function getTeamFerieStatusMeta(status) {
  const key = normalizeTeamFerieText(status);
  if (key === 'approved') return { key, label: 'Approvata' };
  if (key === 'rejected') return { key, label: 'Rifiutata' };
  if (key === 'pending') return { key, label: 'In attesa' };
  return { key: 'neutral', label: 'Nessuno stato' };
}

function getTeamFerieActionLabel(action, statusTo) {
  const actionKey = normalizeTeamFerieText(action);
  if (actionKey === 'requested') return 'Richiesta inviata';
  if (actionKey === 'removed') return 'Richiesta rimossa';
  if (actionKey === 'reviewed') {
    const meta = getTeamFerieStatusMeta(statusTo);
    if (meta.key === 'approved') return 'Richiesta approvata';
    if (meta.key === 'rejected') return 'Richiesta rifiutata';
    return 'Richiesta revisionata';
  }
  return 'Aggiornamento richiesta';
}

function getTeamFerieHistoryLine(row) {
  const fromMeta = getTeamFerieStatusMeta(row.status_from);
  const toMeta = getTeamFerieStatusMeta(row.status_to);
  return `${row.username || '-'} · ${fromMeta.label} → ${toMeta.label} · da ${row.actor_username || '?'}`;
}

function renderTeamFerieDateChips(dates) {
  const items = Array.from(new Set((dates || []).filter(Boolean))).sort();
  if (!items.length) return '<div class="ferie-group-history">Nessun giorno disponibile</div>';
  return `<div class="ferie-date-list">${items.map(date => `<span class="ferie-date-chip">${date}</span>`).join('')}</div>`;
}

function groupTeamFerieLogs(rows) {
  // The backend writes one DB row per day per action.
  // Example: operator requests 5 days → 5 rows (action='requested', status_to='pending').
  //          editor approves     → 5 rows (action='reviewed', status_to='approved').
  // We want ONE card per holiday request, showing the latest status.
  //
  // PASS 1 — cluster individual day-rows into "batches":
  //   same operatore_id + same action + same status_to + same actor + same minute → same batch.
  // PASS 2 — for each operator, find batches that share the same set of dates
  //   (the reviewed batch has the same dates as the requested batch) and collapse them
  //   into a single card keeping only the latest status.

  const batchMap = new Map(); // batchKey → batch
  (rows || []).forEach(row => {
    const minute = String(row.created_at || '').slice(0, 16); // "2024-06-01T14:32"
    const batchKey = [
      row.operatore_id || '',
      row.action || '',
      row.status_to || '',
      row.actor_username || '',
      minute
    ].join('|');
    if (!batchMap.has(batchKey)) {
      batchMap.set(batchKey, {
        batchKey,
        operatoreId: String(row.operatore_id || ''),
        operatoreNome: row.operatore_nome || '-',
        username: row.username || '-',
        actorUsername: row.actor_username || '?',
        action: row.action || '',
        statusTo: row.status_to || '',
        createdAt: String(row.created_at || ''),
        minute,
        dates: []
      });
    }
    const b = batchMap.get(batchKey);
    if (row.data_turno) b.dates.push(row.data_turno);
  });

  // Normalise date lists in every batch
  batchMap.forEach(b => {
    b.dates = Array.from(new Set(b.dates)).sort();
    b.dateKey = b.dates.join(','); // fingerprint for matching request↔review
  });

  // PASS 2 — group batches by operator; merge request+review pairs that share dates.
  // Key: operatore_id + dateKey → card (we keep the latest-status batch's meta).
  const cardMap = new Map();
  const batches = Array.from(batchMap.values())
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt)); // asc → last write wins

  batches.forEach(batch => {
    const cardKey = `${batch.operatoreId}|${batch.dateKey}`;
    if (!cardMap.has(cardKey)) {
      cardMap.set(cardKey, { cardKey, batch });
    } else {
      // Overwrite with the newer batch (latest action/status wins)
      cardMap.get(cardKey).batch = batch;
    }
  });

  return Array.from(cardMap.values()).map(({ batch }) => {
    const statusMeta = getTeamFerieStatusMeta(batch.statusTo);
    const actionLabel = getTeamFerieActionLabel(batch.action, batch.statusTo);
    return {
      key: batch.batchKey,
      operatoreNome: batch.operatoreNome,
      username: batch.username,
      actorUsername: batch.actorUsername,
      createdAt: batch.createdAt,
      latestCreatedAt: batch.createdAt,
      createdLabel: batch.minute.replace('T', ' '),
      statusMeta,
      actionLabel,
      dates: batch.dates
    };
  }).sort((a, b) => b.latestCreatedAt.localeCompare(a.latestCreatedAt));
}

function rerenderTeamFerieWithFocus(fieldId, selectionStart, selectionEnd) {
  renderTeamFerieLogContent();
  if (!fieldId) return;
  const field = document.getElementById(fieldId);
  if (!field) return;
  field.focus();
  if (typeof selectionStart === 'number' && typeof selectionEnd === 'number' && field.setSelectionRange) {
    field.setSelectionRange(selectionStart, selectionEnd);
  }
}

function filterTeamFeriePending(rows) {
  const needle = normalizeTeamFerieText(teamFerieFilters.name);
  const status = teamFerieFilters.status || 'all';
  if (status !== 'all' && status !== 'pending') return [];
  return (rows || []).filter(row => {
    if (!needle) return true;
    return normalizeTeamFerieText(`${row.operatore_nome} ${row.username}`).includes(needle);
  });
}

function filterTeamFerieLogs(rows) {
  const needle = normalizeTeamFerieText(teamFerieFilters.name);
  const status = teamFerieFilters.status || 'all';
  return (rows || []).filter(row => {
    const matchesName = !needle || normalizeTeamFerieText(`${row.operatore_nome} ${row.username} ${row.actor_username}`).includes(needle);
    const rowStatus = normalizeTeamFerieText(row.status_to || '');
    // When filter is 'all', exclude pure-pending entries from storico
    // (they appear in the "In attesa" section above to avoid duplication).
    if (status === 'all' && rowStatus === 'pending') return false;
    const matchesStatus = status === 'all' || rowStatus === status;
    return matchesName && matchesStatus;
  });
}

function renderTeamFerieLogContent() {
  const elFerie = document.getElementById('logContentFerie');
  if (!elFerie) return;
  const pending = filterTeamFeriePending(teamFerieLogData.pending || []);
  const ferieLogs = filterTeamFerieLogs(teamFerieLogData.logs || []);
  const sections = [];
  sections.push(`
    <div class="ferie-filter-bar">
      <input id="teamFerieNameFilter" type="text" placeholder="Filtra per richiedente" value="${teamFerieFilters.name}">
      <select id="teamFerieStatusFilter">
        <option value="all"${teamFerieFilters.status === 'all' ? ' selected' : ''}>Tutti gli stati</option>
        <option value="pending"${teamFerieFilters.status === 'pending' ? ' selected' : ''}>Richieste</option>
        <option value="approved"${teamFerieFilters.status === 'approved' ? ' selected' : ''}>Accettate</option>
        <option value="rejected"${teamFerieFilters.status === 'rejected' ? ' selected' : ''}>Rifiutate</option>
      </select>
    </div>`);
  sections.push('<div class="ferie-log-scroll">');
  if (isEditor && pending.length) {
    sections.push(`
      <div style="margin-bottom:14px">
        <div style="font-size:0.78rem;color:#ccfbf1;font-weight:800;margin-bottom:8px">In attesa di revisione</div>
        ${pending.map(p => `
          <div class="ferie-pending-card">
            <div style="font-weight:800;color:var(--text)">${p.operatore_nome || '-'}</div>
            <div style="font-size:0.74rem;color:var(--muted)">${p.username || '-'} Â· ${p.giorni || 0} giorni</div>
            <div style="font-size:0.72rem;color:#bfdbfe;margin-top:6px">
              ${p.first_day || ''}${p.last_day && p.last_day !== p.first_day ? ` â†’ ${p.last_day}` : ''}
            </div>
            <div class="ferie-pending-actions">
              <button class="btn-editor primary" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "approved")'>âœ… Approva</button>
              <button class="btn-editor" style="color:#fca5a5;border-color:rgba(239,68,68,.3)" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "rejected")'>âœ• Rifiuta</button>
            </div>
          </div>`).join('')}
      </div>`);
  }
  if (ferieLogs.length) {
    sections.push(`
      <div>
        <div style="font-size:0.78rem;color:#cbd5e1;font-weight:800;margin-bottom:8px">Storico ferie</div>
        ${ferieLogs.slice(0, 120).map(l => `
          <div style="padding:8px 0;border-bottom:1px solid var(--border)">
            <div style="display:flex;justify-content:space-between;gap:12px">
              <span style="font-weight:700;color:var(--text)">${l.operatore_nome || '-'}</span>
              <span style="color:var(--muted);font-size:0.7rem">${(l.created_at || '').slice(0,16).replace('T',' ')}</span>
            </div>
            <div style="margin-top:4px;font-size:0.72rem;color:#dbeafe">
              <span class="ferie-log-type">${l.action || 'ferie'}</span> ${l.data_turno || ''}
            </div>
            <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">
              ${l.username || '-'} Â· ${l.status_from || 'vuoto'} â†’ ${l.status_to || 'vuoto'} Â· da ${l.actor_username || '?'}
            </div>
          </div>`).join('')}
      </div>`);
  }
  sections.push('</div>');
  if ((!isEditor || !pending.length) && !ferieLogs.length) {
    sections.push('<p style="color:var(--muted);margin-top:12px">Nessuna richiesta ferie registrata per il filtro selezionato</p>');
  }
  elFerie.innerHTML = sections.join('');
  document.getElementById('teamFerieNameFilter')?.addEventListener('input', e => {
    teamFerieFilters.name = e.target.value || '';
    renderTeamFerieLogContent();
  });
  document.getElementById('teamFerieStatusFilter')?.addEventListener('change', e => {
    teamFerieFilters.status = e.target.value || 'all';
    renderTeamFerieLogContent();
  });
}

renderTeamFerieLogContent = function() {
  const elFerie = document.getElementById('logContentFerie');
  if (!elFerie) return;
  const pending = filterTeamFeriePending(teamFerieLogData.pending || []);
  const ferieLogs = filterTeamFerieLogs(teamFerieLogData.logs || []);
  const sections = [];
  sections.push(`
    <div class="ferie-filter-bar">
      <input id="teamFerieNameFilter" type="text" placeholder="Filtra per richiedente" value="${teamFerieFilters.name}">
      <select id="teamFerieStatusFilter">
        <option value="all"${teamFerieFilters.status === 'all' ? ' selected' : ''}>Tutti gli stati</option>
        <option value="pending"${teamFerieFilters.status === 'pending' ? ' selected' : ''}>Richieste</option>
        <option value="approved"${teamFerieFilters.status === 'approved' ? ' selected' : ''}>Accettate</option>
        <option value="rejected"${teamFerieFilters.status === 'rejected' ? ' selected' : ''}>Rifiutate</option>
      </select>
    </div>`);
  sections.push('<div class="ferie-log-scroll">');
  if (isEditor && pending.length) {
    sections.push(`
      <div style="margin-bottom:14px">
        <div style="font-size:0.78rem;color:#fef08a;font-weight:800;margin-bottom:8px">In attesa di revisione</div>
        ${pending.map(p => `
          <div class="ferie-pending-card" style="border-color:rgba(250,204,21,.28)">
            <div style="font-weight:800;color:var(--text)">${p.operatore_nome || '-'}</div>
            <div style="font-size:0.74rem;color:var(--muted)">${p.username || '-'} · ${p.giorni || 0} giorni</div>
            <div style="font-size:0.72rem;color:#bfdbfe;margin-top:6px">
              ${p.first_day || ''}${p.last_day && p.last_day !== p.first_day ? ` → ${p.last_day}` : ''}
            </div>
            <div class="ferie-pending-actions">
              <button class="btn-editor primary" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "approved")'>Approva</button>
              <button class="btn-editor" style="color:#fca5a5;border-color:rgba(239,68,68,.3)" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "rejected")'>Rifiuta</button>
            </div>
          </div>`).join('')}
      </div>`);
  }
  if (ferieLogs.length) {
    sections.push(`
      <div>
        <div style="font-size:0.78rem;color:#cbd5e1;font-weight:800;margin-bottom:8px">Storico ferie</div>
        ${ferieLogs.slice(0, 120).map(l => {
          const statusMeta = getTeamFerieStatusMeta(l.status_to);
          return `
          <div style="padding:8px 0;border-bottom:1px solid var(--border)">
            <div style="display:flex;justify-content:space-between;gap:12px">
              <span style="font-weight:700;color:var(--text)">${l.operatore_nome || '-'}</span>
              <span style="color:var(--muted);font-size:0.7rem">${(l.created_at || '').slice(0,16).replace('T',' ')}</span>
            </div>
            <div style="margin-top:4px;font-size:0.72rem;color:#dbeafe;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
              <span>${getTeamFerieActionLabel(l.action, l.status_to)} · ${l.data_turno || ''}</span>
              <span class="ferie-log-type ${statusMeta.key}">${statusMeta.label}</span>
            </div>
            <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">
              ${getTeamFerieHistoryLine(l)}
            </div>
          </div>`;
        }).join('')}
      </div>`);
  }
  sections.push('</div>');
  if ((!isEditor || !pending.length) && !ferieLogs.length) {
    sections.push('<p style="color:var(--muted);margin-top:12px">Nessuna richiesta ferie registrata per il filtro selezionato</p>');
  }
  elFerie.innerHTML = sections.join('');
  document.getElementById('teamFerieNameFilter')?.addEventListener('input', e => {
    teamFerieFilters.name = e.target.value || '';
    rerenderTeamFerieWithFocus('teamFerieNameFilter', e.target.selectionStart, e.target.selectionEnd);
  });
  document.getElementById('teamFerieStatusFilter')?.addEventListener('change', e => {
    teamFerieFilters.status = e.target.value || 'all';
    rerenderTeamFerieWithFocus('teamFerieStatusFilter');
  });
}

renderTeamFerieLogContent = function() {
  const elFerie = document.getElementById('logContentFerie');
  if (!elFerie) return;
  const pending = filterTeamFeriePending(teamFerieLogData.pending || []);
  const ferieLogs = filterTeamFerieLogs(teamFerieLogData.logs || []);
  const groupedLogs = groupTeamFerieLogs(ferieLogs).slice(0, 60);
  const sections = [];
  sections.push(`
    <div class="ferie-filter-bar">
      <input id="teamFerieNameFilter" type="text" placeholder="Filtra per richiedente" value="${teamFerieFilters.name}">
      <select id="teamFerieStatusFilter">
        <option value="all"${teamFerieFilters.status === 'all' ? ' selected' : ''}>Tutti gli stati</option>
        <option value="pending"${teamFerieFilters.status === 'pending' ? ' selected' : ''}>Richieste</option>
        <option value="approved"${teamFerieFilters.status === 'approved' ? ' selected' : ''}>Accettate</option>
        <option value="rejected"${teamFerieFilters.status === 'rejected' ? ' selected' : ''}>Rifiutate</option>
      </select>
    </div>`);
  sections.push('<div class="ferie-log-scroll">');
  if (isEditor && pending.length) {
    sections.push(`
      <div style="margin-bottom:14px">
        <div style="font-size:0.78rem;color:#fef08a;font-weight:800;margin-bottom:8px">In attesa di revisione</div>
        ${pending.map(p => `
          <details class="ferie-group-card pending"${teamFerieFilters.status === 'pending' ? ' open' : ''}>
            <summary class="ferie-group-summary">
              <div class="ferie-group-summary-main">
                <div class="ferie-group-title">
                  <span>${p.operatore_nome || '-'}</span>
                  <span class="ferie-log-type pending">In attesa</span>
                </div>
                <div class="ferie-group-meta">
                  <span>${p.username || '-'}</span>
                  <span>${p.giorni || 0} giorni</span>
                  <span>${p.first_day || ''}${p.last_day && p.last_day !== p.first_day ? ` -> ${p.last_day}` : ''}</span>
                </div>
              </div>
              <span class="ferie-group-chevron">></span>
            </summary>
            <div class="ferie-group-body">
              ${renderTeamFerieDateChips(p.dates || [])}
              <div class="ferie-pending-actions">
                <button class="btn-editor primary" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "approved")'>Approva</button>
                <button class="btn-editor" style="color:#fca5a5;border-color:rgba(239,68,68,.3)" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "rejected")'>Rifiuta</button>
              </div>
            </div>
          </details>`).join('')}
      </div>`);
  }
  if (groupedLogs.length) {
    sections.push(`
      <div>
        <div style="font-size:0.78rem;color:#cbd5e1;font-weight:800;margin-bottom:8px">Storico ferie</div>
        ${groupedLogs.map(group => `
          <details class="ferie-group-card ${group.statusMeta.key}">
            <summary class="ferie-group-summary">
              <div class="ferie-group-summary-main">
                <div class="ferie-group-title">
                  <span>${group.operatoreNome}</span>
                  <span class="ferie-log-type ${group.statusMeta.key}">${group.statusMeta.label}</span>
                </div>
                <div class="ferie-group-meta">
                  <span>${group.actionLabel}</span>
                  <span>${group.dates.length} giorni</span>
                  <span>${group.createdLabel || '-'}</span>
                </div>
              </div>
              <span class="ferie-group-chevron">></span>
            </summary>
            <div class="ferie-group-body">
              ${renderTeamFerieDateChips(group.dates)}
              <div class="ferie-group-history">${group.username || '-'} - da ${group.actorUsername || '?'}</div>
            </div>
          </details>`).join('')}
      </div>`);
  }
  sections.push('</div>');
  if ((!isEditor || !pending.length) && !groupedLogs.length) {
    sections.push('<p style="color:var(--muted);margin-top:12px">Nessuna richiesta ferie registrata per il filtro selezionato</p>');
  }
  elFerie.innerHTML = sections.join('');
  document.getElementById('teamFerieNameFilter')?.addEventListener('input', e => {
    teamFerieFilters.name = e.target.value || '';
    rerenderTeamFerieWithFocus('teamFerieNameFilter', e.target.selectionStart, e.target.selectionEnd);
  });
  document.getElementById('teamFerieStatusFilter')?.addEventListener('change', e => {
    teamFerieFilters.status = e.target.value || 'all';
    rerenderTeamFerieWithFocus('teamFerieStatusFilter');
  });
}

loadLog = async function() {
  const [logs, ferieDash] = await Promise.all([
    fetch('/api/team/log?limit=120').then(r=>r.json()).catch(()=>[]),
    fetch('/api/team/ferie/dashboard').then(r=>r.json()).catch(()=>({pending:[], recent_log:[]})),
  ]);
  const pending   = ferieDash.pending     || [];
  const ferieLogs = ferieDash.recent_log  || [];

  // ── TAB TURNI ────────────────────────────────────────────
  const elTurni = document.getElementById('logContentTurni');
  if (elTurni) {
    if (!logs.length) {
      elTurni.innerHTML = '<p style="color:var(--muted)">Nessuna modifica ai turni registrata</p>';
    } else {
      elTurni.innerHTML = logs.slice(0, 80).map(l => {
        const color = l.nuovo_valore ? '#34D399' : '#EF4444';
        return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
          <div style="display:flex;justify-content:space-between;margin-bottom:3px">
            <span style="font-weight:700;color:var(--text)">${l.operatore_nome||''}</span>
            <span style="color:var(--muted);font-size:0.7rem">${(l.data_modifica||'').slice(0,10)}</span>
          </div>
          <div style="font-size:0.72rem">
            📅 ${l.data_turno||''} &nbsp;·&nbsp; ${l.campo||''}
            &nbsp;<span style="text-decoration:line-through;color:#EF4444">${l.vecchio_valore||'—'}</span>
            → <span style="color:${color};font-weight:700">${l.nuovo_valore||'(vuoto)'}</span>
          </div>
          <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">da: ${l.utente||'?'}</div>
        </div>`;
      }).join('');
    }
  }

  // ── TAB FERIE ────────────────────────────────────────────
  const elFerie = document.getElementById('logContentFerie');
  if (elFerie) {
    const sections = [];
    if (isEditor && pending.length) {
      sections.push(`
        <div style="margin-bottom:14px">
          <div style="font-size:0.78rem;color:#ccfbf1;font-weight:800;margin-bottom:8px">In attesa di revisione</div>
          ${pending.map(p => `
            <div class="ferie-pending-card">
              <div style="font-weight:800;color:var(--text)">${p.operatore_nome || '-'}</div>
              <div style="font-size:0.74rem;color:var(--muted)">${p.username || '-'} · ${p.giorni || 0} giorni</div>
              <div style="font-size:0.72rem;color:#bfdbfe;margin-top:6px">
                ${p.first_day || ''}${p.last_day && p.last_day !== p.first_day ? ` → ${p.last_day}` : ''}
              </div>
              <div class="ferie-pending-actions">
                <button class="btn-editor primary" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "approved")'>✅ Approva</button>
                <button class="btn-editor" style="color:#fca5a5;border-color:rgba(239,68,68,.3)" onclick='reviewPendingFerie(${p.operatore_id}, ${JSON.stringify(p.dates || []).replace(/'/g, "\\'")}, "rejected")'>✕ Rifiuta</button>
              </div>
            </div>`).join('')}
        </div>`);
    }
    if (ferieLogs.length) {
      sections.push(`
        <div>
          <div style="font-size:0.78rem;color:#cbd5e1;font-weight:800;margin-bottom:8px">Storico ferie</div>
          ${ferieLogs.slice(0, 60).map(l => `
            <div style="padding:8px 0;border-bottom:1px solid var(--border)">
              <div style="display:flex;justify-content:space-between;gap:12px">
                <span style="font-weight:700;color:var(--text)">${l.operatore_nome || '-'}</span>
                <span style="color:var(--muted);font-size:0.7rem">${(l.created_at || '').slice(0,16).replace('T',' ')}</span>
              </div>
              <div style="margin-top:4px;font-size:0.72rem;color:#dbeafe">
                <span class="ferie-log-type">${l.action || 'ferie'}</span> ${l.data_turno || ''}
              </div>
              <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">
                ${l.username || '-'} · ${l.status_from || 'vuoto'} → ${l.status_to || 'vuoto'} · da ${l.actor_username || '?'}
              </div>
            </div>`).join('')}
        </div>`);
    }
    elFerie.innerHTML = sections.length
      ? sections.join('')
      : '<p style="color:var(--muted)">Nessuna richiesta ferie registrata</p>';
  }

  updatePendingBadges(pending.length, null);
};

loadLog = async function() {
  const [logs, ferieDash, swaps] = await Promise.all([
    fetch('/api/team/log?limit=120').then(r=>r.ok ? r.json() : []).catch(()=>[]),
    fetch('/api/team/ferie/dashboard').then(r=>r.ok ? r.json() : {pending:[], recent_log:[]}).catch(()=>({pending:[], recent_log:[]})),
    fetch('/api/team/swaps').then(r=>r.ok ? r.json() : []).then(x=>Array.isArray(x) ? x : []).catch(()=>[]),
  ]);
  const pending = ferieDash.pending || [];
  const ferieLogs = ferieDash.recent_log || [];

  const elTurni = document.getElementById('logContentTurni');
  if (elTurni) {
    if (!logs.length) {
      elTurni.innerHTML = '<p style="color:var(--muted)">Nessuna modifica ai turni registrata</p>';
    } else {
      elTurni.innerHTML = logs.slice(0, 80).map(l => {
        const color = l.nuovo_valore ? '#34D399' : '#EF4444';
        return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
          <div style="display:flex;justify-content:space-between;margin-bottom:3px">
            <span style="font-weight:700;color:var(--text)">${l.operatore_nome||''}</span>
            <span style="color:var(--muted);font-size:0.7rem">${(l.data_modifica||'').slice(0,10)}</span>
          </div>
          <div style="font-size:0.72rem">
            📅 ${l.data_turno||''} &nbsp;·&nbsp; ${l.campo||''}
            &nbsp;<span style="text-decoration:line-through;color:#EF4444">${l.vecchio_valore||'—'}</span>
            → <span style="color:${color};font-weight:700">${l.nuovo_valore||'(vuoto)'}</span>
          </div>
          <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">da: ${l.utente||'?'}</div>
        </div>`;
      }).join('');
    }
  }

  teamFerieLogData = { pending, logs: ferieLogs };
  renderTeamFerieLogContent();
  renderScambiContent(swaps);

  updatePendingBadges((pending || []).length, countPendingSwaps(swaps));
};

function renderScambiContent(swaps) {
  const el = document.getElementById('logContentScambi');
  if (!el) return;
  
  if (!swaps.length) {
    el.innerHTML = '<p style="color:var(--muted);margin-top:12px">Nessuna richiesta di scambio</p>';
    return;
  }
  
  el.innerHTML = swaps.map(s => {
    const isToMe = Number(s.collega_id) === Number(teamContext?.linked_operatore_id);
    const isFromMe = Number(s.richiedente_id) === Number(teamContext?.linked_operatore_id);
    const editorCanApply = isEditor && ['pending_target', 'pending_editor'].includes(s.stato);
    const targetCanAccept = isToMe && s.stato === 'pending_target';
    const canReview = editorCanApply || targetCanAccept;
    const approveHandler = editorCanApply ? `reviewSwap(${s.id}, 'approve')` : `respondToSwap(${s.id}, 'accept')`;
    const rejectHandler = editorCanApply ? `reviewSwap(${s.id}, 'reject')` : `respondToSwap(${s.id}, 'reject')`;
    const approveLabel = editorCanApply ? 'Approva e applica' : 'Accetta';
    
    let statusLabel = '';
    let statusClass = 'neutral';
    if (s.stato === 'pending_target') { statusLabel = 'In attesa (collega)'; statusClass = 'pending'; }
    else if (s.stato === 'pending_editor') { statusLabel = 'In attesa (Editor)'; statusClass = 'pending'; }
    else if (s.stato === 'approved') { statusLabel = 'Approvata'; statusClass = 'approved'; }
    else if (s.stato === 'rejected') { statusLabel = 'Rifiutata'; statusClass = 'rejected'; }
    else if (s.stato === 'cancelled') { statusLabel = 'Annullata'; statusClass = 'neutral'; }

    return `
      <div class="ferie-pending-card" style="margin-bottom:8px; border-color:var(--border)">
        <div style="display:flex; justify-content:space-between; align-items:start">
          <div style="font-weight:800; font-size:0.8rem">${s.richiedente_nome} ⇄ ${s.collega_nome}</div>
          <span class="ferie-log-type ${statusClass}" style="font-size:0.6rem">${statusLabel}</span>
        </div>
        <div style="font-size:0.72rem; color:var(--muted); margin-top:4px">
          📅 ${s.data} <br>
          Turno ${s.richiedente_nome} (${s.from_col === 'var' ? 'VAR' : 'TAB'}): <strong>${s.from_turno || '---'}</strong> <br>
          Turno ${s.collega_nome} (${s.to_col === 'var' ? 'VAR' : 'TAB'}): <strong>${s.to_turno || '---'}</strong>
        </div>
        ${canReview ? `
          <div class="ferie-pending-actions" style="margin-top:10px">
            <button class="btn-editor primary" style="padding:4px 8px; font-size:0.7rem" onclick="${approveHandler}">${approveLabel}</button>
            <button class="btn-editor" style="padding:4px 8px; font-size:0.7rem; color:#fca5a5" onclick="${rejectHandler}">Rifiuta</button>
          </div>
        ` : ''}
        ${isFromMe && s.stato === 'pending_target' ? `
           <div style="margin-top:8px"><button class="btn-editor" style="padding:3px 6px; font-size:0.65rem" onclick="respondToSwap(${s.id}, 'cancel')">Annulla richiesta</button></div>
        ` : ''}
      </div>
    `;
  }).join('');
}

async function reviewSwap(id, action) {
  const res = await fetch(`/api/team/swaps/${id}/review`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action })
  }).then(r => r.json()).catch(() => ({ ok: false }));
  
  if (!res.ok) {
    alert(res.detail || 'Errore durante la revisione dello scambio.');
    return;
  }
  
  loadLog();
  load();
}

async function respondToSwap(id, action) {
  const res = await fetch(`/api/team/swaps/${id}/respond`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action })
  }).then(r => r.json()).catch(() => ({ ok: false }));
  
  if (!res.ok) {
    alert(res.detail || 'Errore durante la risposta allo scambio.');
    return;
  }
  
  loadLog();
  load();
}

reviewPendingFerie = async function(operatoreId, dates, status) {
  const res = await fetch('/api/team/ferie/review', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ operatore_id: operatoreId, dates, status })
  }).then(r=>r.json()).catch(()=>({ok:false}));
  if (!res.ok) {
    alert(res.detail || 'Errore durante la revisione della richiesta ferie.');
    return;
  }
  if (status === 'approved' && Number(res.skipped || 0) > 0) {
    alert(`Richiesta approvata. F applicata su ${res.applied || 0} giorni; ${res.skipped || 0} celle VAR gia' modificate sono rimaste invariate.`);
  }
  const teamMe = await fetch('/api/team/me').then(r=>r.json()).catch(()=>null);
  if (teamMe) teamContext = { ...(teamContext || {}), ...(teamMe || {}) };
  updateFerieUi();
  await load();
  await loadLog();
};

toggleLog = async function() {
  logOpen = !logOpen;
  const panel = document.getElementById('logPanel');
  panel.style.display = logOpen ? 'block' : 'none';
  if (logOpen) await loadLog();
};

function switchLogTab(tab) {
  const tTurni  = document.getElementById('logContentTurni');
  const tFerie  = document.getElementById('logContentFerie');
  const tScambi = document.getElementById('logContentScambi');
  const bTurni  = document.getElementById('logTabTurni');
  const bFerie  = document.getElementById('logTabFerie');
  const bScambi = document.getElementById('logTabScambi');
  
  if (!tTurni || !tFerie || !bTurni || !bFerie) return;
  
  tTurni.style.display = tab === 'turni' ? '' : 'none';
  tFerie.style.display = tab === 'ferie' ? '' : 'none';
  if (tScambi) tScambi.style.display = tab === 'scambi' ? '' : 'none';
  
  [bTurni, bFerie, bScambi].forEach(b => {
    if (!b) return;
    const active = b.id.toLowerCase().includes(tab);
    b.style.background = active ? 'var(--accent)' : 'var(--card2)';
    b.style.color = active ? 'white' : 'var(--muted)';
    b.style.border = active ? 'none' : '1px solid var(--border)';
  });
}

// ── LOGICA SWAP (CAMBIO TURNO) ────────────────────────────────────────────────
function toggleSwapMode() {
  if (!teamContext?.can_request_ferie) return;
  if (ferieMode) toggleFerieMode();
  swapMode = !swapMode;
  swapPhase = 1;
  swapSource = null;
  swapTarget = null;
  updateSwapUi();
  load();
}

function cancelSwapMode() {
  swapMode = false;
  swapPhase = 1;
  swapSource = null;
  swapTarget = null;
  updateSwapUi();
  refreshSwapCellClasses();
}

function updateSwapUi() {
  const toggleBtn = document.getElementById('swapToggleBtn');
  const submitBtn = document.getElementById('swapSubmitBtn');
  const cancelBtn = document.getElementById('swapCancelBtn');
  const hint      = document.getElementById('swapModeHint');
  const ferieBtn  = document.getElementById('ferieToggleBtn');
  
  if (!toggleBtn || !submitBtn || !cancelBtn || !hint) return;
  
  const canRequest = !!teamContext?.can_request_ferie;
  
  if (swapMode) {
    toggleBtn.style.display = 'none';
    submitBtn.style.display = 'inline-flex';
    cancelBtn.style.display = 'inline-flex';
    hint.style.display = 'inline';
    if (ferieBtn) ferieBtn.style.display = 'none';
    
    if (swapPhase === 1) {
      hint.textContent = 'Fase 1: Seleziona il tuo turno';
      submitBtn.disabled = true;
    } else {
      hint.textContent = swapTarget ? 'Pronto per inviare' : 'Fase 2: Seleziona con quale collega cambiarlo';
      submitBtn.disabled = !swapTarget;
    }
  } else {
    toggleBtn.style.display = canRequest ? 'inline-flex' : 'none';
    submitBtn.style.display = 'none';
    cancelBtn.style.display = 'none';
    hint.style.display = 'none';
    if (ferieBtn) ferieBtn.style.display = canRequest ? 'inline-flex' : 'none';
  }
}

function setBadgeCount(el, count) {
  if (!el) return;
  const n = Number(count || 0);
  if (n > 0) {
    el.textContent = n;
    el.classList.add('show');
  } else {
    el.textContent = '';
    el.classList.remove('show');
  }
}

function updatePendingBadges(ferieCount, scambiCount) {
  if (ferieCount !== null && ferieCount !== undefined) pendingBadgeCounts.ferie = Number(ferieCount || 0);
  if (scambiCount !== null && scambiCount !== undefined) pendingBadgeCounts.scambi = Number(scambiCount || 0);
  setBadgeCount(document.getElementById('ferieAlertBadge'), pendingBadgeCounts.ferie + pendingBadgeCounts.scambi);
  setBadgeCount(document.getElementById('ferieTabBadge'), pendingBadgeCounts.ferie);
  setBadgeCount(document.getElementById('scambiTabBadge'), pendingBadgeCounts.scambi);
}

function countPendingSwaps(swaps) {
  return (swaps || []).filter(s => ['pending_target', 'pending_editor'].includes(s.stato)).length;
}

function getEffectiveSwapSlot(data, opId) {
  const giorno = datiMese?.giorni.find(g => g.data === data);
  const tData  = giorno?.turni.find(t => Number(t.operatore_id) === Number(opId));
  const varTurno = (tData?.turno_var || '').trim();
  const baseTurno = (tData?.turno_base || '').trim();
  if (varTurno) return { col: 'var', turno: varTurno };
  return { col: 'base', turno: baseTurno };
}

function handleSwapSelection(data, opId, col, event) {
  if (!swapMode) return;

  const slot = getEffectiveSwapSlot(data, opId);

  if (swapPhase === 1) {
    if (Number(opId) != Number(teamContext?.linked_operatore_id)) {
      alert('Puoi selezionare solo un turno della tua colonna.');
      return;
    }
    if (!slot.turno) {
      alert('Non c e un turno da scambiare in questo giorno.');
      return;
    }
    swapSource = { data, opId, col: slot.col, turno: slot.turno };
    swapPhase = 2;
    updateSwapUi();
    refreshSwapCellClasses();
  } else {
    if (data !== swapSource.data) {
      alert('Devi selezionare un turno nello stesso giorno.');
      return;
    }
    if (Number(opId) == Number(swapSource.opId)) {
      swapSource = null;
      swapPhase = 1;
      updateSwapUi();
      refreshSwapCellClasses();
      return;
    }

    const opsList = datiMese?.operatori || operatori || [];
    const targetOp = opsList.find(o => Number(o.id) === Number(opId));
    if (targetOp && !targetOp.linked_user_id) {
       alert('Questo operatore non ha un account, impossibile proporre uno scambio.');
       return;
    }
    if (!slot.turno) {
      alert('Il collega non ha un turno da scambiare in questo giorno.');
      return;
    }

    swapTarget = { data, opId, col: slot.col, turno: slot.turno };
    updateSwapUi();
    refreshSwapCellClasses();
  }
}
async function submitSwapRequest() {
  if (!swapSource || !swapTarget) return;
  
  const payload = {
    collega_id: swapTarget.opId,
    data: swapSource.data,
    from_turno: swapSource.turno,
    to_turno: swapTarget.turno,
    from_col: swapSource.col,
    to_col: swapTarget.col
  };
  
  try {
    const res = await fetch('/api/team/swaps', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    
    if (!res.ok) {
      let msg = 'Errore durante l invio della richiesta di scambio.';
      try {
        const data = await res.json();
        if (data.detail) msg = data.detail;
      } catch(e) {}
      alert(msg);
      return;
    }
    
    alert('Richiesta di scambio inviata al collega!');
    cancelSwapMode();
    await load();
    if (logOpen) await loadLog();
  } catch (err) {
    alert('Errore di rete o server non raggiungibile.');
  }
}

// ── NOTIFICHE ────────────────────────────────────────────────────────────────
function toggleNotifDropdown(e) {
  if (e) e.stopPropagation();
  const d = document.getElementById('notifDropdown');
  d.classList.toggle('open');
  if (d.classList.contains('open')) loadNotifications();
}

async function loadNotifications() {
  const res = await fetch('/api/team/notifications').then(r => r.json()).catch(() => []);
  notifications = res;
  renderNotifications();
}

function renderNotifications() {
  const list = document.getElementById('notifList');
  const badge = document.getElementById('notifBadge');
  if (!list || !badge) return;
  
  const unread = notifications.filter(n => !n.is_read);
  badge.textContent = unread.length;
  badge.classList.toggle('show', unread.length > 0);
  
  if (notifications.length === 0) {
    list.innerHTML = '<div class="notif-empty">Nessuna notifica</div>';
    return;
  }
  
  list.innerHTML = notifications.map(n => `
    <div class="notif-item ${n.is_read ? '' : 'unread'}" onclick="handleNotifClick(${n.id}, '${n.link}')">
      <div class="notif-title">${n.title}</div>
      <div class="notif-msg">${n.message}</div>
      <div class="notif-time">${new Date(n.created_at).toLocaleString('it-IT', { day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit' })}</div>
    </div>
  `).join('');
}

async function handleNotifClick(id, link) {
  await fetch(`/api/team/notifications/${id}/read`, { method: 'POST' });
  if (link) window.location.href = link;
  else loadNotifications();
}

async function markAllNotifAsRead() {
  await fetch('/api/team/notifications/read-all', { method: 'POST' });
  loadNotifications();
}

/* ── PRINT PREVIEW MODAL ── */
function openPrintPreviewModal() {
  const overlay = document.getElementById('printPreviewOverlay');
  if (!overlay) return;
  overlay.classList.add('open');
  refreshPrintPreview();
}
function closePrintPreviewModal() {
  const overlay = document.getElementById('printPreviewOverlay');
  if (overlay) overlay.classList.remove('open');
}

function getPrintPreviewSettings() {
  return {
    layout: document.getElementById('ppLayout')?.value || 'landscape',
    color: document.getElementById('ppColor')?.value || 'color',
    paper: document.getElementById('ppPaper')?.value || 'A4',
    scale: parseInt(document.getElementById('ppScale')?.value || '50', 10),
    margins: document.getElementById('ppMargins')?.value || 'default',
    headerFooter: document.getElementById('ppHeaderFooter')?.checked ?? true,
    background: document.getElementById('ppBackground')?.checked ?? true,
  };
}

function refreshPrintPreview() {
  const loading = document.getElementById('ppLoading');
  if (loading) loading.style.display = 'flex';

  const settings = getPrintPreviewSettings();
  let pageWidthMm = settings.paper === 'A4' ? 297 : 356;
  let pageHeightMm = settings.paper === 'A4' ? 210 : 216;
  if (settings.layout === 'portrait') { [pageWidthMm, pageHeightMm] = [pageHeightMm, pageWidthMm]; }
  const marginMm = settings.margins === 'none' ? 0 : settings.margins === 'min' ? 5 : 8;
  const scale = Math.max(20, Math.min(200, settings.scale));

  const now = new Date();
  const generated = now.toLocaleDateString('it-IT', { day:'2-digit', month:'2-digit', year:'numeric' });
  const generatedFull = generated + ' ' + now.toLocaleTimeString('it-IT', { hour:'2-digit', minute:'2-digit' });
  const title = `Turni team - ${MESI_IT[mese - 1]} ${anno}`;

  const wrap = document.getElementById('tableWrap');
  const table = wrap?.querySelector('.team-table');
  if (!wrap || !table) { if (loading) loading.style.display = 'none'; return; }

  const appStyle = Array.from(document.querySelectorAll('style')).map(s => s.textContent || '').join('\n');

  const printHtml = `<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<title>${title}</title>
<style>
${appStyle}
* { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; forced-color-adjust: none !important; box-sizing:border-box; }
html, body { background:#ffffff !important; color:#0f172a !important; margin:0; padding:0; font-family:'Segoe UI',system-ui,sans-serif; }
body { padding:${marginMm}mm; overflow:visible; }
header, .month-tabs-row, .editor-bar, .bottom-nav, #logPanel, .turno-picker, .setup-overlay, .template-overlay, #turnoPicker { display:none !important; }
.table-wrap { overflow:visible !important; background:#ffffff !important; height:auto !important; max-height:none !important; flex:none !important; }
.team-table { font-size:0.75rem !important; border-collapse:collapse !important; width:auto !important; }
.team-table th, .team-table td { position:static !important; filter:none !important; box-shadow:none !important; -webkit-print-color-adjust:exact !important; print-color-adjust:exact !important; }
.team-table th { top:auto !important; left:auto !important; }
.team-table td.td-data, .team-table td.td-giorno, .team-table th.sticky-left, .team-table th.col-giorno { left:auto !important; }
.team-table tr:hover td { filter:none !important; }
.cella, .tpl-cella { min-width:54px !important; min-height:20px !important; font-size:0.74rem !important; -webkit-print-color-adjust:exact !important; print-color-adjust:exact !important; }
.cella.var-cell.filled { font-size:0.8rem !important; }
.cella.var-cell.filled,
.cella.var-cell.filled .mf-inline,
.cella.var-cell.filled .cella-rep-icon,
.cella.var-cell.filled .smart-inline { color:#FF0000 !important; }
.th-op-nome { max-width:none !important; overflow:visible !important; text-overflow:clip !important; }
.print-update-label { text-align:center; font-size:0.7rem; color:#FF0000; font-weight:700; margin-bottom:6px; }
.print-legend { margin-top:12px; font-size:0.75rem; color:#475569; }
@page { size:${pageWidthMm}mm ${pageHeightMm}mm; margin:${marginMm}mm; }
</style>
</head>
<body>
  <div class="print-update-label">Ultimo aggiornamento: ${generatedFull}</div>
  <div class="table-wrap">${wrap.innerHTML}</div>
  <div class="print-legend">R=rep &nbsp; #=smart &nbsp; m/f=mod</div>
</body>
</html>`;

  // Render into the preview iframe
  const paper = document.getElementById('ppPaper');
  const iframe = document.getElementById('ppIframe');
  if (!iframe) return;

  // Set paper size preview
  const paperEl = document.getElementById('ppPaperEl');
  if (paperEl) {
    const pxW = settings.layout === 'landscape' ? 560 : 396;
    const pxH = settings.layout === 'landscape' ? 396 : 560;
    paperEl.style.width = pxW + 'px';
    paperEl.style.minHeight = pxH + 'px';
    iframe.style.height = pxH + 'px';
  }

  try {
    const doc = iframe.contentWindow?.document;
    if (!doc) return;
    doc.open();
    doc.write(printHtml);
    doc.close();
    iframe.onload = () => { if (loading) loading.style.display = 'none'; };
    setTimeout(() => { if (loading) loading.style.display = 'none'; }, 1200);
  } catch(e) {
    if (loading) loading.style.display = 'none';
  }
}

async function executePrint() {
  const now = new Date();
  const generatedFull =
    now.toLocaleDateString('it-IT', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric'
    }) +
    ' ' +
    now.toLocaleTimeString('it-IT', {
      hour: '2-digit',
      minute: '2-digit'
    });

  const title = `Turni team - ${MESI_IT[mese - 1]} ${anno}`;
  const wrap = document.getElementById('tableWrap');
  if (!wrap) return;
  const table = wrap.querySelector('.team-table');
  const operatorCount = datiMese?.operatori?.length || operatori.length || 0;
  const slotCount = Math.max(1, operatorCount * 2);
  const rowCount = table?.querySelectorAll('tbody tr').length || 31;

  const appStyle = Array.from(document.querySelectorAll('style'))
    .map(s => s.textContent || '')
    .join('\n');

  // Calcola scala: margini 5mm sx, 2.5mm dx → area utile 289.5×200mm
  const MM_TO_PX = 96 / 25.4;
  const pageWidthMm = 297;
  const pageHeightMm = 210;
  const marginTopMm = 1.5;
  const marginRightMm = 0.5;
  const marginBottomMm = 0.4;
  const marginLeftMm = 1.5;
  const headerHeightMm = 5;
  const contentWidthMm = pageWidthMm - marginLeftMm - marginRightMm;
  const contentHeightMm = pageHeightMm - marginTopMm - marginBottomMm - headerHeightMm;
  const dateW = 31.7;
  const dayW = 22;
  const sepW = 0.5;
  const sideW = 13.6;
  const fixedWidth = dateW + dayW + (sepW * 2) + (sideW * 7);
  const estimatedPrintRowH = 18;
  const estimatedPrintHeadH = 24;
  const tableH = Math.max(1, estimatedPrintHeadH + (rowCount * estimatedPrintRowH));
  const pageWpx = contentWidthMm * MM_TO_PX;
  const pageHpx = contentHeightMm * MM_TO_PX;
  const slotW = Math.max(28, Math.floor(((pageWpx - fixedWidth) / slotCount) * 1.065));
  const tableW = fixedWidth + (slotCount * slotW);
  const slotFont = slotW <= 30 ? 0.363 : slotW <= 34 ? 0.388 : 0.413;
  const slotVarFont = slotW <= 30 ? 0.4 : slotW <= 34 ? 0.425 : 0.45;
  const opNameFont = slotW <= 30 ? 0.418 : 0.462;
  const scaleW  = pageWpx / tableW;
  const scaleH  = pageHpx / tableH;
  const scaleX  = Math.min(1, scaleW);
  const scaleY  = Math.min(1, scaleH);
  const scale   = scaleX.toFixed(4);
  const scaleYf = scaleY.toFixed(4);

  const printHtml = `<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<title>${title}</title>

<style>
${appStyle}

/* RESET */
* {
  -webkit-print-color-adjust: exact !important;
  print-color-adjust: exact !important;
  forced-color-adjust: none !important;
  box-sizing: border-box;
}

/* PAGINA: A4 landscape, margini 5mm → area utile 287×200mm */
@page {
  size: A4 landscape;
  margin: ${marginTopMm}mm ${marginRightMm}mm ${marginBottomMm}mm ${marginLeftMm}mm;
}

html {
  margin: 0;
  padding: 0;
  width: ${contentWidthMm}mm;
  height: ${pageHeightMm - marginTopMm - marginBottomMm}mm;
}

body {
  position: relative;
  margin: 0;
  padding: 0;
  width: ${contentWidthMm}mm;
  height: ${pageHeightMm - marginTopMm - marginBottomMm}mm;
  background: #fff !important;
  color: #000000 !important;
  font-family: Calibri, 'Segoe UI', Arial, system-ui, sans-serif;
  overflow: hidden;
  page-break-after: avoid;
}

/* NASCONDI UI */
header,
.month-tabs-row,
.editor-bar,
.bottom-nav,
#logPanel,
.turno-picker,
.setup-overlay,
.template-overlay {
  display: none !important;
}

/* HEADER */
.print-header {
  display: block;
  position: absolute;
  left: 0;
  top: 0;
  right: 0;
  font-size: 9px;
  font-weight: 700;
  height: ${headerHeightMm}mm;
  line-height: 4mm;
  margin: 0;
  color: #dc2626 !important;
}

/* WRAPPER DI SCALA: si adatta dinamicamente */
.scale-wrapper {
  position: absolute;
  left: 0;
  top: ${headerHeightMm}mm;
  transform: scaleX(${scale}) scaleY(${scaleYf});
  transform-origin: 0 0;
  width: ${contentWidthMm}mm;
  height: ${contentHeightMm}mm;
  max-height: ${contentHeightMm}mm;
  overflow: visible;
  page-break-inside: avoid;
  break-inside: avoid;
}

/* TABELLA */
.table-wrap {
  --date-w: ${dateW}px;
  --day-w: ${dayW}px;
  --sep-w: ${sepW}px;
  --side-w: ${sideW}px;
  --slot-w: ${slotW}px;
  --slot-font: ${slotFont}rem;
  --slot-var-font: ${slotVarFont}rem;
  --op-name-font: ${opNameFont}rem;
  --team-table-min-width: ${tableW}px;
  overflow: visible !important;
  height: auto !important;
  max-height: none !important;
  background: #fff !important;
}

.table-wrap.is-scroll-mode .team-table,
.table-wrap .team-table {
  width: ${tableW}px !important;
  min-width: ${tableW}px !important;
}

.team-table {
  border-collapse: collapse !important;
  font-size: 0.75rem !important;
  table-layout: fixed !important;
  white-space: nowrap !important;
  background: #fff !important;
  page-break-inside: avoid;
  break-inside: avoid;
}

.team-table th,
.team-table td {
  position: static !important;
  box-shadow: none !important;
  filter: none !important;
  color: #000000;
}

.team-table th {
  padding: 2px 1px !important;
  font-size: 0.46rem !important;
  line-height: 1.05 !important;
}

.team-table td {
  padding-top: 1px !important;
  padding-bottom: 1px !important;
}

.team-table tr {
  page-break-inside: avoid;
  break-inside: avoid;
}

.team-table td.td-turno {
  padding-left: 0.75px !important;
  padding-right: 0.75px !important;
}

.team-table td.td-turno:not(.td-turno-var) {
  padding-right: 0.25px !important;
}

.team-table td.td-turno.td-turno-var {
  padding-left: 0.25px !important;
}

.team-table th.col-op,
.team-table td.td-data,
.team-table td.td-giorno,
.team-table td.td-turno.td-turno-var,
.team-table th.col-destra,
.td-destra {
  border-left-width: 0.5px !important;
  border-right-width: 0.5px !important;
}

.team-table td.td-turno.td-turno-var {
  border-right-width: 1px !important;
}

.team-table th.col-data,
.team-table td.td-data {
  min-width: ${dateW}px !important;
  width: ${dateW}px !important;
  padding-left: 2px !important;
  padding-right: 1px !important;
  line-height: 1 !important;
  text-align: left !important;
}

.team-table td.td-data {
  font-size: 0.37rem !important;
  font-weight: 400 !important;
  padding-right: 0 !important;
}

.team-table th.col-data {
  font-size: 0.355rem !important;
  font-weight: 400 !important;
}

.team-table th.col-giorno,
.team-table td.td-giorno {
  min-width: ${dayW}px !important;
  width: ${dayW}px !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
  line-height: 1 !important;
}

.team-table td.td-giorno {
  font-size: 0.5rem !important;
}

.team-table th.col-giorno {
  font-size: 0.462rem !important;
  font-weight: 400 !important;
}

.team-table th.col-destra,
.td-destra {
  min-width: ${sideW}px !important;
  width: ${sideW}px !important;
  max-width: ${sideW}px !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
  line-height: 1 !important;
  font-weight: 400 !important;
}

.td-destra {
  font-size: 0.345rem !important;
}

.team-table th.col-destra {
  font-size: 0.34rem !important;
  font-weight: 400 !important;
}

.team-table th.col-destra span {
  font-size: 0.36rem !important;
  font-weight: 400 !important;
}

.th-op-nome {
  max-width: none !important;
  overflow: hidden !important;
  text-overflow: clip !important;
  font-size: var(--op-name-font) !important;
  font-weight: 400 !important;
  line-height: 1 !important;
}

.team-table th.col-op > div:last-child {
  font-size: 0.227rem !important;
  font-weight: 400 !important;
  line-height: 1 !important;
  margin-top: 0 !important;
}

.team-table th.col-op > div:first-child {
  font-size: 0.29rem !important;
  font-weight: 400 !important;
  line-height: 1 !important;
  padding: 0 2px !important;
  margin-bottom: 0 !important;
}

/* CELLE */
.cella {
  width: 100% !important;
  min-width: 0 !important;
  min-height: 17px !important;
  padding: 1px 0 !important;
  font-size: var(--slot-font) !important;
  font-weight: 400 !important;
  line-height: .95 !important;
  overflow: hidden !important;
  white-space: nowrap !important;
  gap: 0 !important;
  color: #000000 !important;
}

.cella.empty {
  min-height: 17px !important;
  border: 1px dashed #94a3b8 !important;
}

.cella.var-cell.empty {
  min-height: 17px !important;
  height: 17px !important;
}

.cella.var-cell.filled {
  font-size: var(--slot-var-font) !important;
  font-weight: 400 !important;
  color: #FF0000 !important;
}

.cella.var-cell.filled .mf-inline,
.cella.var-cell.filled .cella-rep-icon,
.cella.var-cell.filled .smart-inline {
  color: #FF0000 !important;
}

.cella-base-sbarrata {
  position: relative !important;
  overflow: hidden !important;
  background-image: linear-gradient(160deg, transparent 48%, #ff0000 49%, #ff0000 51%, transparent 52%) !important;
  background-repeat: no-repeat !important;
  background-size: 100% 100% !important;
}

.cella-base-sbarrata::after {
  content: '' !important;
  position: absolute !important;
  left: -12% !important;
  top: 50% !important;
  width: 124% !important;
  border-top: 2px dashed #ff0000 !important;
  transform: rotate(-19deg) !important;
  transform-origin: center center !important;
  pointer-events: none !important;
  opacity: .95 !important;
}

.mf-inline,
.cella-rep-icon,
.smart-inline {
  display: inline-block !important;
  position: relative !important;
  top: -0.08em !important;
  line-height: 1 !important;
  margin-left: 0 !important;
  flex: 0 0 auto !important;
}

.mf-inline {
  font-size: 0.347rem !important;
  font-weight: 400 !important;
}

.cella-rep-icon {
  font-size: 0.387rem !important;
}

.smart-inline {
  font-size: 0.453rem !important;
  font-weight: 400 !important;
}
</style>
</head>

<body>

  <div class="print-header">
    Turni team &mdash; ${MESI_IT[mese - 1]} ${anno} &nbsp;|&nbsp; Aggiornato: ${generatedFull}
  </div>

  <div class="scale-wrapper">
    <div class="table-wrap">
      ${wrap.innerHTML}
    </div>
  </div>

</body>
</html>`;

  const btn = document.getElementById('downloadPdfBtn');
  const previousLabel = btn ? btn.textContent : 'PDF mese';
  if (btn) { btn.disabled = true; btn.textContent = 'Generazione...'; }

  try {
    const response = await fetch('/api/team/turni/pdf-from-html', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (localStorage.getItem('access_token') || '')
      },
      body: JSON.stringify({ html: printHtml, anno, mese })
    });

    if (!response.ok) {
      if (response.status === 401) { logout(); return; }
      const errText = await response.text();
      throw new Error(errText || `Errore ${response.status}`);
    }

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `turni-team-${anno}-${String(mese).padStart(2, '0')}.pdf`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  } catch (err) {
    console.error('Errore PDF:', err);
    alert('Impossibile generare il PDF: ' + err.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = previousLabel; }
  }
}

function refreshSwapCellClasses() {
  document.querySelectorAll('.cella[data-date]').forEach(el => {
    el.classList.remove('swap-source-selected', 'swap-target-valid', 'swap-target-selected', 'swap-target-disabled');
    el.onclick = el.dataset.origclick ? new Function('event', el.dataset.origclick) : null;
    
    if (!swapMode) return;
    
    const dDate = el.dataset.date;
    const dOpId = Number(el.dataset.opid);
    const dCol = el.dataset.col;
    
    const isOwn = dOpId === Number(teamContext?.linked_operatore_id);
    const isSource = swapSource && swapSource.data === dDate && Number(swapSource.opId) === dOpId && swapSource.col === dCol;
    const isTarget = swapTarget && swapTarget.data === dDate && Number(swapTarget.opId) === dOpId && swapTarget.col === dCol;
    
    if (isSource) {
      el.classList.add('swap-source-selected');
    } else if (isTarget) {
      el.classList.add('swap-target-selected');
    } else if (swapPhase === 1 && isOwn) {
      el.classList.add('swap-target-valid');
    } else if (swapPhase === 2 && swapSource && swapSource.data === dDate && dOpId !== Number(swapSource.opId)) {
      const opsList = datiMese?.operatori || operatori || [];
      const op = opsList.find(o => o.id === dOpId);
      if (op && !op.linked_user_id) {
        el.classList.add('swap-target-disabled');
      } else {
        el.classList.add('swap-target-valid');
      }
    }
  });
}

renderCella = function(turno, flags, data, opId, col, turnoVar, ferieRequest) {
  const isEmpty = !turno;
  const edClass = isEditor ? ' editable' : '';
  const colClass = col === 'var' ? ' var-cell' : '';
  
  // Gestione click per Swap/Ferie
  let customClick = '';
  if (ferieMode) customClick = `onclick="toggleFerieDate('${data}')"`;
  else if (swapMode) customClick = `onclick="handleSwapSelection('${data}',${opId},'${col}',event)"`;

  const clickEvt = isEditor ? `onclick="openPicker('${data}',${opId},'${col}',event)"` : '';
  const contextEvt = isEditor ? `oncontextmenu="applyFerieRangeFromCell('${data}',${opId},event)"` : '';

  const ferieStatusClass = getFerieRequestCellClass(ferieRequest, data, opId, col);
  
  // Classi per Swap
  let swapClass = '';
  if (swapMode) {
    const isOwn = Number(opId) == Number(teamContext?.linked_operatore_id);
    const isSource = swapSource && swapSource.data === data && Number(swapSource.opId) == Number(opId) && swapSource.col === col;
    const isTarget = swapTarget && swapTarget.data === data && Number(swapTarget.opId) == Number(opId) && swapTarget.col === col;

    if (isSource) swapClass = ' swap-source-selected';
    else if (isTarget) swapClass = ' swap-target-selected';
    else if (swapPhase === 1 && isOwn) swapClass = ' swap-target-valid';
    else if (swapPhase === 2 && swapSource && swapSource.data === data && Number(opId) != Number(swapSource.opId)) swapClass = ' swap-target-valid';
  }

  const turnoNorm = (turno || '').trim().toUpperCase();
  const isRep = flags && flags.includes('rep');
  const isSmart = flags && flags.includes('smart');
  const isF = flags && flags.includes('F');
  const isM = flags && flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';

  const repClass = isRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');
  const rfRepClass = turnoNorm === 'RF' && isRep ? ' rf-rep' : '';
  const mfHtml = (isM ? '<span class="mf-inline">m</span>' : '') +
                 (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon = isRep ? '<span class="cella-rep-icon">&reg;</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  const extras = mfHtml + repIcon + smartHtml;
  const marker = col === 'base' ? renderFerieMarker(ferieRequest, data, opId) : '';
  const value = isEmpty ? '' : turno;

  // Blocco selezione se operatore non ha account (per scambi in Fase 2)
  if (swapPhase === 2 && swapMode && !isOwn && col === 'base') {
    const targetOp = operatori.find(o => o.id === opId) || datiMese?.operatori?.find(o => o.id === opId);
    if (targetOp && !targetOp.linked_user_id) {
       swapClass = ' swap-target-disabled';
    }
  }

  const dataAttrs = `data-date="${data}" data-opid="${opId}" data-col="${col}" data-origclick="${customClick ? customClick.replace('onclick="', '').slice(0, -1) : ''}"`;

  if (col === 'base' && turnoVar) {
    const filledClass = isEmpty && !extras ? ' empty' : ' filled';
    return `<div class="cella cella-base-sbarrata${edClass}${repClass}${smartClass}${specialClass}${rfRepClass}${ferieStatusClass}${swapClass}${filledClass}" ${dataAttrs} ${customClick || clickEvt} ${contextEvt}>${value}${extras}${marker}</div>`;
  }

  if (isEmpty) {
    const filledClass = (extras || ferieStatusClass || marker) ? ' filled' : ' empty';
    return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass}${rfRepClass}${ferieStatusClass}${swapClass}${filledClass}" ${dataAttrs} ${customClick || clickEvt} ${contextEvt}>${value}${extras}${marker}</div>`;
  }

  return `<div class="cella${edClass}${colClass}${repClass}${smartClass}${specialClass}${rfRepClass}${ferieStatusClass}${swapClass} filled" ${dataAttrs} ${customClick || clickEvt} ${contextEvt}>${turno}${extras}${marker}</div>`;
};

renderTplCella = function(dow, pos) {
  const d = (tplData[dow] && tplData[dow][pos]) || { turno_base: '', flags: '' };
  const flags = d.flags || '';
  const turno = d.turno_base || '';
  const turnoNorm = turno.trim().toUpperCase();
  const isRep = flags.includes('rep');
  const isSmart = flags.includes('smart');
  const isF = flags.includes('F');
  const isM = flags.includes('M');
  const isFestTurno = ['F', 'RF', 'EX FEST', 'EX F.', 'LUTTO'].includes(turnoNorm);
  const isMalTurno = turnoNorm === 'MAL';
  const rfRepClass = turnoNorm === 'RF' && isRep ? ' rf-rep' : '';
  const repClass = isRep ? ' rep' : '';
  const smartClass = isSmart ? ' smart' : '';
  const specialClass = isFestTurno ? ' special-fest' : (isMalTurno ? ' special-mal' : '');
  const stateClass = turno || isRep || isSmart || isF || isM ? ' filled' : ' empty';
  const mfHtml = (isM ? '<span class="mf-inline">m</span>' : '') + (isF ? '<span class="mf-inline">f</span>' : '');
  const repIcon = isRep ? '<span class="cella-rep-icon">&reg;</span>' : '';
  const smartHtml = isSmart ? '<span class="smart-inline">#</span>' : '';
  return `<div class="cella tpl-cella${repClass}${smartClass}${specialClass}${rfRepClass}${stateClass}" onclick="openPickerForTemplate(${dow},${pos},event)">${turno || ''}${mfHtml}${repIcon}${smartHtml}</div>`;
};

loadLog = async function() {
  const [logs, ferieDash, swaps] = await Promise.all([
    fetch('/api/team/log?limit=120').then(r=>r.ok ? r.json() : []).catch(()=>[]),
    fetch('/api/team/ferie/dashboard').then(r=>r.ok ? r.json() : {pending:[], recent_log:[]}).catch(()=>({pending:[], recent_log:[]})),
    fetch('/api/team/swaps').then(r=>r.ok ? r.json() : []).then(x=>Array.isArray(x) ? x : []).catch(()=>[]),
  ]);
  const pending = ferieDash.pending || [];
  const ferieLogs = ferieDash.recent_log || [];

  const elTurni = document.getElementById('logContentTurni');
  if (elTurni) {
    if (!logs.length) {
      elTurni.innerHTML = '<p style="color:var(--muted)">Nessuna modifica ai turni registrata</p>';
    } else {
      elTurni.innerHTML = logs.slice(0, 80).map(l => {
        const color = l.nuovo_valore ? '#34D399' : '#EF4444';
        return `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
          <div style="display:flex;justify-content:space-between;margin-bottom:3px">
            <span style="font-weight:700;color:var(--text)">${l.operatore_nome||''}</span>
            <span style="color:var(--muted);font-size:0.7rem">${(l.data_modifica||'').slice(0,10)}</span>
          </div>
          <div style="font-size:0.72rem">
            📅 ${l.data_turno||''} &nbsp;·&nbsp; ${l.campo||''}
            &nbsp;<span style="text-decoration:line-through;color:#EF4444">${l.vecchio_valore||'—'}</span>
            → <span style="color:${color};font-weight:700">${l.nuovo_valore||'(vuoto)'}</span>
          </div>
          <div style="font-size:0.68rem;color:var(--muted);margin-top:2px">da: ${l.utente||'?'}</div>
        </div>`;
      }).join('');
    }
  }

  teamFerieLogData = { pending, logs: ferieLogs };
  renderTeamFerieLogContent();
  renderScambiContent(swaps);

  updatePendingBadges((pending || []).length, countPendingSwaps(swaps));
};

