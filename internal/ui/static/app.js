const el = (sel, parent=document) => parent.querySelector(sel);
const els = (sel, parent=document) => [...parent.querySelectorAll(sel)];

const state = {
  base: location.origin,
  token: localStorage.getItem('icetoken') || '',
  connected: false,
  version: '',
  peers: [],
  stats: {},
  locks: [],
  index: [],
  ready: false,
  err: '',
  series: [], // rolling total requests series
};

function headers() {
  const h = {};
  if (state.token) h['Authorization'] = `Bearer ${state.token}`;
  return h;
}

async function api(path, opts={}) {
  const res = await fetch(state.base + path, {
    ...opts,
    headers: { 'Accept': 'application/json', ...(opts.headers||{}), ...headers() },
  });
  return res;
}

async function fetchJson(path) {
  const res = await api(path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function fetchText(path) {
  const res = await fetch(state.base + path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.text();
}

function renderLogin() {
  const app = el('#app');
  app.innerHTML = `
    <div class="container">
      <div class="card">
        <h2>Přihlášení</h2>
        <p>Zadejte API token pro přístup do rozhraní.</p>
        <div style="display:flex;gap:8px;align-items:center;">
          <input id="token" type="password" placeholder="API token" />
          <button id="login">Přihlásit</button>
        </div>
        ${state.err ? `<p style="color:#a40000">${state.err}</p>` : ''}
      </div>
    </div>`;
  el('#login').onclick = async () => {
    state.token = el('#token').value.trim();
    localStorage.setItem('icetoken', state.token);
    state.err = '';
    init();
  };
}

function sparkline(values, width=180, height=40) {
  if (!values || values.length === 0) return '';
  const max = Math.max(...values);
  const min = Math.min(...values);
  const norm = v => max === min ? 0.5 : (v - min) / (max - min);
  const step = width / Math.max(values.length - 1, 1);
  let d = '';
  values.forEach((v,i)=>{
    const x = i*step;
    const y = height - norm(v)*height;
    d += (i===0?`M ${x},${y}`:` L ${x},${y}`);
  });
  return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
    <path d="${d}" fill="none" stroke="#0a84ff" stroke-width="2"/>
  </svg>`;
}

function render() {
  const app = el('#app');
  const peersRows = state.peers.map(p=>`<tr><td>${p}</td></tr>`).join('');
  const locksRows = state.locks.map(l=>`<tr><td>${l.path}</td><td>${l.holder}</td><td>${new Date(l.since).toLocaleString()}</td></tr>`).join('');

  // Sparkline uses rolling total request counts over time
  const reqCounts = state.series;

  app.innerHTML = `
    <div class="header">
      <h1>IceCluster</h1>
      <span>ver ${state.version||''}</span>
      <span class="badge ${state.ready?'ok':'down'}">${state.ready?'Ready':'Not ready'}</span>
      <div style="margin-left:auto;display:flex;gap:8px;align-items:center;">
        <button class="secondary" id="logout">Odhlásit</button>
      </div>
    </div>
    <div class="container">
      <div class="grid">
        <div class="card">
          <h3>Peers</h3>
          <table class="table"><thead><tr><th>URL</th></tr></thead><tbody>${peersRows}</tbody></table>
        </div>
        <div class="card">
          <h3>Locks</h3>
          <table class="table"><thead><tr><th>Path</th><th>Holder</th><th>Since</th></tr></thead><tbody>${locksRows}</tbody></table>
        </div>
        <div class="card">
          <h3>Stats (per peer)</h3>
          <div id="stats"></div>
        </div>
      </div>
      <div class="card">
        <h3>Request rate</h3>
        ${sparkline(reqCounts)}
      </div>
    </div>
    <div class="footer">
      &copy; IceCluster UI
    </div>
  `;
  el('#logout').onclick = ()=>{ localStorage.removeItem('icetoken'); state.token=''; init(); };

  // Render stats as table(s)
  const statsEl = el('#stats');
  const entries = Object.entries(state.stats||{});
  statsEl.innerHTML = entries.map(([peer,data])=>{
    const ops = Object.entries(data.ops||{}).map(([op,m])=>`<tr><td>${op}</td><td>${m.requests}</td><td>${m.success}</td><td>${m.fail}</td><td>${m.avg_ms?.toFixed?.(2)||'-'}</td><td>${m.last_ms?.toFixed?.(2)||'-'}</td></tr>`).join('');
    return `<div style="margin-bottom:12px">
      <div><strong>${peer}</strong> total=${data.total}</div>
      <table class="table"><thead><tr><th>Op</th><th>Req</th><th>OK</th><th>Fail</th><th>Avg ms</th><th>Last ms</th></tr></thead><tbody>${ops}</tbody></table>
    </div>`
  }).join('');
}

async function refresh() {
  try {
    // readiness
    const ready = await fetchJson('/v1/ready');
    state.ready = !!ready.ready;

    // version
    state.version = await fetchText('/v1/version');

    // peers (secured)
    try { state.peers = await fetchJson('/v1/peers'); } catch { state.peers = []; }

    // locks (secured)
    try { state.locks = await fetchJson('/v1/locks'); } catch { state.locks = []; }

  // stats (secured)
  try { state.stats = await fetchJson('/v1/stats'); } catch { state.stats = {}; }

  // update rolling series: sum totals across peers
  const totalNow = Object.values(state.stats||{}).reduce((acc, s)=>acc + (s.total||0), 0);
  const prev = state._lastTotal || 0;
  const delta = Math.max(0, totalNow - prev);
  state._lastTotal = totalNow;
  state.series.push(delta);
  if (state.series.length > 60) state.series.shift();

    render();
  } catch (e) {
    state.err = String(e?.message||e);
    renderLogin();
  }
}

function startPolling() {
  refresh();
  setInterval(refresh, 3000);
}

async function init() {
  // If any secured call fails with 401 and we have token empty, show login
  if (!state.token) {
    // try to call a secured endpoint to see if token is required
    const res = await api('/v1/peers');
    if (res.status === 401) { renderLogin(); return; }
  }
  startPolling();
}

init();
