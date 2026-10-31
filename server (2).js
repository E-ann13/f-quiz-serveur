// F-QUIZ — serveur de salons pour le multijoueur en ligne
// Rôle : relayer les messages entre les joueurs d'un salon (2 pour les duels, jusqu'à 8 pour le Grand Prix). Le serveur ne connaît pas les avions secrets :
// chaque appareil répond lui-même aux questions sur son propre avion.
//
// Démarrage : node server.js   (port : variable PORT, 8787 par défaut)
// Santé : GET /health → {"ok":true,...,"db":"upstash"|"memoire"}
// v1.3 : suppression des données en ligne (lbdel/cdel), filtre des noms grossiers
// v1.2 : classements (lbput/lbget) et sauvegarde en ligne (cnew/cput/cget/clink/cjoin), stockés dans Upstash Redis

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8787;
const ROOM_TTL_MS = 2 * 60 * 60 * 1000;      // un salon inactif disparaît au bout de 2 h
const RECONNECT_GRACE_MS = 10 * 60 * 1000;   // un joueur déconnecté (appli en arrière-plan, coupure) garde sa place 10 min
const MAX_MSG_BYTES = 64 * 1024;
const MAX_MSGS_PER_10S = 120;
const MAX_PLAYERS = 8;                         // duels à 2, Grand Prix jusqu'à 8
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sans I, O, 0, 1 (confusions)

// Transfert de compte : la progression est déposée 15 min sous un code court, lisible une seule fois
const XFER_TTL_MS = 15 * 60 * 1000;
const XFER_MAX_BYTES = 1024 * 1024;
const xfers = new Map(); // code -> {data, at}
function makeXferCode() {
  for (let n = 0; n < 50; n++) {
    let c = '';
    for (let i = 0; i < 6; i++) c += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
    if (!xfers.has(c)) return c;
  }
  throw new Error('no code');
}
setInterval(() => { const now = Date.now(); for (const [k, v] of xfers) if (now - v.at > XFER_TTL_MS) xfers.delete(k); }, 60 * 1000).unref();


// ===== Données durables : classements et sauvegarde en ligne =====
// Base Upstash Redis (variables UPSTASH_REDIS_REST_URL et UPSTASH_REDIS_REST_TOKEN sur Render).
// Sans ces variables, tout reste en mémoire (perdu quand le serveur s'endort) : pratique pour tester.
const crypto = require('crypto');
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const UP_TOK = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const DB_ON = !!(UP_URL && UP_TOK);
const SAVE_MAX = 900 * 1024;
const sha = s => crypto.createHash('sha256').update(String(s)).digest('hex');
const mem = { kv: new Map(), z: new Map(), exp: new Map() };
function memExpired(k) { const e = mem.exp.get(k); if (e && e < Date.now()) { mem.kv.delete(k); mem.z.delete(k); mem.exp.delete(k); return true; } return false; }
// mini-Redis en mémoire (les seules commandes utilisées)
function memCmd(c) {
  const [op, k] = [String(c[0]).toUpperCase(), c[1]];
  if (k !== undefined) memExpired(k);
  switch (op) {
    case 'GET': return mem.kv.has(k) ? mem.kv.get(k) : null;
    case 'SET': {
      const nx = c.includes('NX'); if (nx && mem.kv.has(k)) return null;
      mem.kv.set(k, String(c[2])); const ex = c.indexOf('EX'); if (ex > 0) mem.exp.set(k, Date.now() + (+c[ex + 1]) * 1000); return 'OK';
    }
    case 'DEL': { let n = 0; c.slice(1).forEach(x => { if (mem.kv.delete(x)) n++; if (mem.z.delete(x)) n++; }); return n; }
    case 'ZREM': { const z = mem.z.get(k); return z && z.delete(String(c[2])) ? 1 : 0; }
    case 'INCR': { const v = (+mem.kv.get(k) || 0) + 1; mem.kv.set(k, String(v)); return v; }
    case 'MGET': return c.slice(1).map(x => (memExpired(x), mem.kv.has(x) ? mem.kv.get(x) : null));
    case 'EXPIRE': mem.exp.set(k, Date.now() + (+c[2]) * 1000); return 1;
    case 'ZADD': {
      // ZADD key [GT|LT] score member
      let i = 2, mode = null; if (c[i] === 'GT' || c[i] === 'LT') mode = c[i++];
      const sc = +c[i], m = String(c[i + 1]); const z = mem.z.get(k) || new Map(); mem.z.set(k, z);
      const old = z.get(m);
      if (old === undefined || !mode || (mode === 'GT' && sc > old) || (mode === 'LT' && sc < old)) z.set(m, sc);
      return 1;
    }
    case 'ZSCORE': { const z = mem.z.get(k); const v = z && z.get(String(c[2])); return v === undefined ? null : String(v); }
    case 'ZCARD': { const z = mem.z.get(k); return z ? z.size : 0; }
    case 'ZRANGE': {
      // ZRANGE key start stop [REV] WITHSCORES
      const z = mem.z.get(k); if (!z) return [];
      const rev = c.includes('REV'); const arr = [...z.entries()].sort((a, b) => rev ? b[1] - a[1] : a[1] - b[1]);
      const out = []; arr.slice(+c[2], +c[3] + 1).forEach(([m, sc]) => out.push(m, String(sc))); return out;
    }
    case 'ZRANK': case 'ZREVRANK': {
      const z = mem.z.get(k); if (!z || !z.has(String(c[2]))) return null;
      const arr = [...z.entries()].sort((a, b) => op === 'ZREVRANK' ? b[1] - a[1] : a[1] - b[1]); return arr.findIndex(e => e[0] === String(c[2]));
    }
  }
  throw new Error('cmd ' + op);
}
async function db(cmd) {
  if (!DB_ON) return memCmd(cmd);
  const r = await fetch(UP_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOK, 'content-type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
}
async function dbMany(cmds) {
  if (!DB_ON) return cmds.map(memCmd);
  const r = await fetch(UP_URL + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + UP_TOK, 'content-type': 'application/json' }, body: JSON.stringify(cmds) });
  const j = await r.json(); if (!Array.isArray(j)) throw new Error('pipeline'); return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
const okId = s => typeof s === 'string' && /^[a-z0-9]{8,32}$/.test(s);
const okKey = s => typeof s === 'string' && s.length >= 16 && s.length <= 64;
// Filtre des noms grossiers (classements et salons) : le nom est remplacé par « Pilote »
const BAD_LONG = ['connard', 'connasse', 'salope', 'salaud', 'putain', 'pute', 'encule', 'enculer', 'batard', 'merde', 'couille', 'niquer', 'nique', 'ntm', 'tagueule', 'pede', 'tapette', 'gouine', 'bougnoul', 'negro', 'nazi', 'hitler', 'fuck', 'shit', 'bitch', 'cunt', 'dick', 'pussy', 'nigger', 'nigga', 'faggot', 'whore', 'slut', 'porn', 'asshole', 'bite', 'zizi', 'chatte', 'branleur', 'branle', 'suceur', 'sucemoi', 'fdp', 'ta mere', 'tamere', 'pd'];
const BAD_WORD = ['con', 'cul', 'fag', 'sex', 'pd', 'fdp', 'ntm', 'kkk', 'ass'];
function rude(name) {
  const base = String(name).toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/0/g, 'o').replace(/1|!/g, 'i').replace(/3/g, 'e').replace(/4|@/g, 'a').replace(/5|\$/g, 's').replace(/7/g, 't');
  const squash = base.replace(/[^a-z]/g, ''), words = base.split(/[^a-z]+/).filter(Boolean);
  return BAD_LONG.some(w => w.replace(/ /g, '').length >= 5 ? squash.includes(w.replace(/ /g, '')) : words.includes(w)) || BAD_WORD.some(w => words.includes(w));
}
const cleanName = s => { const n = String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24); return !n || rude(n) ? 'Pilote' : n; };
// semaine ISO (lundi → dimanche), en heure de Paris à peu près (UTC+1)
function weekKey(t) {
  const d = new Date((t || Date.now()) + 3600e3); d.setUTCHours(0, 0, 0, 0);
  const day = d.getUTCDay() || 7; d.setUTCDate(d.getUTCDate() + 4 - day);
  const y0 = new Date(Date.UTC(d.getUTCFullYear(), 0, 1)); const w = Math.ceil(((d - y0) / 864e5 + 1) / 7);
  return d.getUTCFullYear() + '-W' + String(w).padStart(2, '0');
}
const DAY_RE = /^\d{4}-\d{1,2}-\d{1,2}$/;
function dayOk(d) { if (!DAY_RE.test(d)) return false; const [y, m, j] = d.split('-').map(Number); const t = Date.UTC(y, m - 1, j); return Math.abs(t - Date.now()) < 2.2 * 864e5; }
// Classements : lb:xp (XP total), lb:wxp:<semaine> (XP gagnée cette semaine), lb:day:<date> (essais pour trouver l'avion du jour, le moins possible)
const LB = {
  xp: { key: () => 'lb:xp', high: true, max: 1e7 },
  wxp: { key: () => 'lb:wxp:' + weekKey(), high: true, max: 2e5, ttl: 45 * 864e5 },
  day: { key: d => 'lb:day:' + d, high: false, max: 999, ttl: 10 * 864e5 }
};
async function lbAuth(pid, pkey) {
  if (!okId(pid) || !okKey(pkey)) return false;
  const h = sha(pkey);
  const set = await db(['SET', 'lbk:' + pid, h, 'NX']);
  if (set === 'OK') return true;
  return (await db(['GET', 'lbk:' + pid])) === h;
}
async function lbPut(m) {
  if (!(await lbAuth(m.pid, m.pkey))) return { error: 'auth' };
  const meta = JSON.stringify({ n: cleanName(m.name), av: String(m.av || '').slice(0, 40), ban: String(m.ban || '').slice(0, 24), g: String(m.grade || '').slice(0, 40) });
  const cmds = [['SET', 'lbp:' + m.pid, meta]];
  (Array.isArray(m.entries) ? m.entries : []).slice(0, 5).forEach(e => {
    const B = LB[e && e.b]; if (!B) return; const s = Math.round(+e.s);
    if (!(s >= (B.high ? 0 : 1)) || s > B.max) return;
    if (e.b === 'day' && !dayOk(e.d)) return;
    const k = B.key(e.d);
    cmds.push(['ZADD', k, B.high ? 'GT' : 'LT', s, m.pid]);
    if (B.ttl) cmds.push(['EXPIRE', k, Math.round(B.ttl / 1000)]);
  });
  await dbMany(cmds);
  return { ok: true };
}
async function lbDel(m) {
  if (!(await lbAuth(m.pid, m.pkey))) return { error: 'auth' };
  const now = Date.now(), days = [-1, 0, 1].map(k => { const d = new Date(now + k * 864e5); return d.getUTCFullYear() + '-' + (d.getUTCMonth() + 1) + '-' + d.getUTCDate(); });
  const cmds = [['ZREM', 'lb:xp', m.pid], ['ZREM', 'lb:wxp:' + weekKey(), m.pid], ['ZREM', 'lb:wxp:' + weekKey(now - 7 * 864e5), m.pid]]
    .concat(days.map(d => ['ZREM', 'lb:day:' + d, m.pid]), [['DEL', 'lbp:' + m.pid, 'lbk:' + m.pid]]);
  await dbMany(cmds);
  return { ok: true };
}
async function lbGet(m) {
  const B = LB[m.b]; if (!B) return { error: 'board' };
  if (m.b === 'day' && !DAY_RE.test(m.d || '')) return { error: 'day' };
  const k = B.key(m.d);
  const res = await dbMany([['ZRANGE', k, 0, 49].concat(B.high ? ['REV'] : [], ['WITHSCORES']), ['ZCARD', k]].concat(okId(m.pid) ? [[B.high ? 'ZREVRANK' : 'ZRANK', k, m.pid], ['ZSCORE', k, m.pid]] : []));
  const flat = res[0] || [], ids = []; for (let i = 0; i < flat.length; i += 2) ids.push([flat[i], +flat[i + 1]]);
  const metas = ids.length ? await db(['MGET'].concat(ids.map(x => 'lbp:' + x[0]))) : [];
  const top = ids.map(([id, s], i) => { let mt = {}; try { mt = JSON.parse(metas[i] || '{}') || {}; } catch (e) {} return { n: mt.n || 'Pilote', av: mt.av || '', ban: mt.ban || '', g: mt.g || '', s, me: id === m.pid }; });
  const me = okId(m.pid) && res[2] !== null && res[2] !== undefined ? { rank: +res[2] + 1, s: +res[3] } : null;
  return { top, total: +res[1] || 0, me, week: m.b === 'wxp' ? weekKey() : undefined };
}
// Sauvegarde en ligne : un compte = un identifiant + une clé secrète (gardés sur les appareils reliés)
async function cAuth(id, s) {
  if (!okId(id) || !okKey(s)) return false;
  return (await db(['GET', 'cacc:' + id])) === sha(s);
}
async function cloudNew() {
  const id = crypto.randomBytes(8).toString('hex'), s = crypto.randomBytes(18).toString('base64').replace(/[+/=]/g, '').slice(0, 24);
  await db(['SET', 'cacc:' + id, sha(s), 'NX']);
  return { acc: id, s };
}
async function cloudPut(m) {
  if (!(await cAuth(m.acc, m.s))) return { error: 'auth' };
  const data = String(m.data || '');
  if (!/^FQSAVE\d\./.test(data) || data.length > SAVE_MAX) return { error: 'bad' };
  const cur = +(await db(['GET', 'crev:' + m.acc])) || 0;
  if (!m.force && (+m.rev || 0) !== cur) return { conflict: true, rev: cur };
  const rev = cur + 1;
  await dbMany([['SET', 'csav:' + m.acc, data], ['SET', 'crev:' + m.acc, String(rev)], ['SET', 'cat:' + m.acc, String(Date.now())]]);
  return { ok: true, rev };
}
async function cloudGet(m) {
  if (!(await cAuth(m.acc, m.s))) return { error: 'auth' };
  const [rev, at] = await db(['MGET', 'crev:' + m.acc, 'cat:' + m.acc]);
  const r = +rev || 0;
  if (m.rev !== undefined && +m.rev === r) return { same: true, rev: r };
  const data = r ? await db(['GET', 'csav:' + m.acc]) : null;
  return { rev: r, at: +at || 0, data };
}
async function cloudDel(m) {
  if (!(await cAuth(m.acc, m.s))) return { error: 'auth' };
  await db(['DEL', 'cacc:' + m.acc, 'csav:' + m.acc, 'crev:' + m.acc, 'cat:' + m.acc]);
  return { ok: true };
}
async function cloudLink(m) {
  if (!(await cAuth(m.acc, m.s))) return { error: 'auth' };
  for (let n = 0; n < 20; n++) {
    let c = ''; for (let i = 0; i < 6; i++) c += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
    if ((await db(['SET', 'clink:' + c, JSON.stringify({ acc: m.acc, s: m.s }), 'EX', 900, 'NX'])) === 'OK') return { code: c, ttl: XFER_TTL_MS };
  }
  return { error: 'busy' };
}
async function cloudJoin(m) {
  const c = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const v = await db(['GET', 'clink:' + c]);
  if (!v) return { error: 'not found' };
  await db(['DEL', 'clink:' + c]);
  return JSON.parse(v);
}
const DATA_OPS = { lbput: lbPut, lbget: lbGet, lbdel: lbDel, cdel: cloudDel, cnew: cloudNew, cput: cloudPut, cget: cloudGet, clink: cloudLink, cjoin: cloudJoin };

const rooms = new Map(); // code -> {code, created, touched, players: [{id, token, name, ws, seat, leftAt}], log: []}

function makeCode() {
  for (let n = 0; n < 50; n++) {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
  throw new Error('no code');
}
const rid = () => Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 10);
const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); };
const others = (room, me) => room.players.filter(p => p !== me);
const publicPlayers = room => room.players.map(p => ({ seat: p.seat, name: p.name, online: !!(p.ws && p.ws.readyState === 1) }));
const hostOf = room => room.players.reduce((a, p) => (!a || p.seat < a.seat ? p : a), null);
// Liste des salons pour l'écran « Jouer en ligne » : le code n'est donné que pour les salons publics
function roomList() {
  const out = [];
  for (const r of rooms.values()) {
    if (!r.players.some(p => p.ws && p.ws.readyState === 1)) continue;
    const h = hostOf(r), m = r.meta || {};
    out.push({ code: m.pub ? r.code : null, pub: !!m.pub, host: h ? h.name : '?', n: r.players.length, on: r.players.filter(p => p.ws && p.ws.readyState === 1).length, cap: m.cap || 2, game: m.game || 'radar', started: !!m.started, created: r.created });
  }
  return out.sort((a, b) => (b.pub - a.pub) || (a.started - b.started) || (b.created - a.created)).slice(0, 30).map(({ created, ...x }) => x);
}
function broadcastPresence(room) { room.players.forEach(p => send(p.ws, { t: 'presence', players: publicPlayers(room) })); }

const server = http.createServer((req, res) => {
  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET,POST,OPTIONS', 'access-control-allow-headers': 'content-type', 'cache-control': 'no-store' };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }
  if (req.url === '/xfer' && req.method === 'POST') {
    let body = '', big = false;
    req.on('data', d => { if (big) return; body += d; if (body.length > XFER_MAX_BYTES) { big = true; res.writeHead(413, cors); res.end('{"error":"too big"}'); req.destroy(); } });
    req.on('end', () => {
      if (big) return;
      if (!/^(FQZ\d-|FQSAVE\d\.)/.test(body)) { res.writeHead(400, { ...cors, 'content-type': 'application/json' }); res.end('{"error":"bad"}'); return; }
      if (xfers.size > 5000) { res.writeHead(503, cors); res.end('{"error":"busy"}'); return; }
      const code = makeXferCode();
      xfers.set(code, { data: body, at: Date.now() });
      res.writeHead(200, { ...cors, 'content-type': 'application/json' });
      res.end(JSON.stringify({ code, ttl: XFER_TTL_MS }));
    });
    return;
  }
  const xm = req.url.match(/^\/xfer\/([A-Za-z0-9]{6})$/);
  if (xm && req.method === 'GET') {
    const k = xm[1].toUpperCase(), x = xfers.get(k);
    if (!x || Date.now() - x.at > XFER_TTL_MS) { xfers.delete(k); res.writeHead(404, { ...cors, 'content-type': 'application/json' }); res.end('{"error":"not found"}'); return; }
    xfers.delete(k);
    res.writeHead(200, { ...cors, 'content-type': 'text/plain; charset=utf-8' });
    res.end(x.data);
    return;
  }
  if (req.url === '/rooms') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ rooms: roomList() }));
    return;
  }
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, uptime: Math.round(process.uptime()), db: DB_ON ? 'upstash' : 'memoire' }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Serveur F-QUIZ en ligne.');
});

const wss = new WebSocketServer({ server, maxPayload: XFER_MAX_BYTES + 4096 });

wss.on('connection', (ws) => {
  let room = null, me = null, bucket = [];
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    const now = Date.now();
    bucket = bucket.filter(t => now - t < 10000); bucket.push(now);
    if (bucket.length > MAX_MSGS_PER_10S) { send(ws, { t: 'error', code: 'flood', msg: 'Trop de messages' }); return; }
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (raw.length > MAX_MSG_BYTES && m.t !== 'xput' && m.t !== 'cput') return;
    if (DATA_OPS[m.t]) {
      DATA_OPS[m.t](m).then(r => send(ws, Object.assign({ t: m.t, id: m.id }, r)), e => { console.error(m.t, e.message); send(ws, { t: m.t, id: m.id, error: 'server' }); });
      return;
    }

    switch (m.t) {
      // Même service que GET /rooms et /xfer, mais par WebSocket : pour les appareils (certains iPhone/iPad) qui bloquent les requêtes HTTP depuis un fichier local
      case 'rooms': send(ws, { t: 'rooms', id: m.id, rooms: roomList() }); break;
      case 'xput': {
        const data = String(m.data || '');
        if (!/^(FQZ\d-|FQSAVE\d\.)/.test(data) || data.length > XFER_MAX_BYTES) { send(ws, { t: 'xput', id: m.id, error: 'bad' }); break; }
        if (xfers.size > 5000) { send(ws, { t: 'xput', id: m.id, error: 'busy' }); break; }
        const code = makeXferCode(); xfers.set(code, { data, at: now });
        send(ws, { t: 'xput', id: m.id, code, ttl: XFER_TTL_MS });
        break;
      }
      case 'xget': {
        const k = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, ''), x = xfers.get(k);
        if (!x || now - x.at > XFER_TTL_MS) { xfers.delete(k); send(ws, { t: 'xget', id: m.id, error: 'not found' }); break; }
        xfers.delete(k); send(ws, { t: 'xget', id: m.id, data: x.data });
        break;
      }
      case 'create': {
        const code = makeCode();
        room = { code, created: now, touched: now, players: [], log: [], meta: { pub: false, game: 'radar', cap: 2, started: false } };
        me = { id: rid(), token: rid(), name: cleanName(m.name || 'Joueur 1'), ws, seat: 1, leftAt: null };
        room.players.push(me); rooms.set(code, room);
        send(ws, { t: 'joined', code, seat: 1, token: me.token, players: publicPlayers(room) });
        break;
      }
      case 'join': {
        const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        const r = rooms.get(code);
        if (!r) { send(ws, { t: 'error', code: 'no-room', msg: 'Aucun salon avec ce code' }); return; }
        // Reprise de place après une coupure (même jeton)
        const back = m.token && r.players.find(p => p.token === m.token);
        if (back) {
          if (back.ws && back.ws !== ws) try { back.ws.close(4000, 'replaced'); } catch (e) {}
          back.ws = ws; back.leftAt = null; room = r; me = back; r.touched = now;
          send(ws, { t: 'joined', code, seat: me.seat, token: me.token, players: publicPlayers(r), resumed: true, log: r.log.slice(-200) });
          broadcastPresence(r);
          return;
        }
        // Entrée depuis la liste publique : le salon doit être ouvert, pas commencé, avec une place libre
        if (m.pub) {
          const mt = r.meta || {};
          if (!mt.pub) { send(ws, { t: 'error', code: 'no-room', msg: 'Ce salon n’est plus public' }); return; }
          if (mt.started) { send(ws, { t: 'error', code: 'full', msg: 'La partie a déjà commencé' }); return; }
          if (r.players.length >= (mt.cap || MAX_PLAYERS)) { send(ws, { t: 'error', code: 'full', msg: 'Ce salon est déjà complet' }); return; }
        }
        if (r.players.length >= MAX_PLAYERS) { send(ws, { t: 'error', code: 'full', msg: 'Ce salon est déjà complet' }); return; }
        let seat = 1; while (r.players.some(p => p.seat === seat)) seat++;
        room = r; me = { id: rid(), token: rid(), name: cleanName(m.name || ('Joueur ' + seat)), ws, seat, leftAt: null };
        r.players.push(me); r.players.sort((x, y) => x.seat - y.seat); r.touched = now;
        send(ws, { t: 'joined', code, seat, token: me.token, players: publicPlayers(r) });
        broadcastPresence(r);
        break;
      }
      case 'msg': {
        // Message de jeu relayé à l'adversaire (contenu libre, défini par le client) ; gardé pour les reprises
        if (!room || !me) return;
        room.touched = now;
        const out = { t: 'msg', from: me.seat, seq: room.log.length + 1, body: m.body };
        room.log.push(out); if (room.log.length > 500) room.log.shift();
        others(room, me).forEach(p => send(p.ws, out));
        send(ws, { t: 'ack', seq: out.seq, ref: m.ref });
        break;
      }
      case 'meta': {
        // Réglages du salon envoyés par l'hôte (plus petit siège) : public / jeu / places / partie lancée
        if (!room || !me || hostOf(room) !== me) return;
        room.meta = {
          pub: !!m.pub,
          game: String(m.game || 'radar').slice(0, 12),
          cap: Math.max(2, Math.min(MAX_PLAYERS, +m.cap || 2)),
          started: !!m.started
        };
        room.touched = now;
        break;
      }
      case 'ping': send(ws, { t: 'pong', at: m.at }); break;
      case 'leave': {
        if (!room || !me) return;
        room.players = room.players.filter(p => p !== me);
        others(room, me).forEach(p => send(p.ws, { t: 'left', seat: me.seat }));
        if (!room.players.length) rooms.delete(room.code); else broadcastPresence(room);
        room = null; me = null;
        break;
      }
    }
  });

  ws.on('close', () => {
    if (room && me && me.ws === ws) { me.leftAt = Date.now(); broadcastPresence(room); }
  });
});

// Battement de cœur (détecte les connexions mortes) + ménage des salons
setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; try { ws.ping(); } catch (e) {} });
  const now = Date.now();
  for (const [code, r] of rooms) {
    const gone = r.players.filter(p => p.leftAt && now - p.leftAt > RECONNECT_GRACE_MS);
    if (gone.length) {
      r.players = r.players.filter(p => !gone.includes(p));
      // les joueurs restants voient tout de suite que la place est libérée
      gone.forEach(g => r.players.forEach(p => send(p.ws, { t: 'left', seat: g.seat })));
      if (r.players.length) broadcastPresence(r);
    }
    if (!r.players.length || now - r.touched > ROOM_TTL_MS) rooms.delete(code);
  }
}, 20000);

server.listen(PORT, () => console.log(`F-QUIZ serveur prêt sur le port ${PORT} · données : ${DB_ON ? 'Upstash' : 'en mémoire'}`));
