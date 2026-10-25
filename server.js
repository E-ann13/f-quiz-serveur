// F-QUIZ — serveur de salons pour le multijoueur en ligne
// Rôle : relayer les messages entre les joueurs d'un salon (2 pour les duels, jusqu'à 8 pour le Grand Prix). Le serveur ne connaît pas les avions secrets :
// chaque appareil répond lui-même aux questions sur son propre avion.
//
// Démarrage : node server.js   (port : variable PORT, 8787 par défaut)
// Santé : GET /health → {"ok":true,...}

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8787;
const ROOM_TTL_MS = 2 * 60 * 60 * 1000;      // un salon inactif disparaît au bout de 2 h
const RECONNECT_GRACE_MS = 10 * 60 * 1000;   // un joueur déconnecté (appli en arrière-plan, coupure) garde sa place 10 min
const MAX_MSG_BYTES = 64 * 1024;
const MAX_MSGS_PER_10S = 120;
const MAX_PLAYERS = 8;                         // duels à 2, Grand Prix jusqu'à 8
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sans I, O, 0, 1 (confusions)

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
    out.push({ code: m.pub ? r.code : null, pub: !!m.pub, host: h ? h.name : '?', n: r.players.length, cap: m.cap || 2, game: m.game || 'radar', started: !!m.started, created: r.created });
  }
  return out.sort((a, b) => (b.pub - a.pub) || (a.started - b.started) || (b.created - a.created)).slice(0, 30).map(({ created, ...x }) => x);
}
function broadcastPresence(room) { room.players.forEach(p => send(p.ws, { t: 'presence', players: publicPlayers(room) })); }

const server = http.createServer((req, res) => {
  if (req.url === '/rooms') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ rooms: roomList() }));
    return;
  }
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, uptime: Math.round(process.uptime()) }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Serveur F-QUIZ en ligne.');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG_BYTES });

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

    switch (m.t) {
      case 'create': {
        const code = makeCode();
        room = { code, created: now, touched: now, players: [], log: [], meta: { pub: false, game: 'radar', cap: 2, started: false } };
        me = { id: rid(), token: rid(), name: String(m.name || 'Joueur 1').slice(0, 24), ws, seat: 1, leftAt: null };
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
        room = r; me = { id: rid(), token: rid(), name: String(m.name || ('Joueur ' + seat)).slice(0, 24), ws, seat, leftAt: null };
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
    r.players = r.players.filter(p => !(p.leftAt && now - p.leftAt > RECONNECT_GRACE_MS));
    if (!r.players.length || now - r.touched > ROOM_TTL_MS) rooms.delete(code);
  }
}, 20000);

server.listen(PORT, () => console.log(`F-QUIZ serveur prêt sur le port ${PORT}`));
