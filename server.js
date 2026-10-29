'use strict';
/**
 * Aviator game server (single instance).
 * Endpoints used by index.html:
 *   GET  /api/game/state      GET /api/game/events (SSE)
 *   POST /api/game/bet        POST /api/game/cancel       POST /api/game/cashout
 *   GET  /api/user/balance    POST /api/user/set-balance (ignored on purpose)
 *
 * Balance lives in Firebase RTDB: users/{uid}/balance (changed ONLY here, with atomic transactions).
 */
const http = require('http');
const crypto = require('crypto');

// ---------- Config (environment variables) ----------
const PORT = Number(process.env.PORT || 3000);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map((s) => s.trim()).filter(Boolean);
const DATABASE_URL = process.env.FIREBASE_DATABASE_URL || 'https://np-trunament-bd-default-rtdb.firebaseio.com';
const SERVICE_ACCOUNT = process.env.FIREBASE_SERVICE_ACCOUNT || ''; // full JSON text of the service account key
const REQUIRE_AUTH = process.env.REQUIRE_AUTH === 'true';           // require a valid Firebase ID token
const COUNTDOWN_MS = Number(process.env.COUNTDOWN_MS || 10000);
const CRASHED_MS = Number(process.env.CRASHED_MS || 3500);
const GROWTH = Number(process.env.GROWTH || 0.085);                 // multiplier = e^(GROWTH * seconds)
const HOUSE_EDGE = Number(process.env.HOUSE_EDGE || 0.03);
const MIN_BET = 10, MAX_BET = 10000;
const MAX_PAYOUT = Number(process.env.MAX_PAYOUT || 200000);        // max win for a single bet
const MAX_CRASH = 1000;

// ---------- Balance store: Firebase RTDB (production) or memory (DEV only) ----------
let admin = null, db = null, devMode = false;
const devBalances = new Map();
if (SERVICE_ACCOUNT) {
  admin = require('firebase-admin');
  admin.initializeApp({ credential: admin.credential.cert(JSON.parse(SERVICE_ACCOUNT)), databaseURL: DATABASE_URL });
  db = admin.database();
  console.log('Firebase connected');
} else if (process.env.NODE_ENV !== 'production') {
  devMode = true;
  console.warn('DEV MODE: no FIREBASE_SERVICE_ACCOUNT, using in-memory balances (start = 1000). Do NOT use in production.');
} else {
  console.error('FIREBASE_SERVICE_ACCOUNT is required in production'); process.exit(1);
}

const round2 = (n) => Math.round(n * 100) / 100;

async function getBalance(uid) {
  if (devMode) { if (!devBalances.has(uid)) devBalances.set(uid, 1000); return devBalances.get(uid); }
  const snap = await db.ref(`users/${uid}/balance`).once('value');
  return Number(snap.val() || 0);
}
// Atomic change. delta<0 fails (returns null) if the balance is not enough.
async function changeBalance(uid, delta) {
  if (devMode) {
    const cur = await getBalance(uid);
    if (cur + delta < 0) return null;
    const nv = round2(cur + delta); devBalances.set(uid, nv); return nv;
  }
  const res = await db.ref(`users/${uid}/balance`).transaction((cur) => {
    const prev = Number(cur || 0);
    if (prev + delta < 0) return; // abort
    return round2(prev + delta);
  });
  return res.committed ? Number(res.snapshot.val()) : null;
}
function logTx(entry) {
  if (devMode) return;
  db.ref('game_transactions').push({ ...entry, time: Date.now() }).catch(() => {});
}

// ---------- Game state ----------
const game = {
  status: 'WAITING', roundId: '', roundNo: 0,
  crashPoint: 1, startedAt: 0, flyStart: 0, crashedAt: 0,
  currentMultiplier: 1, history: [],
  bets: new Map(),            // `${uid}:${panel}` -> { uid, panel, amount, state: 'pending'|'active'|'cashed' }
  keys: new Map()             // idempotencyKey -> response
};

function newCrashPoint() {
  const r = crypto.randomInt(0, 2 ** 32) / 2 ** 32;
  const c = Math.floor(((1 - HOUSE_EDGE) / (1 - r)) * 100) / 100;
  return Math.max(1, Math.min(MAX_CRASH, c));
}
function multiplierAt(ms) { return Math.max(1, Math.floor(Math.exp(GROWTH * (ms / 1000)) * 100) / 100); }

function startWaiting() {
  game.roundNo += 1;
  game.roundId = `rnd_${Date.now()}_${game.roundNo}`;
  game.status = 'WAITING';
  game.startedAt = Date.now();
  game.currentMultiplier = 1;
  game.crashPoint = newCrashPoint();
  game.bets.clear();
  if (game.keys.size > 5000) game.keys.clear();
}
function startFlying() { game.status = 'FLYING'; game.flyStart = Date.now(); }
function crash() {
  game.status = 'CRASHED'; game.crashedAt = Date.now(); game.currentMultiplier = game.crashPoint;
  game.history.unshift(game.crashPoint); if (game.history.length > 20) game.history.pop();
  game.bets.forEach((b) => { if (b.state === 'active') logTx({ type: 'bet_lost', uid: b.uid, amount: b.amount, round: game.roundId, panel: b.panel }); });
}

function publicState() {
  const now = Date.now();
  const total = COUNTDOWN_MS;
  const elapsed = game.status === 'WAITING' ? now - game.startedAt : total;
  return {
    status: game.status, roundId: game.roundId,
    currentMultiplier: game.currentMultiplier,
    countdownProgress: game.status === 'WAITING' ? Math.min(1, elapsed / total) : 1,
    countdownSeconds: game.status === 'WAITING' ? Math.max(0, (total - elapsed) / 1000) : 0,
    history: game.history
  };
}

// ---------- SSE ----------
const clients = new Set();
function broadcast() {
  const data = `data: ${JSON.stringify({ state: publicState() })}\n\n`;
  clients.forEach((res) => { try { res.write(data); } catch (e) { clients.delete(res); } });
}

// Main loop (100 ms)
setInterval(() => {
  const now = Date.now();
  if (game.status === 'WAITING') {
    if (now - game.startedAt >= COUNTDOWN_MS) startFlying();
  } else if (game.status === 'FLYING') {
    const m = multiplierAt(now - game.flyStart);
    if (m >= game.crashPoint) crash(); else game.currentMultiplier = m;
  } else if (game.status === 'CRASHED') {
    if (now - game.crashedAt >= CRASHED_MS) startWaiting();
  }
  broadcast();
}, 100);
startWaiting();

// ---------- Auth ----------
async function resolveUid(req, bodyUid) {
  const h = req.headers.authorization || '';
  if (h.startsWith('Bearer ') && admin) {
    try {
      const dec = await admin.auth().verifyIdToken(h.slice(7));
      if (bodyUid && dec.uid !== bodyUid) return { error: 'UID_MISMATCH' };
      return { uid: dec.uid };
    } catch (e) { return { error: 'BAD_TOKEN' }; }
  }
  if (REQUIRE_AUTH) return { error: 'AUTH_REQUIRED' };
  if (!bodyUid || typeof bodyUid !== 'string' || bodyUid.length > 128 || /[.#$\[\]\/]/.test(bodyUid)) return { error: 'BAD_UID' };
  return { uid: bodyUid };
}

// ---------- Handlers ----------
async function handleBet(req, body) {
  const a = await resolveUid(req, body.uid); if (a.error) return { success: false, error: a.error };
  const { uid } = a, panel = Number(body.panel), amount = Number(body.amount), key = String(body.idempotencyKey || '');
  if (!key) return { success: false, error: 'NO_KEY' };
  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'WAITING') return { success: false, error: 'BETTING_CLOSED' };
  if (![1, 2].includes(panel) || !Number.isFinite(amount) || amount < MIN_BET || amount > MAX_BET) return { success: false, error: 'BAD_INPUT' };
  const bk = `${uid}:${panel}`;
  if (game.bets.has(bk)) return { success: false, error: 'ALREADY_PLACED' };
  const roundId = game.roundId;
  game.bets.set(bk, { uid, panel, amount, state: 'pending' });           // reserve synchronously (no double bet)
  const bal = await changeBalance(uid, -amount);
  const b = game.bets.get(bk);
  if (bal === null) { game.bets.delete(bk); return { success: false, error: 'INSUFFICIENT_BALANCE' }; }
  if (!b || game.roundId !== roundId) { await changeBalance(uid, amount); return { success: false, error: 'ROUND_CHANGED' }; }
  b.state = 'active';
  logTx({ type: 'bet', uid, amount, round: roundId, panel });
  const res = { success: true, balance: bal };
  game.keys.set(key, res);
  return res;
}

async function handleCancel(req, body) {
  const a = await resolveUid(req, body.uid); if (a.error) return { success: false, error: a.error };
  const { uid } = a, panel = Number(body.panel), key = String(body.idempotencyKey || '');
  if (!key) return { success: false, error: 'NO_KEY' };
  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'WAITING') return { success: false, error: 'CANCEL_CLOSED' };
  const bk = `${uid}:${panel}`, b = game.bets.get(bk);
  if (!b || b.state !== 'active') return { success: false, error: 'NO_ACTIVE_BET' };
  b.state = 'cancelling';                                                  // block cashout/double cancel
  const roundId = game.roundId, amount = b.amount;
  const bal = await changeBalance(uid, amount);                           // refund the amount that was really charged
  if (bal === null) { b.state = 'active'; return { success: false, error: 'REFUND_FAILED' }; }
  game.bets.delete(bk);
  logTx({ type: 'bet_cancel', uid, amount, round: roundId, panel });
  const res = { success: true, balance: bal };
  game.keys.set(key, res);
  return res;
}

async function handleCashout(req, body) {
  const a = await resolveUid(req, body.uid); if (a.error) return { success: false, error: a.error };
  const { uid } = a, panel = Number(body.panel), key = String(body.idempotencyKey || '');
  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'FLYING') return { success: false, error: 'NOT_FLYING' };
  const b = game.bets.get(`${uid}:${panel}`);
  if (!b || b.state !== 'active') return { success: false, error: 'NO_ACTIVE_BET' };
  const m = multiplierAt(Date.now() - game.flyStart);
  if (m >= game.crashPoint) return { success: false, error: 'CRASHED' };
  b.state = 'cashed';                                                      // set before await: prevents double payout
  const win = round2(Math.min(b.amount * m, MAX_PAYOUT));
  const bal = await changeBalance(uid, win);
  if (bal === null) { b.state = 'active'; return { success: false, error: 'PAYOUT_FAILED' }; }
  logTx({ type: 'cashout', uid, amount: b.amount, win, multiplier: m, round: game.roundId, panel });
  const res = { success: true, winAmount: win, multiplier: m, balance: bal };
  game.keys.set(key, res);
  return res;
}

// ---------- HTTP plumbing ----------
function cors(req, res) {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.includes('*')) res.setHeader('Access-Control-Allow-Origin', '*');
  else if (origin && ALLOWED_ORIGINS.includes(origin)) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}
function readJson(req) {
  return new Promise((resolve) => {
    let s = ''; req.on('data', (c) => { s += c; if (s.length > 20000) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(s || '{}')); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}
const send = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };

const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  try {
    if (req.method === 'GET' && url.pathname === '/api/game/state') return send(res, 200, { state: publicState() });
    if (req.method === 'GET' && url.pathname === '/api/game/events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write(`data: ${JSON.stringify({ state: publicState() })}\n\n`);
      clients.add(res); req.on('close', () => clients.delete(res)); return;
    }
    if (req.method === 'GET' && url.pathname === '/api/user/balance') {
      const a = await resolveUid(req, url.searchParams.get('uid')); if (a.error) return send(res, 401, { success: false, error: a.error });
      return send(res, 200, { success: true, balance: await getBalance(a.uid) });
    }
    if (req.method === 'POST' && url.pathname === '/api/user/set-balance') return send(res, 200, { success: true, ignored: true }); // clients can never set balance
    if (req.method === 'POST' && url.pathname === '/api/game/bet') return send(res, 200, await handleBet(req, await readJson(req)));
    if (req.method === 'POST' && url.pathname === '/api/game/cancel') return send(res, 200, await handleCancel(req, await readJson(req)));
    if (req.method === 'POST' && url.pathname === '/api/game/cashout') return send(res, 200, await handleCashout(req, await readJson(req)));
    if (url.pathname === '/' || url.pathname === '/health') return send(res, 200, { ok: true });
    send(res, 404, { error: 'NOT_FOUND' });
  } catch (e) { console.error(e); send(res, 500, { success: false, error: 'SERVER_ERROR' }); }
});
server.listen(PORT, () => console.log('Aviator server on :' + PORT));
