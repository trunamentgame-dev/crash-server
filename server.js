'use strict';
/**
 * Aviator authoritative game server (single instance).
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
// Small table: 1..SMALL_TABLE_MAX real bettors in a round -> house edge SMALL_TABLE_EDGE (0.05 = about 95% average return), no payout cap.
const SMALL_TABLE_MAX = Number(process.env.SMALL_TABLE_MAX || 5);
const SMALL_TABLE_EDGE = Number(process.env.SMALL_TABLE_EDGE || 0.05);
// Empty round (nobody bet): house edge EMPTY_TABLE_EDGE (0.03 = 3%).
const EMPTY_TABLE_EDGE = Number(process.env.EMPTY_TABLE_EDGE || 0.03);
// Big table (more than SMALL_TABLE_MAX bettors): when the total already paid out in the round reaches this % of the round's total bets,
// the plane crashes at once. 0 = off.
const PAYOUT_CAP_PERCENT = Number(process.env.PAYOUT_CAP_PERCENT || 80);

// ---------- Rate limits (per IP, per minute) ----------
const RATE_GENERAL = Number(process.env.RATE_GENERAL || 120);       // normal requests (state, balance ...)
const RATE_ACTION = Number(process.env.RATE_ACTION || 60);          // bet / cancel / cashout
const MAX_SSE_PER_IP = Number(process.env.MAX_SSE_PER_IP || 5);     // live connections (/api/game/events) at once
const RATE_WINDOW_MS = 60000;

// ---------- Balance store: Firebase RTDB (production) or memory (DEV only) ----------
let admin = null, db = null, devMode = false;
const devBalances = new Map();
if (SERVICE_ACCOUNT) {
  try {
    admin = require('firebase-admin');
    const saObj = typeof SERVICE_ACCOUNT === 'string' ? JSON.parse(SERVICE_ACCOUNT) : SERVICE_ACCOUNT;
    admin.initializeApp({
      credential: admin.credential.cert(saObj),
      databaseURL: DATABASE_URL
    });
    db = admin.database();
    console.log('Firebase Admin SDK connected successfully');
  } catch (err) {
    console.error('Firebase initialization error:', err);
    process.exit(1);
  }
} else if (process.env.NODE_ENV !== 'production') {
  devMode = true;
  console.warn('DEV MODE: no FIREBASE_SERVICE_ACCOUNT, using in-memory balances (start = 1000). Do NOT use in production.');
} else {
  console.error('FIREBASE_SERVICE_ACCOUNT is required in production');
  process.exit(1);
}

const round2 = (n) => Math.round(n * 100) / 100;

async function getBalance(uid) {
  if (devMode) {
    if (!devBalances.has(uid)) devBalances.set(uid, 1000);
    return devBalances.get(uid);
  }
  try {
    const snap = await db.ref(`users/${uid}/balance`).once('value');
    const val = snap.val();
    return val !== null && !isNaN(val) ? Number(val) : 0;
  } catch(e) {
    console.error('getBalance error for uid:', uid, e);
    return 0;
  }
}

// Atomic balance change. delta<0 fails (returns null) if the balance is not enough.
async function changeBalance(uid, delta) {
  if (devMode) {
    const cur = await getBalance(uid);
    if (cur + delta < 0) return null;
    const nv = round2(cur + delta);
    devBalances.set(uid, nv);
    return nv;
  }

  // Pre-check the balance so missing or negative balance never passes
  if (delta < 0) {
    const real = await getBalance(uid);
    if (real + delta < 0) return null;
  }

  const res = await db.ref(`users/${uid}/balance`).transaction((cur) => {
    // When Firebase first calls this with null before fetching, don't abort immediately
    if (cur === null && delta < 0) return 0;
    const prev = Number(cur || 0);
    if (prev + delta < 0) return; // abort: insufficient balance
    return round2(prev + delta);
  });

  return res.committed ? Number(res.snapshot.val()) : null;
}

function logRound(entry) {
  if (devMode || !db) return;
  db.ref('game_rounds').push({ ...entry, time: Date.now() }).catch(() => {});
}

function logTx(entry) {
  if (devMode || !db) return;
  db.ref('game_transactions').push({ ...entry, time: Date.now() }).catch(() => {});
}

// ---------- Game state ----------
const game = {
  status: 'WAITING', roundId: '', roundNo: 0,
  crashPoint: 1, startedAt: 0, flyStart: 0, crashedAt: 0,
  currentMultiplier: 1, history: [],
  pool: 0, paid: 0, bettors: 0, mode: 'EMPTY', crashReason: 'RANDOM',
  bets: new Map(),
  keys: new Map()
};

function newCrashPoint(edge = HOUSE_EDGE) {
  const r = crypto.randomInt(0, 2 ** 32) / 2 ** 32;
  const c = Math.floor(((1 - edge) / (1 - r)) * 100) / 100;
  return Math.max(1, Math.min(MAX_CRASH, c));
}

function multiplierAt(ms) {
  return Math.max(1, Math.floor(Math.exp(GROWTH * (ms / 1000)) * 100) / 100);
}

function startWaiting() {
  game.roundNo += 1;
  game.roundId = `rnd_${Date.now()}_${game.roundNo}`;
  game.status = 'WAITING';
  game.startedAt = Date.now();
  game.currentMultiplier = 1;
  game.crashPoint = newCrashPoint();
  game.bets.clear();
  game.pool = 0; game.paid = 0; game.bettors = 0; game.mode = 'EMPTY'; game.crashReason = 'RANDOM';
  if (game.keys.size > 5000) game.keys.clear();
}

function startFlying() {
  const uids = new Set();
  game.bets.forEach((b) => { if (b.state === 'active' || b.state === 'pending') uids.add(b.uid); });
  game.bettors = uids.size;
  if (game.bettors === 0) { game.mode = 'EMPTY'; game.crashPoint = newCrashPoint(EMPTY_TABLE_EDGE); }
  else if (game.bettors <= SMALL_TABLE_MAX) { game.mode = 'SMALL'; game.crashPoint = newCrashPoint(SMALL_TABLE_EDGE); }
  else game.mode = 'LARGE';
  game.status = 'FLYING';
  game.flyStart = Date.now();
}

function crash() {
  game.status = 'CRASHED';
  game.crashedAt = Date.now();
  game.currentMultiplier = game.crashPoint;
  game.history.unshift(game.crashPoint);
  if (game.history.length > 20) game.history.pop();
  game.bets.forEach((b) => {
    if (b.state === 'active') {
      logTx({ type: 'bet_lost', game: 'Aviator', uid: b.uid, amount: b.amount, round: game.roundId, panel: b.panel });
    }
  });
  const info = { round: game.roundId, mode: game.mode, bettors: game.bettors, crashPoint: game.crashPoint, pool: game.pool, paid: game.paid, reason: game.crashReason };
  console.log('CRASH', JSON.stringify(info));
  logRound(info);
}

function forceCrash(reason) {
  if (game.status !== 'FLYING') return;
  game.crashPoint = Math.max(1, multiplierAt(Date.now() - game.flyStart));
  game.crashReason = reason;
  crash();
}

function publicState() {
  const now = Date.now();
  const total = COUNTDOWN_MS;
  const elapsed = game.status === 'WAITING' ? now - game.startedAt : total;
  return {
    status: game.status,
    roundId: game.roundId,
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

// ---------- Robust Auth Resolution (Chrome & Mobile WebView Safe) ----------
async function resolveUid(req, bodyUid) {
  const h = req.headers.authorization || '';

  // 1. If Bearer token is provided and Admin SDK is loaded, verify it
  if (h.startsWith('Bearer ') && admin) {
    const token = h.slice(7).trim();
    try {
      const dec = await admin.auth().verifyIdToken(token);
      if (bodyUid && dec.uid !== bodyUid) return { error: 'UID_MISMATCH' };
      return { uid: dec.uid };
    } catch (e) {
      // If REQUIRE_AUTH is true, strict token verification is required
      if (REQUIRE_AUTH) {
        return { error: 'BAD_TOKEN' };
      }
      // If REQUIRE_AUTH is false, token might be expired in long-running mobile WebView session.
      // Gracefully fall back to bodyUid if valid, so mobile users aren't abruptly kicked out!
      if (bodyUid && typeof bodyUid === 'string' && bodyUid.length <= 128 && !/[.#$\[\]\/]/.test(bodyUid)) {
        return { uid: bodyUid };
      }
      return { error: 'BAD_TOKEN' };
    }
  }

  // 2. Strict auth enforcement when configured
  if (REQUIRE_AUTH) return { error: 'AUTH_REQUIRED' };

  // 3. Fallback to bodyUid / query UID (for WebView / standard platform integration)
  if (!bodyUid || typeof bodyUid !== 'string' || bodyUid.length > 128 || /[.#$\[\]\/]/.test(bodyUid)) {
    return { error: 'BAD_UID' };
  }
  return { uid: bodyUid.trim() };
}

// ---------- Handlers ----------
async function handleBet(req, body) {
  const a = await resolveUid(req, body.uid);
  if (a.error) return { success: false, error: a.error };

  const { uid } = a;
  const panel = Number(body.panel);
  const amount = Number(body.amount);
  const key = String(body.idempotencyKey || '');

  if (!key) return { success: false, error: 'NO_KEY' };
  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'WAITING') return { success: false, error: 'BETTING_CLOSED' };
  if (![1, 2].includes(panel) || !Number.isFinite(amount) || amount < MIN_BET || amount > MAX_BET) {
    return { success: false, error: 'BAD_INPUT' };
  }

  const bk = `${uid}:${panel}`;
  if (game.bets.has(bk)) return { success: false, error: 'ALREADY_PLACED' };

  const roundId = game.roundId;
  game.bets.set(bk, { uid, panel, amount, state: 'pending' });

  const bal = await changeBalance(uid, -amount);
  const b = game.bets.get(bk);

  if (bal === null) {
    game.bets.delete(bk);
    return { success: false, error: 'INSUFFICIENT_BALANCE' };
  }

  if (!b || game.roundId !== roundId) {
    await changeBalance(uid, amount); // auto refund if round shifted
    return { success: false, error: 'ROUND_CHANGED' };
  }

  b.state = 'active';
  game.pool = round2(game.pool + amount);
  logTx({ type: 'bet', game: 'Aviator', uid, amount, balance: bal, round: roundId, panel });

  const res = { success: true, balance: bal };
  game.keys.set(key, res);
  return res;
}

async function handleCancel(req, body) {
  const a = await resolveUid(req, body.uid);
  if (a.error) return { success: false, error: a.error };

  const { uid } = a;
  const panel = Number(body.panel);
  const key = String(body.idempotencyKey || '');

  if (!key) return { success: false, error: 'NO_KEY' };
  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'WAITING') return { success: false, error: 'CANCEL_CLOSED' };

  const bk = `${uid}:${panel}`;
  const b = game.bets.get(bk);
  if (!b || b.state !== 'active') return { success: false, error: 'NO_ACTIVE_BET' };

  b.state = 'cancelling';
  const roundId = game.roundId;
  const amount = b.amount;
  const bal = await changeBalance(uid, amount);

  if (bal === null) {
    b.state = 'active';
    return { success: false, error: 'REFUND_FAILED' };
  }

  game.bets.delete(bk);
  game.pool = round2(Math.max(0, game.pool - amount));
  logTx({ type: 'bet_cancel', game: 'Aviator', uid, amount, balance: bal, round: roundId, panel });

  const res = { success: true, balance: bal };
  game.keys.set(key, res);
  return res;
}

async function handleCashout(req, body) {
  const a = await resolveUid(req, body.uid);
  if (a.error) return { success: false, error: a.error };

  const { uid } = a;
  const panel = Number(body.panel);
  const key = String(body.idempotencyKey || '');

  if (game.keys.has(key)) return game.keys.get(key);
  if (game.status !== 'FLYING') return { success: false, error: 'NOT_FLYING' };

  const b = game.bets.get(`${uid}:${panel}`);
  if (!b || b.state !== 'active') return { success: false, error: 'NO_ACTIVE_BET' };

  const m = multiplierAt(Date.now() - game.flyStart);
  if (m >= game.crashPoint) return { success: false, error: 'CRASHED' };

  b.state = 'cashed';
  const win = round2(Math.min(b.amount * m, MAX_PAYOUT));
  const bal = await changeBalance(uid, win);

  if (bal === null) {
    b.state = 'active';
    return { success: false, error: 'PAYOUT_FAILED' };
  }

  logTx({ type: 'cashout', game: 'Aviator', uid, amount: b.amount, win, multiplier: m, balance: bal, round: game.roundId, panel });
  const res = { success: true, winAmount: win, multiplier: m, balance: bal };
  game.keys.set(key, res);
  game.paid = round2(game.paid + win);

  if (PAYOUT_CAP_PERCENT > 0 && game.mode === 'LARGE' && game.status === 'FLYING' && game.paid >= game.pool * PAYOUT_CAP_PERCENT / 100) {
    forceCrash('PAYOUT_CAP');
  }

  return res;
}

// ---------- HTTP Plumbing & Enhanced Mobile CORS ----------
function clientIp(req) {
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return xf || req.socket.remoteAddress || 'unknown';
}

const rateGeneral = new Map(), rateAction = new Map(), sseCount = new Map();

function allow(map, ip, limit) {
  const now = Date.now();
  let e = map.get(ip);
  if (!e || now >= e.reset) { e = { n: 0, reset: now + RATE_WINDOW_MS }; map.set(ip, e); }
  e.n += 1;
  return e.n <= limit;
}

setInterval(() => {
  const now = Date.now();
  [rateGeneral, rateAction].forEach((m) => m.forEach((e, ip) => { if (now >= e.reset) m.delete(ip); }));
}, 60000).unref();

const tooMany = (res) => {
  res.setHeader('Retry-After', '10');
  send(res, 429, { success: false, error: 'RATE_LIMITED' });
};

// Mobile WebView-safe CORS
function cors(req, res) {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.includes('*') || !origin || origin === 'null' || origin.startsWith('file://') || origin.startsWith('capacitor://') || origin.startsWith('http://localhost')) {
    res.setHeader('Access-Control-Allow-Origin', origin && origin !== 'null' ? origin : '*');
  } else if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function readJson(req) {
  return new Promise((resolve) => {
    let s = '';
    req.on('data', (c) => { s += c; if (s.length > 20000) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(s || '{}')); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

const send = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  const url = new URL(req.url, 'http://x');
  const ip = clientIp(req);
  const isEvents = req.method === 'GET' && url.pathname === '/api/game/events';
  const isAction = req.method === 'POST' && ['/api/game/bet', '/api/game/cancel', '/api/game/cashout'].includes(url.pathname);

  if (url.pathname !== '/' && url.pathname !== '/health' && !isEvents) {
    if (!allow(rateGeneral, ip, RATE_GENERAL)) return tooMany(res);
    if (isAction && !allow(rateAction, ip, RATE_ACTION)) return tooMany(res);
  }

  try {
    if (req.method === 'GET' && url.pathname === '/api/game/state') {
      return send(res, 200, { state: publicState() });
    }

    if (isEvents) {
      if ((sseCount.get(ip) || 0) >= MAX_SSE_PER_IP) return tooMany(res);
      sseCount.set(ip, (sseCount.get(ip) || 0) + 1);
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        const n = (sseCount.get(ip) || 1) - 1;
        if (n <= 0) sseCount.delete(ip); else sseCount.set(ip, n);
        clients.delete(res);
      };
      req.on('close', release);
      res.on('close', release);
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no'
      });
      res.write(`data: ${JSON.stringify({ state: publicState() })}\n\n`);
      clients.add(res);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/user/balance') {
      const a = await resolveUid(req, url.searchParams.get('uid'));
      if (a.error) return send(res, 401, { success: false, error: a.error });
      return send(res, 200, { success: true, balance: await getBalance(a.uid) });
    }

    if (req.method === 'POST' && url.pathname === '/api/user/set-balance') {
      return send(res, 200, { success: true, ignored: true });
    }

    if (req.method === 'POST' && url.pathname === '/api/game/bet') {
      return send(res, 200, await handleBet(req, await readJson(req)));
    }

    if (req.method === 'POST' && url.pathname === '/api/game/cancel') {
      return send(res, 200, await handleCancel(req, await readJson(req)));
    }

    if (req.method === 'POST' && url.pathname === '/api/game/cashout') {
      return send(res, 200, await handleCashout(req, await readJson(req)));
    }

    if (url.pathname === '/' || url.pathname === '/health') {
      return send(res, 200, { ok: true, status: 'RUNNING' });
    }

    send(res, 404, { error: 'NOT_FOUND' });
  } catch (e) {
    console.error('Server unhandled error:', e);
    send(res, 500, { success: false, error: 'SERVER_ERROR' });
  }
});

server.listen(PORT, () => console.log('Aviator server running on port ' + PORT));
