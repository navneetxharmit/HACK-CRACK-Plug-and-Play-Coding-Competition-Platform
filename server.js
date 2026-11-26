/**
 * HACK & CRACK — Competition Backend (Local SQLite + WebSocket)
 * =============================================================
 * - Express REST API
 * - Local SQLite via built-in node:sqlite (zero config, no password)
 * - WebSocket for real-time leaderboard + admin live feed
 */

const express          = require('express');
const http             = require('http');
const path             = require('path');
const WebSocket        = require('ws');
const cors             = require('cors');
const { DatabaseSync } = require('node:sqlite');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const PORT    = Number(process.env.PORT || 3000);
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'hackcrack.db');

const db = new DatabaseSync(DB_PATH);

function parseJsonCol(val, fallback) {
  if (val === null || val === undefined) return fallback;
  if (typeof val === 'object') return val;
  try { return JSON.parse(val); } catch { return fallback; }
}

function seedDefaultUsers() {
  const insertUser = db.prepare(
    'INSERT INTO users (username, password, role, assignedQuestions, solved) VALUES (?, ?, ?, ?, ?)'
  );
  db.exec('BEGIN');
  try {
    for (let i = 1; i <= 100; i++) {
      const num = String(i).padStart(2, '0');
      insertUser.run(`user${num}`, 'nopassword', 'competitor', '{}', '{}');
    }
    insertUser.run('admin', 'adminhash@2006', 'admin', '{}', '{}');
    db.prepare(
      `INSERT INTO comp_state (state_key, started, ended, start_time)
       VALUES ('state', 0, 0, NULL)
       ON CONFLICT(state_key) DO UPDATE SET started = 0, ended = 0, start_time = NULL`
    ).run();
    db.exec('COMMIT');
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
}

function initDB() {
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA busy_timeout = 5000;');

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'competitor',
      score INTEGER NOT NULL DEFAULT 0,
      login_time TEXT NULL,
      last_active TEXT NULL,
      disqualified TEXT NULL,
      cheat_count INTEGER NOT NULL DEFAULT 0,
      cheat_reason TEXT NULL,
      cheat_flag TEXT NULL,
      locked INTEGER NOT NULL DEFAULT 0,
      spared_once INTEGER NOT NULL DEFAULT 0,
      name TEXT NULL,
      class TEXT NULL,
      roll TEXT NULL,
      phone TEXT NULL,
      email TEXT NULL,
      assignedQuestions TEXT NULL,
      solved TEXT NULL,
      wrongAnswers INTEGER NOT NULL DEFAULT 0,
      sparedOnce INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_users_role_score ON users (role, score);

    CREATE TABLE IF NOT EXISTS submissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL,
      question_id TEXT NOT NULL,
      answer TEXT NULL,
      correct INTEGER NOT NULL DEFAULT 0,
      points INTEGER NOT NULL DEFAULT 0,
      submitted_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_subs_user_q ON submissions (username, question_id);
    CREATE INDEX IF NOT EXISTS idx_subs_correct_user ON submissions (correct, username);
    CREATE INDEX IF NOT EXISTS idx_subs_submitted_at ON submissions (submitted_at);

    CREATE TABLE IF NOT EXISTS comp_state (
      state_key TEXT PRIMARY KEY,
      started INTEGER NOT NULL DEFAULT 0,
      ended INTEGER NOT NULL DEFAULT 0,
      start_time TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS activity (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'info',
      ts TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_activity_ts ON activity (ts);
  `);

  db.prepare(
    `INSERT OR IGNORE INTO comp_state (state_key, started, ended, start_time) VALUES ('state', 0, 0, NULL)`
  ).run();

  const { cnt } = db.prepare('SELECT COUNT(*) AS cnt FROM users').get();
  if (cnt === 0) {
    seedDefaultUsers();
    console.log('✅ Seeded 101 default users into local SQLite');
  }

  console.log(`✅ SQLite Ready (${DB_PATH})`);
}

// ─────────────────────────────────────────
// WEBSOCKET
// ─────────────────────────────────────────
function broadcast(event, payload) {
  const msg = JSON.stringify({ event, payload });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// Ping every 30s to detect dead connections and clean them up
const WS_PING_INTERVAL = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);
wss.on('close', () => clearInterval(WS_PING_INTERVAL));

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  console.log('WS client connected. Total:', wss.clients.size);
  try {
    ws.send(JSON.stringify({ event: 'leaderboard',        payload: getLeaderboard() }));
    ws.send(JSON.stringify({ event: 'competition_status', payload: getCompStatus() }));
    const logs = db.prepare('SELECT message, type, ts FROM activity ORDER BY id DESC LIMIT 50').all();
    ws.send(JSON.stringify({ event: 'activity_log',       payload: logs.reverse() }));
  } catch (e) {}
});

// ─────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────
function getLeaderboard() {
  const rows = db.prepare(`
    SELECT u.username, u.score, u.disqualified, u.name, u.class, u.roll,
           u.login_time, u.last_active,
           COALESCE(s.solved_count, 0) AS solved_count
    FROM users u
    LEFT JOIN (
      SELECT username, COUNT(*) AS solved_count
      FROM submissions
      WHERE correct = 1
      GROUP BY username
    ) s ON s.username = u.username
    WHERE u.role = 'competitor'
    ORDER BY u.score DESC, (u.login_time IS NULL) ASC, u.login_time ASC, (u.last_active IS NULL) ASC, u.last_active ASC
  `).all();

  return rows.map(u => ({
    username:     u.username,
    score:        u.score,
    disqualified: u.disqualified,
    name:         u.name,
    class:        u.class,
    roll:         u.roll,
    login_time:   u.login_time,
    last_active:  u.last_active,
    solved_count: Number(u.solved_count) || 0
  }));
}

function getCompStatus() {
  const s = db.prepare(
    `SELECT started, ended, start_time FROM comp_state WHERE state_key = 'state' LIMIT 1`
  ).get();
  if (!s) {
    db.prepare(
      `INSERT OR IGNORE INTO comp_state (state_key, started, ended, start_time) VALUES ('state', 0, 0, NULL)`
    ).run();
    return { started: 0, ended: 0, start_time: null };
  }
  return { started: s.started, ended: s.ended, start_time: s.start_time };
}

function logActivity(message, type = 'info') {
  try {
    const now = new Date().toISOString();
    db.prepare('INSERT INTO activity (message, type, ts) VALUES (?, ?, ?)').run(message, type, now);
    broadcast('activity', { message, type, ts: now });

    const { cnt } = db.prepare('SELECT COUNT(*) AS cnt FROM activity').get();
    if (cnt > 200) {
      db.prepare(
        `DELETE FROM activity WHERE id NOT IN (SELECT id FROM activity ORDER BY id DESC LIMIT 200)`
      ).run();
    }
  } catch (e) {}
}

function userToRow(u) {
  return {
    username:     u.username,
    password:     u.password,
    score:        u.score,
    login_time:   u.login_time,
    last_active:  u.last_active,
    disqualified: u.disqualified,
    cheat_count:  u.cheat_count,
    cheat_reason: u.cheat_reason,
    cheat_flag:   parseJsonCol(u.cheat_flag, null),
    locked:       u.locked,
    spared_once:  u.spared_once,
    name:         u.name,
    class:        u.class,
    roll:         u.roll,
    phone:        u.phone,
    email:        u.email,
    assignedQuestions: parseJsonCol(u.assignedQuestions, {}),
    solved:       parseJsonCol(u.solved, {}),
    wrongAnswers: u.wrongAnswers,
    sparedOnce:   Boolean(u.sparedOnce),
    personalDetails: (u.name ? { name: u.name, class: u.class, roll: u.roll, phone: u.phone, email: u.email } : null),
    loginTime:    u.login_time
  };
}

// ─────────────────────────────────────────
// SEED / RESET & ROOT
// ─────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/health', (req, res) => res.send('HACK & CRACK Backend is running 🚀'));

app.get('/add-users', (req, res) => {
  try {
    db.exec('DELETE FROM submissions; DELETE FROM activity; DELETE FROM users; DELETE FROM comp_state;');
    seedDefaultUsers();

    broadcast('competition_status', { started: 0, ended: 0 });
    broadcast('leaderboard', []);
    res.json({ success: true, message: '101 users inserted and competition reset!' });
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

// ─────────────────────────────────────────
// LOGIN
// ─────────────────────────────────────────
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (!username) return res.status(400).json({ error: 'Missing username' });

  // Admin — requires password
  if (username === 'admin') {
    if (password === 'adminhash@2006')
      return res.json({ success: true, role: 'admin' });
    return res.status(401).json({ error: 'Invalid admin credentials' });
  }

  try {
    const user = db.prepare(
      `SELECT * FROM users WHERE username = ? AND role = 'competitor' LIMIT 1`
    ).get(username);
    if (!user) return res.status(401).json({ error: 'User not found. Check your username.' });
    if (user.locked) return res.status(403).json({ error: 'Account locked by admin' });

    const comp = getCompStatus();
    if (!comp.started) return res.status(403).json({ error: 'Competition has not started yet' });
    if (comp.ended)    return res.status(403).json({ error: 'Competition has ended' });

    if (user.disqualified === 'quit')
      return res.status(403).json({ error: 'You have already quit the competition. Your score has been saved. Please wait for the admin to reset.' });
    if (user.disqualified === 'gameover')
      return res.status(403).json({ error: 'Your session has ended (time up or 5 wrong answers). Your score has been saved. Please wait for the admin to reset.' });
    if (user.disqualified === 'pending')
      return res.status(403).json({ error: "Your account is under admin review for cheating. Please wait for the admin's decision." });
    if (user.disqualified === 'disqualified' || user.disqualified === 'permanent')
      return res.status(403).json({ error: 'You have been permanently disqualified for cheating. You cannot re-enter.' });

    const now = new Date().toISOString();
    if (!user.login_time) {
      db.prepare('UPDATE users SET last_active = ?, login_time = ? WHERE username = ?').run(now, now, username);
    } else {
      db.prepare('UPDATE users SET last_active = ? WHERE username = ?').run(now, username);
    }

    logActivity(`🔐 ${username} logged in`, 'ok');
    broadcast('user_status', { username, status: 'online', ts: now });
    broadcast('leaderboard', getLeaderboard());

    const fresh = db.prepare('SELECT * FROM users WHERE username = ? LIMIT 1').get(username);
    return res.json({ success: true, role: 'user', user: userToRow(fresh) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────
// SUBMISSION
// ─────────────────────────────────────────
app.post('/api/submit', (req, res) => {
  const { username, questionId, answer, correct, points } = req.body;
  if (!username || !questionId) return res.status(400).json({ error: 'Missing fields' });
  try {
    const user = db.prepare('SELECT * FROM users WHERE username = ? LIMIT 1').get(username);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.disqualified && user.disqualified !== 'gameover')
      return res.status(403).json({ error: 'User disqualified' });

    const already = db.prepare(
      'SELECT id FROM submissions WHERE username = ? AND question_id = ? AND correct = 1 LIMIT 1'
    ).get(username, String(questionId));
    if (already) return res.json({ success: false, error: 'Already solved' });

    const now = new Date().toISOString();
    const isCorrect = correct ? 1 : 0;
    const pts = isCorrect ? Number(points || 0) : 0;

    db.prepare(
      'INSERT INTO submissions (username, question_id, answer, correct, points, submitted_at) VALUES (?, ?, ?, ?, ?, ?)'
    ).run(username, String(questionId), answer ?? null, isCorrect, pts, now);

    if (isCorrect) {
      db.prepare(
        'UPDATE users SET last_active = ?, score = COALESCE(score, 0) + ? WHERE username = ?'
      ).run(now, pts, username);
    } else {
      db.prepare('UPDATE users SET last_active = ? WHERE username = ?').run(now, username);
    }

    const emoji = isCorrect ? '✅' : '❌';
    logActivity(`${emoji} ${username} answered Q${questionId} — ${isCorrect ? `+${pts}pts` : 'wrong'}`, isCorrect ? 'ok' : 'err');
    broadcast('leaderboard', getLeaderboard());

    const fresh = db.prepare('SELECT score FROM users WHERE username = ? LIMIT 1').get(username);
    return res.json({ success: true, newScore: fresh.score });
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

// ─────────────────────────────────────────
// CHEAT
// ─────────────────────────────────────────
app.post('/api/cheat', (req, res) => {
  const { username, reason } = req.body;
  try {
    const user = db.prepare('SELECT * FROM users WHERE username = ? LIMIT 1').get(username);
    if (!user) return res.status(404).json({ error: 'User not found' });
    const newCount = (user.cheat_count || 0) + 1;
    const now = new Date().toISOString();

    if (user.spared_once) {
      db.prepare(
        `UPDATE users SET disqualified = 'disqualified', locked = 1, cheat_count = ?, cheat_reason = ?, score = 0 WHERE username = ?`
      ).run(newCount, reason ?? null, username);
      logActivity(`🔒 2ND OFFENSE AUTO-DISQUALIFIED: ${username} — ${reason}`, 'warn');
      broadcast('user_status', { username, status: 'offline', reason: 'disqualified', ts: now });
      broadcast('leaderboard', getLeaderboard());
      broadcast('cheat_alert', { username, reason, autoDisqualified: true });
      return res.json({ success: true, autoDisqualified: true });
    } else {
      db.prepare(
        `UPDATE users SET disqualified = 'pending', locked = 1, cheat_count = ?, cheat_reason = ?, score = 0 WHERE username = ?`
      ).run(newCount, reason ?? null, username);
      logActivity(`🚨 CHEATING DETECTED (1st offense): ${username} — ${reason}`, 'warn');
      broadcast('user_status', { username, status: 'offline', reason: 'pending', ts: now });
      broadcast('leaderboard', getLeaderboard());
      broadcast('cheat_alert', { username, reason, autoDisqualified: false });
      return res.json({ success: true, autoDisqualified: false });
    }
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

// Admin cheat review
app.post('/api/admin/cheat-review', (req, res) => {
  const { username, action } = req.body;
  try {
    if (action === 'approve') {
      db.prepare(
        `UPDATE users SET disqualified = 'disqualified', locked = 1, score = 0 WHERE username = ?`
      ).run(username);
      logActivity(`🔒 Admin permanently disqualified ${username}`, 'warn');
      broadcast('cheat_decision', { username, decision: 'disqualified' });
    } else {
      const { total } = db.prepare(
        'SELECT COALESCE(SUM(points), 0) AS total FROM submissions WHERE username = ? AND correct = 1'
      ).get(username);
      const restoredScore = Number(total) || 0;
      db.prepare(
        `UPDATE users
         SET disqualified = NULL, locked = 0, cheat_count = 0, cheat_reason = NULL, cheat_flag = NULL,
             spared_once = 1, sparedOnce = 1, score = ?
         WHERE username = ?`
      ).run(restoredScore, username);
      logActivity(`✅ Admin spared ${username} — score restored to ${restoredScore}pts. Next offence = auto-disqualify.`, 'ok');
      broadcast('cheat_decision', { username, decision: 'spared' });
    }
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) {
    res.json({ success: false, error: err.message });
  }
});

// ─────────────────────────────────────────
// ADMIN ROUTES
// ─────────────────────────────────────────
app.get('/api/admin/users', (req, res) => {
  try {
    const users = db.prepare(`SELECT * FROM users WHERE role = 'competitor' ORDER BY score DESC`).all();
    res.json(users.map(userToRow));
  } catch (err) { res.json([]); }
});

app.get('/api/admin/submissions', (req, res) => {
  try {
    const subs = db.prepare(
      'SELECT id, username, question_id, answer, correct, points, submitted_at FROM submissions ORDER BY submitted_at DESC, id DESC LIMIT 200'
    ).all();
    res.json(subs);
  } catch (err) { res.json([]); }
});

app.get('/api/admin/activity', (req, res) => {
  try {
    const logs = db.prepare(
      'SELECT message, type, ts FROM activity ORDER BY ts DESC, id DESC LIMIT 100'
    ).all();
    res.json(logs);
  } catch (err) { res.json([]); }
});

app.post('/api/admin/start', (req, res) => {
  try {
    db.prepare(
      `INSERT INTO comp_state (state_key, started, ended, start_time)
       VALUES ('state', 1, 0, ?)
       ON CONFLICT(state_key) DO UPDATE SET started = 1, ended = 0, start_time = excluded.start_time`
    ).run(new Date().toISOString());
    logActivity('🚀 Competition STARTED by admin', 'ok');
    broadcast('competition_status', getCompStatus());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/admin/end', (req, res) => {
  try {
    db.prepare(`UPDATE comp_state SET ended = 1 WHERE state_key = 'state'`).run();
    db.prepare(`UPDATE users SET locked = 1 WHERE role = 'competitor'`).run();
    logActivity('🏁 Competition ENDED by admin', 'warn');
    broadcast('competition_status', getCompStatus());
    broadcast('competition_ended', { leaderboard: getLeaderboard() });
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/admin/reset', (req, res) => {
  try {
    db.prepare(
      `UPDATE users SET
         score = 0, disqualified = NULL, cheat_count = 0, cheat_reason = NULL, cheat_flag = NULL,
         login_time = NULL, last_active = NULL, locked = 0, spared_once = 0, sparedOnce = 0,
         name = NULL, class = NULL, roll = NULL, phone = NULL, email = NULL,
         assignedQuestions = '{}', solved = '{}', wrongAnswers = 0
       WHERE role = 'competitor'`
    ).run();
    db.exec('DELETE FROM submissions; DELETE FROM activity;');
    db.prepare(
      `INSERT INTO comp_state (state_key, started, ended, start_time)
       VALUES ('state', 0, 0, NULL)
       ON CONFLICT(state_key) DO UPDATE SET started = 0, ended = 0, start_time = NULL`
    ).run();
    broadcast('leaderboard', getLeaderboard());
    broadcast('competition_status', { started: 0, ended: 0 });
    broadcast('force_logout', { reason: 'Competition has been reset by admin. Please wait for the next session.' });
    logActivity('🔄 Competition FULLY RESET by admin — all users logged out, all data cleared', 'warn');
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/admin/unlock-user', (req, res) => {
  const { username } = req.body;
  try {
    db.prepare('UPDATE users SET locked = 0, disqualified = NULL WHERE username = ?').run(username);
    logActivity(`🔓 Admin unlocked ${username}`, 'ok');
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/admin/reset-user', (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'Missing username' });
  try {
    db.prepare(
      `UPDATE users SET
         score = 0, disqualified = NULL, cheat_count = 0, cheat_reason = NULL, cheat_flag = NULL,
         login_time = NULL, last_active = NULL, locked = 0, spared_once = 0, sparedOnce = 0,
         assignedQuestions = '{}', solved = '{}', wrongAnswers = 0
       WHERE username = ?`
    ).run(username);
    db.prepare('DELETE FROM submissions WHERE username = ?').run(username);
    logActivity(`↺ Admin reset user ${username} — all data cleared including cheat history`, 'warn');
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/admin/update-passwords', (req, res) => {
  logActivity('🔑 Password update called (no-op in local SQLite mode)', 'ok');
  res.json({ success: true, updated: 100 });
});

// ─────────────────────────────────────────
// PUBLIC ROUTES
// ─────────────────────────────────────────
app.get('/api/leaderboard', (req, res) => {
  try { res.json(getLeaderboard()); }
  catch (err) { res.json([]); }
});

app.get('/api/status', (req, res) => {
  try { res.json(getCompStatus()); }
  catch (err) { res.json({ started: 0, ended: 0 }); }
});

// ─────────────────────────────────────────
// USER ROUTES
// ─────────────────────────────────────────
app.post('/api/user/details', (req, res) => {
  const { username, name, class: cls, roll, phone, email } = req.body;
  try {
    db.prepare(
      'UPDATE users SET name = ?, class = ?, roll = ?, phone = ?, email = ? WHERE username = ?'
    ).run(name ?? null, cls ?? null, roll ?? null, phone ?? null, email ?? null, username);
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/quit', (req, res) => {
  const { username, finalScore } = req.body;
  if (!username) return res.status(400).json({ error: 'Missing username' });
  try {
    const now = new Date().toISOString();
    db.prepare(`UPDATE users SET disqualified = 'quit', last_active = ? WHERE username = ?`).run(now, username);
    logActivity(`🚪 ${username} quit the competition with ${finalScore} pts`, 'warn');
    broadcast('user_status', { username, status: 'offline', reason: 'quit', ts: now });
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/gameover', (req, res) => {
  const { username, finalScore } = req.body;
  if (!username) return res.status(400).json({ error: 'Missing username' });
  try {
    const now = new Date().toISOString();
    db.prepare(`UPDATE users SET disqualified = 'gameover', last_active = ? WHERE username = ?`).run(now, username);
    logActivity(`💀 ${username} game over — final score: ${finalScore} pts`, 'err');
    broadcast('user_status', { username, status: 'offline', reason: 'gameover', ts: now });
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post('/api/ping', (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'Missing username' });
  try {
    const now = new Date().toISOString();
    db.prepare('UPDATE users SET last_active = ? WHERE username = ?').run(now, username);
    broadcast('user_status', { username, status: 'online', ts: now });
    return res.json({ success: true, ts: now });
  } catch (err) { res.json({ success: false }); }
});

// Save progress (assignedQuestions, solved, wrongAnswers)
app.post('/save-progress', (req, res) => {
  const { username, assignedQuestions, solved, score, wrongAnswers, disqualified, locked, sparedOnce } = req.body;
  try {
    const fields = [];
    const values = [];
    if (assignedQuestions !== undefined) { fields.push('assignedQuestions = ?'); values.push(JSON.stringify(assignedQuestions)); }
    if (solved !== undefined)            { fields.push('solved = ?');            values.push(JSON.stringify(solved)); }
    if (score !== undefined)             { fields.push('score = ?');             values.push(score); }
    if (wrongAnswers !== undefined)      { fields.push('wrongAnswers = ?');      values.push(wrongAnswers); }
    if (disqualified !== undefined)      { fields.push('disqualified = ?');      values.push(disqualified); }
    if (locked !== undefined)            { fields.push('locked = ?');            values.push(locked ? 1 : 0); }
    if (sparedOnce !== undefined)        { fields.push('sparedOnce = ?, spared_once = ?'); values.push(sparedOnce ? 1 : 0, sparedOnce ? 1 : 0); }

    if (fields.length > 0 && username) {
      values.push(username);
      db.prepare(`UPDATE users SET ${fields.join(', ')} WHERE username = ?`).run(...values);
    }
    broadcast('leaderboard', getLeaderboard());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.get('/all-users', (req, res) => {
  try {
    const users = db.prepare(`SELECT * FROM users WHERE role = 'competitor'`).all();
    res.json({ success: true, users: users.map(userToRow) });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

// Legacy compatibility
app.get('/comp-state', (req, res) => {
  try { const s = getCompStatus(); res.json({ started: !!s.started, ended: !!s.ended }); }
  catch (err) { res.json({ started: false, ended: false }); }
});

app.post('/comp-state', (req, res) => {
  const { started, ended } = req.body;
  try {
    db.prepare(
      `INSERT INTO comp_state (state_key, started, ended)
       VALUES ('state', ?, ?)
       ON CONFLICT(state_key) DO UPDATE SET started = excluded.started, ended = excluded.ended`
    ).run(started ? 1 : 0, ended ? 1 : 0);
    broadcast('competition_status', getCompStatus());
    res.json({ success: true });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

// ─────────────────────────────────────────
// API 404
// ─────────────────────────────────────────
app.use('/api', (req, res) => {
  res.status(404).json({ error: `API route not found: ${req.method} ${req.path}` });
});

// ─────────────────────────────────────────
// START
// ─────────────────────────────────────────
initDB();

server.listen(PORT, () => {
  console.log(`\n🚀 HACK & CRACK Server running on http://localhost:${PORT}`);
  console.log(`📡 WebSocket active on ws://localhost:${PORT}`);
  console.log(`\n👤 Admin login: admin / adminhash@2006`);
  console.log(`👥 Users: user01–user100  (NO PASSWORD — username only)\n`);
});
