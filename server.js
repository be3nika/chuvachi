import 'dotenv/config';
import express from 'express';
import http from 'http';
import crypto from 'crypto';
import { Server } from 'socket.io';
import { Bot, InlineKeyboard } from 'grammy';

// ---------- Налаштування ----------
const PORT = Number(process.env.PORT) || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const WEBAPP_URL = process.env.WEBAPP_URL || `http://localhost:${PORT}`;
const DEV_MODE = process.env.DEV_MODE === '1';

const WIN_SCORE = 2;        // гра до 2 перемог (best of 3)
const ROUND_MS = 15_000;    // час на хід
const NEXT_ROUND_MS = 2_500;

// Хто кого б'є
const BEATS = { rock: 'scissors', scissors: 'paper', paper: 'rock' };
const MOVES = Object.keys(BEATS);
const ROOM_RE = /^[A-Z0-9]{6}$/;

// ---------- Перевірка, що юзер справді з Telegram ----------
// https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
function verifyInitData(initData) {
  if (!initData || !BOT_TOKEN) return null;
  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  if (!hash) return null;
  params.delete('hash');

  const dataCheckString = [...params.entries()]
    .map(([k, v]) => `${k}=${v}`)
    .sort()
    .join('\n');

  const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const calc = crypto.createHmac('sha256', secret).update(dataCheckString).digest('hex');
  if (calc.length !== hash.length ||
      !crypto.timingSafeEqual(Buffer.from(calc), Buffer.from(hash))) return null;

  const authDate = Number(params.get('auth_date'));
  if (!authDate || Date.now() / 1000 - authDate > 24 * 3600) return null;

  try { return JSON.parse(params.get('user')); } catch { return null; }
}

// ---------- Стан (у пам'яті; для продакшну — Redis/PostgreSQL) ----------
let waiting = null;            // сокет, що шукає випадкового суперника
const rooms = new Map();       // code -> сокет-хост приватної кімнати
const stats = new Map();       // userId -> { name, wins, losses }

function record(user, won) {
  const s = stats.get(user.id) || { name: user.name, wins: 0, losses: 0 };
  s.name = user.name;
  won ? s.wins++ : s.losses++;
  stats.set(user.id, s);
}

// ---------- HTTP + Socket.IO ----------
const app = express();
app.get('/', (_req, res) => res.sendFile('index.html', { root: import.meta.dirname }));
app.get('/api/leaderboard', (_req, res) => {
  const top = [...stats.values()]
    .sort((a, b) => b.wins - a.wins || a.losses - b.losses)
    .slice(0, 10);
  res.json(top);
});

const server = http.createServer(app);
const io = new Server(server);

io.use((socket, next) => {
  const { initData, devName } = socket.handshake.auth || {};
  let user = verifyInitData(initData);
  if (!user && DEV_MODE) {
    user = { id: 'dev_' + socket.id.slice(0, 6), first_name: String(devName || 'Dev').slice(0, 20) };
  }
  if (!user) return next(new Error('unauthorized'));
  socket.data.user = { id: String(user.id), name: user.first_name || user.username || 'Чувак' };
  next();
});

// ---------- Логіка матчу ----------
function startMatch(a, b) {
  const m = { id: crypto.randomUUID(), players: [a, b], score: [0, 0],
              moves: [null, null], round: 0, timer: null, over: false };
  m.players.forEach((s, i) => {
    s.data.match = m;
    s.data.idx = i;
    s.join(m.id);
    s.emit('match:start', { opponent: m.players[1 - i].data.user.name, winScore: WIN_SCORE });
  });
  setTimeout(() => nextRound(m), 1200);
}

function nextRound(m) {
  if (m.over) return;
  m.round++;
  m.moves = [null, null];
  io.to(m.id).emit('round:start', { round: m.round, timeMs: ROUND_MS });
  m.timer = setTimeout(() => resolveRound(m), ROUND_MS);
}

function resolveRound(m) {
  if (m.over) return;
  clearTimeout(m.timer);
  // хто не встиг — отримує випадковий хід
  const mv = m.moves.map(x => x || MOVES[crypto.randomInt(3)]);
  const winner = mv[0] === mv[1] ? -1 : (BEATS[mv[0]] === mv[1] ? 0 : 1);
  if (winner >= 0) m.score[winner]++;

  m.players.forEach((s, i) => {
    s.emit('round:result', {
      you: mv[i], opponent: mv[1 - i],
      result: winner === -1 ? 'draw' : winner === i ? 'win' : 'lose',
      score: [m.score[i], m.score[1 - i]],
      auto: !m.moves[i],
    });
  });

  const champ = m.score.findIndex(x => x >= WIN_SCORE);
  if (champ >= 0) setTimeout(() => endMatch(m, champ, 'score'), NEXT_ROUND_MS);
  else setTimeout(() => nextRound(m), NEXT_ROUND_MS);
}

function endMatch(m, winnerIdx, reason) {
  if (m.over) return;
  m.over = true;
  clearTimeout(m.timer);
  m.players.forEach((s, i) => {
    record(s.data.user, i === winnerIdx);
    s.emit('match:end', { result: i === winnerIdx ? 'win' : 'lose', reason,
                          score: [m.score[i], m.score[1 - i]] });
    s.leave(m.id);
    s.data.match = null;
  });
}

function cancelSearch(socket) {
  if (waiting === socket) waiting = null;
  if (socket.data.room) {
    rooms.delete(socket.data.room);
    socket.data.room = null;
  }
}

let BOT_USERNAME = null;

io.on('connection', (socket) => {
  socket.emit('hello', { name: socket.data.user.name });

  socket.on('queue:join', () => {
    if (socket.data.match) return;
    cancelSearch(socket);
    if (waiting && waiting.connected && waiting !== socket) {
      const opp = waiting;
      waiting = null;
      startMatch(opp, socket);
    } else {
      waiting = socket;
      socket.emit('queue:waiting');
    }
  });

  socket.on('room:create', () => {
    if (socket.data.match) return;
    cancelSearch(socket);
    let code;
    do { code = crypto.randomBytes(4).toString('hex').toUpperCase().slice(0, 6); }
    while (rooms.has(code));
    rooms.set(code, socket);
    socket.data.room = code;
    const link = BOT_USERNAME ? `https://t.me/${BOT_USERNAME}?start=${code}` : null;
    socket.emit('room:created', { code, link });
  });

  socket.on('room:join', ({ code } = {}) => {
    if (socket.data.match) return;
    code = String(code || '').toUpperCase();
    const host = ROOM_RE.test(code) ? rooms.get(code) : null;
    if (!host || !host.connected || host === socket) {
      return socket.emit('room:error', { message: 'Кімнату не знайдено або вона вже зайнята' });
    }
    cancelSearch(socket);
    rooms.delete(code);
    host.data.room = null;
    startMatch(host, socket);
  });

  socket.on('search:cancel', () => cancelSearch(socket));

  socket.on('move', ({ move } = {}) => {
    const m = socket.data.match;
    if (!m || m.over || !MOVES.includes(move)) return;
    const i = socket.data.idx;
    if (m.moves[i]) return; // хід уже зроблено
    m.moves[i] = move;
    m.players[1 - i].emit('opponent:ready');
    if (m.moves[0] && m.moves[1]) resolveRound(m);
  });

  socket.on('disconnect', () => {
    cancelSearch(socket);
    const m = socket.data.match;
    if (m && !m.over) endMatch(m, 1 - socket.data.idx, 'opponent_left');
  });
});

server.listen(PORT, () => {
  console.log(`🚀 Сервер на http://localhost:${PORT}${DEV_MODE ? '  (DEV_MODE)' : ''}`);
});

// ---------- Бот ----------
if (BOT_TOKEN) {
  const bot = new Bot(BOT_TOKEN);

  bot.command('start', async (ctx) => {
    const code = String(ctx.match || '').trim().toUpperCase();
    const isInvite = ROOM_RE.test(code);
    const url = isInvite ? `${WEBAPP_URL}?room=${code}` : WEBAPP_URL;
    const kb = new InlineKeyboard().webApp(isInvite ? '⚔️ Прийняти виклик' : '🎮 Грати', url);
    await ctx.reply(
      isInvite
        ? 'Тебе викликали на дуель у Чуваках! 🪨✂️📄'
        : 'Камінь, ножиці, папір — хто з чуваків сильніший? 🪨✂️📄\nГра до 2 перемог.',
      { reply_markup: kb },
    );
  });

  bot.catch((err) => console.error('Bot error:', err.message));

  try {
    await bot.init();
    BOT_USERNAME = bot.botInfo.username;
    if (WEBAPP_URL.startsWith('https://')) {
      await bot.api.setChatMenuButton({
        menu_button: { type: 'web_app', text: 'Грати', web_app: { url: WEBAPP_URL } },
      });
    }
    bot.start();
    console.log(`🤖 Бот @${BOT_USERNAME} запущено`);
  } catch (e) {
    console.error('Не вдалося запустити бота:', e.message);
  }
} else {
  console.warn('⚠️  BOT_TOKEN не задано — бот не запущено');
}

