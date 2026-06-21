const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const bcrypt = require('bcryptjs');
const crypto = require('crypto');
const { Pool } = require('pg');

app.use(express.static('public'));
app.use(express.json());

/* --- ABANDON PENALTY --- */
app.post('/api/abandon-penalty', async (req, res) => {
  const { username } = req.body || {};
  if (!username) { res.sendStatus(400); return; }
  try {
    await pool.query(
      `UPDATE users SET mmr = GREATEST(0, mmr - 25), updated_at = NOW() WHERE username = $1`,
      [username]
    );
    console.log(`Abandon penalty applied to: ${username}`);
    res.sendStatus(200);
  } catch(e) {
    console.error('Abandon penalty error:', e);
    res.sendStatus(500);
  }
});

/* --- RENDER DATABASE FIX & AUTO-BUILD --- */
const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost:5432/dumpster_dice',
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

pool.connect(async (err, client, release) => {
  if (err) {
    console.error('Database connection failed. Is the DATABASE_URL correct?', err.stack);
  } else {
    console.log('✅ Successfully connected to the PostgreSQL database!');
    
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          username VARCHAR(50) UNIQUE NOT NULL,
          password_hash VARCHAR(255) NOT NULL,
          money INTEGER DEFAULT 0,
          mmr INTEGER DEFAULT 0,
          wins INTEGER DEFAULT 0,
          losses INTEGER DEFAULT 0,
          inventory JSONB DEFAULT '[]',
          equipped_charm VARCHAR(100) DEFAULT NULL,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        );
      `);
      await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS session_token VARCHAR(128) UNIQUE;`);
      await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS session_created_at TIMESTAMP DEFAULT NOW();`);
      console.log('✅ Database tables are built and ready!');
    } catch (dbErr) {
      console.error('❌ Failed to build database tables:', dbErr);
    } finally {
      release(); 
    }
  }
});

const MAX_ROOMS = 5;
const MAX_PLAYERS = 4;
const rooms = new Map();
let roomCounter = 0;
const trades = new Map();
let tradeCounter = 0;

/* --- DB HELPERS --- */
function makeSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

async function dbGet(username) {
  const r = await pool.query('SELECT * FROM users WHERE username=$1', [username]);
  return r.rows[0] || null;
}

async function dbGetBySessionToken(sessionToken) {
  const r = await pool.query('SELECT * FROM users WHERE session_token=$1', [sessionToken]);
  return r.rows[0] || null;
}

async function dbCreate(username, passwordHash) {
  const sessionToken = makeSessionToken();
  const r = await pool.query(
    'INSERT INTO users (username, password_hash, session_token, session_created_at) VALUES ($1,$2,$3,NOW()) RETURNING *',
    [username, passwordHash, sessionToken]
  );
  return r.rows[0];
}

async function dbRefreshSession(username) {
  const sessionToken = makeSessionToken();
  const r = await pool.query(
    'UPDATE users SET session_token=$2, session_created_at=NOW(), updated_at=NOW() WHERE username=$1 RETURNING *',
    [username, sessionToken]
  );
  return r.rows[0];
}

async function dbClearSession(username) {
  await pool.query(
    'UPDATE users SET session_token=NULL, updated_at=NOW() WHERE username=$1',
    [username]
  );
}

async function dbSave(username, { money, mmr, wins, losses, inventory, equippedCharm }) {
  await pool.query(
    `UPDATE users SET money=$2, mmr=$3, wins=$4, losses=$5,
     inventory=$6, equipped_charm=$7, updated_at=NOW()
     WHERE username=$1`,
    [username, money, mmr, wins, losses, JSON.stringify(inventory), equippedCharm || null]
  );
}

async function dbResetUserProgress(username) {
  const result = await pool.query(
    `UPDATE users
     SET money = 0,
         mmr = 0,
         wins = 0,
         losses = 0,
         inventory = '[]'::jsonb,
         equipped_charm = NULL,
         updated_at = NOW()
     WHERE username = $1
     RETURNING username`,
    [username]
  );

  return result.rows[0] || null;
}

function safeProfile(row) {
  return {
    username: row.username,
    money: row.money,
    mmr: row.mmr,
    wins: row.wins,
    losses: row.losses,
    inventory: Array.isArray(row.inventory) ? row.inventory : [],
    equippedCharm: row.equipped_charm || null,
    sessionToken: row.session_token || null
  };
}

/* --- LOBBY HELPERS --- */
function getLobbyList() {
  const list = [];
  for (const room of rooms.values()) {
    list.push({
      id: room.id,
      name: room.name,
      hostName: room.players[0]?.name || '?',
      playerCount: room.players.length,
      maxPlayers: MAX_PLAYERS,
      gameStarted: room.gameStarted
    });
  }
  return list;
}

function broadcastLobbyList() { io.emit('lobbyList', getLobbyList()); }

async function broadcastLeaderboard() {
  try {
    const r = await pool.query('SELECT username, mmr FROM users ORDER BY mmr DESC LIMIT 10');
    io.emit('leaderboardData', r.rows);
  } catch (e) {
    console.error('Leaderboard broadcast error:', e);
  }
}

function closeRoom(room, reason) {
  if (!rooms.has(room.id)) return;
  console.log(`Room ${room.id} closed: ${reason}`);
  room.players.forEach(p => io.to(p.socketId).emit('lobbyClosed', { reason }));
  room.pendingRequests.forEach(r => io.to(r.socketId).emit('joinDenied', reason || 'Lobby closed'));
  rooms.delete(room.id);
  broadcastLobbyList();
}

function broadcastRoomUpdate(room) {
  const data = {
    roomId: room.id,
    players: room.players.map((p, i) => ({
      name: p.name, color: p.color, socketId: p.socketId, isHost: i === 0
    }))
  };
  room.players.forEach(p => io.to(p.socketId).emit('lobbyUpdate', data));
}

function getRoomBySocket(socketId) {
  for (const room of rooms.values()) {
    if (room.players.some(p => p.socketId === socketId)) return room;
  }
  return null;
}

function getTradeBySocket(socketId) {
  for (const trade of trades.values()) {
    if (trade.fromSocketId === socketId || trade.toSocketId === socketId) return trade;
  }
  return null;
}

function getOnlineTradePlayers(exceptSocketId) {
  const list = [];
  for (const [socketId, s] of io.sockets.sockets) {
    if (socketId === exceptSocketId) continue;
    if (!s.data || !s.data.username) continue;
    const room = getRoomBySocket(socketId);
    list.push({
      socketId,
      name: s.data.username,
      color: '#00E5FF',
      inLobby: !!room,
      inGame: !!(room && room.gameStarted),
      roomName: room ? room.name : ''
    });
  }
  return list.sort((a, b) => a.name.localeCompare(b.name));
}

function sanitizeMoneyOffer(amount) {
  const n = Math.floor(Number(amount) || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(n, 1000000000);
}

function sanitizeTradeItems(items) {
  if (!Array.isArray(items)) return [];
  return items.slice(0, 12).map(item => {
    if (typeof item === 'string') return { charm: item.slice(0, 16), wear: 'FT' };
    if (!item || typeof item !== 'object') return null;
    const wear = ['FN','MW','FT','WW','BS'].includes(item.wear) ? item.wear : 'FT';
    return {
      charm: String(item.charm || item.c || '?').slice(0, 16),
      wear,
      uid: String(item.uid || '').slice(0, 80)
    };
  }).filter(item => item && item.charm && item.charm !== '?');
}

function cancelTrade(trade, reason = 'Trade cancelled') {
  if (!trade || !trades.has(trade.id)) return;
  io.to(trade.fromSocketId).emit('tradeCancelled', { reason });
  io.to(trade.toSocketId).emit('tradeCancelled', { reason });
  trades.delete(trade.id);
}

/* --- SOCKET --- */
io.on('connection', (socket) => {
  console.log('Connected: ' + socket.id);
  socket.emit('lobbyList', getLobbyList());

  /* AUTH */
  socket.on('register', async ({ username, password }) => {
    try {
      const trimName = (username || '').trim();
      if (!trimName || trimName.length < 2) { socket.emit('authError', 'Username must be at least 2 characters'); return; }
      if (!password || password.length < 4) { socket.emit('authError', 'Password must be at least 4 characters'); return; }
      const existing = await dbGet(trimName);
      if (existing) { socket.emit('authError', 'That username is already taken'); return; }
      const hash = await bcrypt.hash(password, 10);
      const row = await dbCreate(trimName, hash);
      socket.data.username = trimName;
      console.log('Registered: ' + trimName);
      socket.emit('authSuccess', safeProfile(row));
      broadcastLeaderboard();
    } catch (e) {
      console.error('Register error:', e);
      socket.emit('authError', 'Server error. Try again.');
    }
  });

  socket.on('login', async ({ username, password }) => {
    try {
      const trimName = (username || '').trim();
      if (!trimName) { socket.emit('authError', 'Enter a username'); return; }
      const row = await dbGet(trimName);
      if (!row) { socket.emit('authError', 'Username not found'); return; }
      const ok = await bcrypt.compare(password, row.password_hash);
      if (!ok) { socket.emit('authError', 'Wrong password'); return; }
      const refreshed = await dbRefreshSession(trimName);
      socket.data.username = trimName;
      console.log('Logged in: ' + trimName);
      socket.emit('authSuccess', safeProfile(refreshed));
      broadcastLeaderboard();
    } catch (e) {
      console.error('Login error:', e);
      socket.emit('authError', 'Server error. Try again.');
    }
  });

  socket.on('autoLogin', async ({ sessionToken }) => {
    try {
      if (!sessionToken || typeof sessionToken !== 'string' || sessionToken.length < 32) {
        socket.emit('autoLoginFailed');
        return;
      }
      const row = await dbGetBySessionToken(sessionToken);
      if (!row) {
        socket.emit('autoLoginFailed');
        return;
      }
      socket.data.username = row.username;
      console.log('Auto-login: ' + row.username);
      socket.emit('authSuccess', safeProfile(row));
      broadcastLeaderboard();
    } catch (e) {
      console.error('Auto-login error:', e);
      socket.emit('autoLoginFailed');
    }
  });

  socket.on('logout', async ({ username }) => {
    try {
      if (username) await dbClearSession(username);
      socket.data.username = null;
      socket.emit('loggedOut');
    } catch (e) {
      console.error('Logout error:', e);
    }
  });

  socket.on('saveProfile', async ({ username, money, mmr, wins, losses, inventory, equippedCharm }) => {
    try {
      if (!username) return;
      await dbSave(username, { money, mmr, wins, losses, inventory, equippedCharm });
      broadcastLeaderboard();
    } catch (e) {
      console.error('Save error:', e);
    }
  });

  /* LOBBY */
  socket.on('createLobby', ({ name, color, lobbyName }) => {
    if (getRoomBySocket(socket.id)) { socket.emit('lobbyError', 'Already in a lobby'); return; }
    if (rooms.size >= MAX_ROOMS) { socket.emit('lobbyError', 'Max lobbies (5) reached — try joining one!'); return; }
    const id = String(++roomCounter);
    const room = {
      id,
      name: (lobbyName || '').trim() || `${name}'s Game`,
      hostSocketId: socket.id,
      players: [{ socketId: socket.id, name, color }],
      gameStarted: false,
      currentTurnSocketId: null,
      humanPlayerMap: {},
      pendingRequests: []
    };
    rooms.set(id, room);
    socket.emit('joinedLobby', { isHost: true, roomId: id });
    broadcastLobbyList();
    broadcastRoomUpdate(room);
    console.log(`Lobby "${room.name}" created (id:${id})`);
  });

  socket.on('requestJoin', ({ roomId, name, color }) => {
    const room = rooms.get(roomId);
    if (!room) { socket.emit('lobbyError', 'Lobby not found'); return; }
    if (room.gameStarted) { socket.emit('lobbyError', 'Game already started'); return; }
    if (room.players.length >= MAX_PLAYERS) { socket.emit('lobbyError', 'Lobby is full'); return; }
    if (room.pendingRequests.some(r => r.socketId === socket.id)) { socket.emit('lobbyError', 'Request already sent'); return; }
    room.pendingRequests.push({ socketId: socket.id, name, color });
    socket.emit('joinPending', { roomId, hostName: room.players[0]?.name });
    io.to(room.hostSocketId).emit('joinRequest', { socketId: socket.id, name, color, roomId });
    console.log(`${name} requested to join room ${roomId}`);
  });

  socket.on('approveJoin', ({ roomId, socketId }) => {
    const room = rooms.get(roomId);
    if (!room || socket.id !== room.hostSocketId) return;
    const req = room.pendingRequests.find(r => r.socketId === socketId);
    if (!req) return;
    room.pendingRequests = room.pendingRequests.filter(r => r.socketId !== socketId);
    if (room.players.length >= MAX_PLAYERS) { io.to(socketId).emit('joinDenied', 'Lobby is now full'); return; }
    room.players.push({ socketId, name: req.name, color: req.color });
    io.to(socketId).emit('joinApproved', { roomId, isHost: false });
    broadcastLobbyList();
    broadcastRoomUpdate(room);
    console.log(`${req.name} approved for room ${roomId}`);
  });

  socket.on('denyJoin', ({ roomId, socketId }) => {
    const room = rooms.get(roomId);
    if (!room || socket.id !== room.hostSocketId) return;
    room.pendingRequests = room.pendingRequests.filter(r => r.socketId !== socketId);
    io.to(socketId).emit('joinDenied', 'Your request was denied by the host');
  });

  socket.on('closeLobby', ({ roomId, reason } = {}) => {
    const room = roomId ? rooms.get(String(roomId)) : getRoomBySocket(socket.id);
    if (!room) return;
    if (socket.id !== room.hostSocketId) return;
    closeRoom(room, reason || 'Host closed the lobby');
  });

  socket.on('leaveLobby', ({ roomId, reason, closeIfHost, forceClose } = {}) => {
    const room = roomId ? rooms.get(String(roomId)) : getRoomBySocket(socket.id);
    if (!room) return;

    const wasHost = socket.id === room.hostSocketId;
    if (wasHost || closeIfHost || forceClose) {
      closeRoom(room, reason || 'Host left the lobby');
      return;
    }

    room.pendingRequests = room.pendingRequests.filter(r => r.socketId !== socket.id);
    room.players = room.players.filter(p => p.socketId !== socket.id);
    socket.emit('lobbyClosed', { reason: reason || 'You left the lobby' });

    if (room.players.length === 0) {
      closeRoom(room, 'All players left');
    } else {
      broadcastRoomUpdate(room);
      broadcastLobbyList();
    }
  });

  socket.on('cancelJoinRequest', ({ roomId } = {}) => {
    const room = roomId ? rooms.get(String(roomId)) : null;
    if (!room) return;
    room.pendingRequests = room.pendingRequests.filter(r => r.socketId !== socket.id);
  });

  socket.on('startGame', ({ gameMode, humanPlayerMap }) => {
    const room = getRoomBySocket(socket.id);
    if (!room || socket.id !== room.hostSocketId) return;
    room.gameStarted = true;
    room.humanPlayerMap = humanPlayerMap || {};
    room.players.forEach(p => io.to(p.socketId).emit('gameStarted', { gameMode, humanPlayerMap }));
    broadcastLobbyList();
    console.log(`Room ${room.id} game started (mode:${gameMode})`);
  });

  socket.on('setCurrentTurn', ({ socketId }) => {
    const room = getRoomBySocket(socket.id);
    if (!room || socket.id !== room.hostSocketId) return;
    room.currentTurnSocketId = socketId;
    if (socketId && socketId !== room.hostSocketId) io.to(socketId).emit('yourTurn');
  });

  socket.on('requestRoll', () => {
    const room = getRoomBySocket(socket.id);
    if (!room) return;
    const isHost = socket.id === room.hostSocketId;
    const isTurn = !room.currentTurnSocketId || room.currentTurnSocketId === socket.id;
    if (room.gameStarted && !isHost && !isTurn) { socket.emit('notYourTurn'); return; }
    const roll = Math.floor(Math.random() * 6) + 1;
    console.log(`Room ${room.id}: ${socket.id} rolled ${roll}`);
    room.players.forEach(p => io.to(p.socketId).emit('rollResult', { playerID: socket.id, roll }));
  });

  socket.on('stateSync', (state) => {
    const room = getRoomBySocket(socket.id);
    if (!room || socket.id !== room.hostSocketId) return;
    room.players.forEach(p => { if (p.socketId !== socket.id) io.to(p.socketId).emit('stateSync', state); });
    if (state.gameActive === false && room.gameStarted && !room.closePending) {
      room.closePending = true;
      setTimeout(() => closeRoom(room, 'Game over'), 8000);
    }
  });

  socket.on('gameFx', (fx = {}) => {
    const room = getRoomBySocket(socket.id);
    if (!room || !fx || typeof fx !== 'object') return;

    const allowedTypes = new Set(['spinnerStart', 'spinnerTick', 'spinnerEnd', 'roll', 'taunt']);
    if (!allowedTypes.has(fx.type)) return;

    const safeFx = { ...fx };
    if (typeof safeFx.text === 'string') safeFx.text = safeFx.text.substring(0, 160);
    if (typeof safeFx.message === 'string') safeFx.message = safeFx.message.substring(0, 180);
    if (typeof safeFx.name === 'string') safeFx.name = safeFx.name.substring(0, 32);
    if (typeof safeFx.color !== 'string') safeFx.color = '#fff';

    room.players.forEach(p => {
      if (p.socketId !== socket.id) io.to(p.socketId).emit('gameFx', safeFx);
    });
  });

  /* TRADING */
  socket.on('getTradePlayers', () => {
    socket.emit('tradePlayers', getOnlineTradePlayers(socket.id));
  });

  socket.on('tradeRequest', ({ toSocketId, fromName, fromColor }) => {
    if (!socket.data.username) {
      socket.emit('tradeError', 'Sign in before trading.');
      return;
    }

    if (!toSocketId || toSocketId === socket.id) {
      socket.emit('tradeError', 'Pick another player to trade with.');
      return;
    }

    const targetSocket = io.sockets.sockets.get(String(toSocketId));
    if (!targetSocket || !targetSocket.data || !targetSocket.data.username) {
      socket.emit('tradeError', 'That player is not online anymore.');
      return;
    }

    if (getTradeBySocket(socket.id) || getTradeBySocket(toSocketId)) {
      socket.emit('tradeError', 'One of you is already in a trade.');
      return;
    }

    const tradeId = String(++tradeCounter);
    const trade = {
      id: tradeId,
      roomId: null,
      fromSocketId: socket.id,
      toSocketId: String(toSocketId),
      fromName: String(fromName || socket.data.username || 'Player').slice(0, 32),
      fromColor: fromColor || '#00E5FF',
      toName: targetSocket.data.username,
      toColor: '#00E5FF',
      fromOffer: [],
      toOffer: [],
      fromMoney: 0,
      toMoney: 0,
      fromReady: false,
      toReady: false,
      accepted: false
    };

    trades.set(tradeId, trade);

    io.to(trade.toSocketId).emit('tradeRequest', {
      tradeId,
      fromSocketId: socket.id,
      fromName: trade.fromName,
      fromColor: trade.fromColor
    });
  });

  socket.on('tradeRespond', ({ tradeId, accepted }) => {
    const trade = trades.get(String(tradeId));
    if (!trade || socket.id !== trade.toSocketId) return;

    if (!accepted) {
      cancelTrade(trade, 'Trade declined');
      return;
    }

    trade.accepted = true;

    io.to(trade.fromSocketId).emit('tradeStarted', {
      tradeId: trade.id,
      partner: {
        socketId: trade.toSocketId,
        name: trade.toName,
        color: trade.toColor
      }
    });

    io.to(trade.toSocketId).emit('tradeStarted', {
      tradeId: trade.id,
      partner: {
        socketId: trade.fromSocketId,
        name: trade.fromName,
        color: trade.fromColor
      }
    });
  });

  socket.on('tradeOfferUpdate', ({ tradeId, offerItems, moneyOffer }) => {
    const trade = trades.get(String(tradeId));
    if (!trade || !trade.accepted) return;

    const sanitized = sanitizeTradeItems(offerItems);
    const money = sanitizeMoneyOffer(moneyOffer);

    if (socket.id === trade.fromSocketId) {
      trade.fromOffer = sanitized;
      trade.fromMoney = money;
      trade.fromReady = false;

      io.to(trade.toSocketId).emit('tradeOfferUpdate', {
        offerItems: trade.fromOffer,
        moneyOffer: trade.fromMoney,
        ready: trade.fromReady
      });

      io.to(trade.fromSocketId).emit('tradeReadyUpdate', {
        youReady: trade.fromReady,
        themReady: trade.toReady,
        theirMoneyOffer: trade.toMoney
      });
    } else if (socket.id === trade.toSocketId) {
      trade.toOffer = sanitized;
      trade.toMoney = money;
      trade.toReady = false;

      io.to(trade.fromSocketId).emit('tradeOfferUpdate', {
        offerItems: trade.toOffer,
        moneyOffer: trade.toMoney,
        ready: trade.toReady
      });

      io.to(trade.toSocketId).emit('tradeReadyUpdate', {
        youReady: trade.toReady,
        themReady: trade.fromReady,
        theirMoneyOffer: trade.fromMoney
      });
    }
  });

  socket.on('tradeReady', ({ tradeId, offerItems, moneyOffer }) => {
    const trade = trades.get(String(tradeId));
    if (!trade || !trade.accepted) return;

    const sanitized = sanitizeTradeItems(offerItems);
    const money = sanitizeMoneyOffer(moneyOffer);

    if (socket.id === trade.fromSocketId) {
      trade.fromOffer = sanitized;
      trade.fromMoney = money;
      trade.fromReady = true;
    } else if (socket.id === trade.toSocketId) {
      trade.toOffer = sanitized;
      trade.toMoney = money;
      trade.toReady = true;
    } else {
      return;
    }

    io.to(trade.fromSocketId).emit('tradeReadyUpdate', {
      youReady: trade.fromReady,
      themReady: trade.toReady,
      theirMoneyOffer: trade.toMoney
    });

    io.to(trade.toSocketId).emit('tradeReadyUpdate', {
      youReady: trade.toReady,
      themReady: trade.fromReady,
      theirMoneyOffer: trade.fromMoney
    });

    if (trade.fromReady && trade.toReady) {
      io.to(trade.fromSocketId).emit('tradeComplete', {
        receiveItems: trade.toOffer,
        receiveMoney: trade.toMoney
      });

      io.to(trade.toSocketId).emit('tradeComplete', {
        receiveItems: trade.fromOffer,
        receiveMoney: trade.fromMoney
      });

      trades.delete(trade.id);
    }
  });

  socket.on('tradeCancel', ({ tradeId } = {}) => {
    const trade = tradeId ? trades.get(String(tradeId)) : getTradeBySocket(socket.id);
    if (!trade) return;
    if (socket.id !== trade.fromSocketId && socket.id !== trade.toSocketId) return;
    cancelTrade(trade, 'Trade cancelled');
  });

  socket.on('chatMessage', ({ name, color, text }) => {
    const room = getRoomBySocket(socket.id);
    if (!room || !name || !text) return;
    room.players.forEach(p => io.to(p.socketId).emit('chatMessage', {
      name, color: color || '#fff', text: String(text).substring(0, 200)
    }));
  });

  socket.on('getLobbyList', () => { socket.emit('lobbyList', getLobbyList()); });

  socket.on('getLeaderboard', async () => {
    try {
      const r = await pool.query(
        'SELECT username, mmr FROM users ORDER BY mmr DESC LIMIT 10'
      );
      socket.emit('leaderboardData', r.rows);
    } catch(e) { console.error('Leaderboard error:', e); }
  });

  /* ADMIN */
  socket.on('adminGetUsers', async ({ username }) => {
    if (username !== 'Bingle Berry') { socket.emit('adminError', 'Unauthorized'); return; }
    try {
      const r = await pool.query(
        'SELECT username, money, mmr, wins, losses, created_at FROM users ORDER BY username ASC'
      );
      socket.emit('adminUsersList', r.rows);
    } catch(e) { console.error(e); socket.emit('adminError', 'Failed to fetch users'); }
  });

  socket.on('adminDeleteUser', async ({ adminUsername, targetUsername }) => {
    if (adminUsername !== 'Bingle Berry') { socket.emit('adminError', 'Unauthorized'); return; }
    if (targetUsername === 'Bingle Berry') { socket.emit('adminError', 'Cannot delete admin account'); return; }
    try {
      await pool.query('DELETE FROM users WHERE username=$1', [targetUsername]);
      console.log(`Admin deleted user: ${targetUsername}`);
      socket.emit('adminDeleteSuccess', targetUsername);
    } catch(e) { console.error(e); socket.emit('adminError', 'Delete failed'); }
  });

  socket.on('adminResetUser', async ({ adminUsername, targetUsername }) => {
    if (adminUsername !== 'Bingle Berry' || socket.data.username !== 'Bingle Berry') {
      socket.emit('adminError', 'Unauthorized');
      return;
    }

    try {
      const target = (targetUsername || '').trim();
      if (!target) {
        socket.emit('adminError', 'Missing target user');
        return;
      }

      const resetRow = await dbResetUserProgress(target);
      if (!resetRow) {
        socket.emit('adminError', `User not found: ${target}`);
        return;
      }

      console.log(`↻ Admin reset progress for: ${target}`);

      io.emit('profilesReset', { by: adminUsername, targetUsername: target, all: false });
      socket.emit('adminResetUserSuccess', { targetUsername: target });

      const r = await pool.query('SELECT username, mmr FROM users ORDER BY mmr DESC LIMIT 10');
      io.emit('leaderboardData', r.rows);
    } catch (e) {
      console.error('Admin reset-user error:', e);
      socket.emit('adminError', 'User reset failed');
    }
  });

  socket.on('adminResetAllUsers', async ({ adminUsername }) => {
    if (adminUsername !== 'Bingle Berry' || socket.data.username !== 'Bingle Berry') {
      socket.emit('adminError', 'Unauthorized');
      return;
    }

    try {
      const result = await pool.query(`
        UPDATE users
        SET money = 0,
            mmr = 0,
            wins = 0,
            losses = 0,
            inventory = '[]'::jsonb,
            equipped_charm = NULL,
            updated_at = NOW()
      `);

      console.log(`🧨 Admin reset all player progress. Accounts reset: ${result.rowCount}`);

      io.emit('profilesReset', { by: adminUsername, all: true });
      io.emit('leaderboardData', []);
      socket.emit('adminResetAllSuccess', { resetCount: result.rowCount });
    } catch (e) {
      console.error('Admin reset-all error:', e);
      socket.emit('adminError', 'Reset failed');
    }
  });

  socket.on('adminTriggerGodMode', ({ adminUsername, targetUsername }) => {
    if (adminUsername !== 'Bingle Berry') { 
      socket.emit('adminError', 'Unauthorized. Only Bingle Berry has this power.'); 
      return; 
    }
    console.log(`⚡ Admin granted God Mode to: ${targetUsername}`);
    io.emit('godModeActivated', { targetUsername }); 
  });

  socket.on('disconnect', () => {
    console.log('Disconnected: ' + socket.id);

    const activeTrade = getTradeBySocket(socket.id);
    if (activeTrade) cancelTrade(activeTrade, 'Player disconnected');

    for (const room of rooms.values()) {
      room.pendingRequests = room.pendingRequests.filter(r => r.socketId !== socket.id);
    }

    const room = getRoomBySocket(socket.id);
    if (!room) { broadcastLobbyList(); return; }

    const wasHost = socket.id === room.hostSocketId;
    room.players = room.players.filter(p => p.socketId !== socket.id);
    
    if (wasHost) {
      closeRoom(room, 'Host left the lobby');
    } else if (room.players.length === 0) {
      closeRoom(room, 'All players left');
    } else {
      broadcastRoomUpdate(room);
      broadcastLobbyList();
    }
  });
});

/* --- PORT FIX --- */
const PORT = process.env.PORT || 5000;
http.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

/* --- LEADERBOARD AUTO-REFRESH --- */
setInterval(async () => {
  try {
    const r = await pool.query('SELECT username, mmr FROM users ORDER BY mmr DESC LIMIT 10');
    io.emit('leaderboardData', r.rows);
  } catch (e) {
    console.error('Auto-leaderboard update error:', e);
  }
}, 3 * 60 * 1000);
