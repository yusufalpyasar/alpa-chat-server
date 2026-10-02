/**
 * ALPA Chat Server - Phase 1
 *
 * Technology: Node.js + ws (WebSocket)
 *
 * What this server does:
 *  - Accepts WebSocket connections from ALPA-CHAT.html clients
 *  - Routes messages between users in the same room
 *  - Manages rooms and online user lists
 *  - Enforces basic server-side rules (rate limiting, room membership)
 *
 * What this server intentionally does NOT do (Phase 1):
 *  - Read message content (Phase 2 will add E2EE so server cannot read it)
 *  - Persist messages to disk/database (by design for privacy)
 *  - Authenticate users beyond room membership
 *
 * Security notes for Phase 1:
 *  ✅ Server controls room membership
 *  ✅ Rate limiting per connection
 *  ✅ Username length limits enforced server-side
 *  ✅ Room ID validation server-side
 *  ❌ Message content is plaintext (E2EE comes in Phase 2)
 *  ❌ No persistent authentication / user accounts yet
 *  ❌ No TLS (add a reverse proxy like nginx for production)
 */

'use strict';

const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const {
  joinRoom,
  leaveRoom,
  broadcastToRoom,
  getRoomUserList,
  getStats,
} = require('./roomManager');

// ─── Configuration ────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

// Rate limiting: max messages per window per connection
const RATE_LIMIT_WINDOW_MS = 5000;   // 5 seconds
const RATE_LIMIT_MAX_MSGS = 20;      // max 20 messages in 5s

// Limits
const MAX_USERNAME_LENGTH = 32;
const MAX_ROOM_ID_LENGTH = 32;
const MAX_MESSAGE_LENGTH = 4096;     // 4 KB plaintext; will grow with E2EE overhead

// ─── Server ──────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: PORT });

console.log(`\n🔐 ALPA Chat Server (Phase 1) — ws://localhost:${PORT}`);
console.log(`   Message encryption: NONE (Phase 2 will add E2EE)`);
console.log(`   Message persistence: NONE (privacy by design)\n`);

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Validates room ID format: letters, digits, hyphens, 3–32 chars.
 * Example valid: ALP-X7K92
 */
function isValidRoomId(roomId) {
  if (typeof roomId !== 'string') return false;
  if (roomId.length < 3 || roomId.length > MAX_ROOM_ID_LENGTH) return false;
  return /^[A-Za-z0-9\-]+$/.test(roomId);
}

/**
 * Validates username: printable, no leading/trailing spaces, 1–32 chars.
 */
function isValidUsername(username) {
  if (typeof username !== 'string') return false;
  const trimmed = username.trim();
  return trimmed.length >= 1 && trimmed.length <= MAX_USERNAME_LENGTH;
}

/**
 * Sends a typed JSON message to a single WebSocket.
 */
function send(ws, message) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(message));
  }
}

/**
 * Sends an error to the client and optionally closes the connection.
 */
function sendError(ws, code, message, close = false) {
  send(ws, { type: 'error', code, message });
  if (close) ws.terminate();
}

// ─── Connection handler ───────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  // Per-connection state
  const clientMeta = {
    ws,
    userId: uuidv4(),
    username: null,
    roomId: null,
    joinedAt: null,
    // Rate limiting state
    _msgCount: 0,
    _rateLimitTimer: null,
  };

  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`[+] Connection from ${ip} | userId=${clientMeta.userId}`);

  // ── Message handler ────────────────────────────────────────────────────────
  ws.on('message', (rawData) => {
    // Reject oversized frames early
    if (rawData.length > MAX_MESSAGE_LENGTH + 512) {
      return sendError(ws, 'MSG_TOO_LARGE', 'Message too large');
    }

    // Rate limiting
    clientMeta._msgCount++;
    if (!clientMeta._rateLimitTimer) {
      clientMeta._rateLimitTimer = setTimeout(() => {
        clientMeta._msgCount = 0;
        clientMeta._rateLimitTimer = null;
      }, RATE_LIMIT_WINDOW_MS);
    }
    if (clientMeta._msgCount > RATE_LIMIT_MAX_MSGS) {
      return sendError(ws, 'RATE_LIMITED', 'Sending too fast, slow down.');
    }

    // Parse
    let msg;
    try {
      msg = JSON.parse(rawData);
    } catch {
      return sendError(ws, 'INVALID_JSON', 'Invalid JSON');
    }

    // Dispatch
    switch (msg.type) {
      case 'join':
        handleJoin(clientMeta, msg);
        break;
      case 'chat':
        handleChat(clientMeta, msg);
        break;
      case 'typing':
        handleTyping(clientMeta, msg);
        break;
      default:
        sendError(ws, 'UNKNOWN_TYPE', `Unknown message type: ${msg.type}`);
    }
  });

  // ── Disconnect handler ─────────────────────────────────────────────────────
  ws.on('close', () => {
    handleDisconnect(clientMeta);
  });

  ws.on('error', (err) => {
    console.error(`[!] WebSocket error for ${clientMeta.userId}:`, err.message);
  });
});

// ─── Message handlers ─────────────────────────────────────────────────────────

/**
 * Client wants to join a room.
 * Expected payload: { type: 'join', username: string, roomId: string }
 */
function handleJoin(clientMeta, msg) {
  const { ws } = clientMeta;

  // Already in a room
  if (clientMeta.roomId) {
    return sendError(ws, 'ALREADY_JOINED', 'Already in a room');
  }

  // Validate inputs (server-side, not trusting client)
  if (!isValidUsername(msg.username)) {
    return sendError(ws, 'INVALID_USERNAME',
      `Username must be 1–${MAX_USERNAME_LENGTH} characters.`);
  }
  if (!isValidRoomId(msg.roomId)) {
    return sendError(ws, 'INVALID_ROOM_ID',
      'Room ID must be 3–32 alphanumeric characters (hyphens allowed).');
  }

  const username = msg.username.trim();
  const roomId = msg.roomId.trim().toUpperCase();

  clientMeta.username = username;
  clientMeta.joinedAt = Date.now();

  joinRoom(roomId, clientMeta);

  console.log(`[→] ${username} (${clientMeta.userId}) joined room ${roomId}`);

  // Confirm to the joining client
  send(ws, {
    type: 'joined',
    userId: clientMeta.userId,
    username,
    roomId,
    users: getRoomUserList(roomId),
    serverTime: Date.now(),
  });

  // Notify others in the room
  broadcastToRoom(roomId, {
    type: 'user_joined',
    userId: clientMeta.userId,
    username,
    users: getRoomUserList(roomId),
    timestamp: Date.now(),
  }, clientMeta);
}

/**
 * Client sends a chat message.
 * Expected payload: { type: 'chat', text: string }
 *
 * Phase 2 note: `text` will be replaced by `encryptedPayload` (ciphertext).
 * The server will relay it without being able to read it.
 */
function handleChat(clientMeta, msg) {
  const { ws, roomId, username, userId } = clientMeta;

  if (!roomId) {
    return sendError(ws, 'NOT_IN_ROOM', 'Join a room first.');
  }

  if (typeof msg.text !== 'string' || msg.text.trim().length === 0) {
    return;
  }

  if (msg.text.length > MAX_MESSAGE_LENGTH) {
    return sendError(ws, 'MSG_TOO_LARGE', 'Message too long.');
  }

  // Use client's timestamp (it was included in the HMAC signature).
  // Validate it's a reasonable number (reject if too far from server time).
  const now = Date.now();
  const clientTs = typeof msg.timestamp === 'number' && Number.isFinite(msg.timestamp)
    ? msg.timestamp
    : now;
  // Accept timestamps within ±5 minutes to tolerate clock skew
  const timestamp = Math.abs(clientTs - now) < 300_000 ? clientTs : now;


  // Server relays the message + HMAC integrity tag.
  // Server does NOT log message content (privacy by design).
  // HMAC is computed client-side with a password the server never sees.
  // Phase 2: msg.text → msg.encryptedPayload (E2EE).
  broadcastToRoom(roomId, {
    type: 'chat',
    userId,
    username,
    text: msg.text,
    hmac: msg.hmac || null,   // HMAC-SHA256 integrity tag (client-computed)
    timestamp,
  }, null);
}

/**
 * Client is typing. Relays to room without logging.
 * Expected payload: { type: 'typing', isTyping: boolean }
 */
function handleTyping(clientMeta, msg) {
  const { roomId, username, userId } = clientMeta;
  if (!roomId) return;

  broadcastToRoom(roomId, {
    type: 'typing',
    userId,
    username,
    isTyping: !!msg.isTyping,
  }, clientMeta); // exclude sender
}

/**
 * Client disconnected. Clean up and notify room.
 */
function handleDisconnect(clientMeta) {
  const { userId, username, roomId } = clientMeta;

  if (clientMeta._rateLimitTimer) {
    clearTimeout(clientMeta._rateLimitTimer);
  }

  if (!roomId) {
    console.log(`[-] Disconnected (was not in a room) | userId=${userId}`);
    return;
  }

  leaveRoom(clientMeta);

  console.log(`[-] ${username} (${userId}) left room ${roomId}`);

  broadcastToRoom(roomId, {
    type: 'user_left',
    userId,
    username,
    users: getRoomUserList(roomId),
    timestamp: Date.now(),
  });
}

// ─── Admin / monitoring ───────────────────────────────────────────────────────
setInterval(() => {
  const stats = getStats();
  const roomCount = Object.keys(stats).length;
  if (roomCount > 0) {
    console.log(`[stats] Active rooms: ${roomCount} |`, stats);
  }
}, 60_000);

wss.on('listening', () => {
  console.log(`✅ Listening on ws://localhost:${PORT}\n`);
});
