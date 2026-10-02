/**
 * ALPA Chat - Room Manager (Phase 1)
 *
 * Manages rooms and connected users in memory.
 * Phase 2 will add: encrypted message relay, key exchange, persistence.
 *
 * Data model:
 *   rooms: Map<roomId, Set<clientMeta>>
 *   clientMeta: { ws, userId, username, roomId, joinedAt }
 */

'use strict';

// rooms: roomId → Set of clientMeta objects
const rooms = new Map();

/**
 * Returns or creates a room by ID.
 * @param {string} roomId
 * @returns {Set}
 */
function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
  }
  return rooms.get(roomId);
}

/**
 * Joins a user to a room.
 * @param {string} roomId
 * @param {object} clientMeta - { ws, userId, username }
 */
function joinRoom(roomId, clientMeta) {
  const room = getOrCreateRoom(roomId);
  clientMeta.roomId = roomId;
  room.add(clientMeta);
}

/**
 * Removes a client from their room. Cleans up empty rooms.
 * @param {object} clientMeta
 */
function leaveRoom(clientMeta) {
  const { roomId } = clientMeta;
  if (!roomId || !rooms.has(roomId)) return;

  const room = rooms.get(roomId);
  room.delete(clientMeta);

  if (room.size === 0) {
    rooms.delete(roomId);
  }
}

/**
 * Gets all clients in a room.
 * @param {string} roomId
 * @returns {Set}
 */
function getRoomClients(roomId) {
  return rooms.get(roomId) || new Set();
}

/**
 * Broadcasts a message to all clients in a room, optionally excluding one.
 * @param {string} roomId
 * @param {object} message - will be JSON.stringify'd
 * @param {object|null} excludeClient - clientMeta to skip
 */
function broadcastToRoom(roomId, message, excludeClient = null) {
  const room = getRoomClients(roomId);
  const payload = JSON.stringify(message);

  for (const client of room) {
    if (client === excludeClient) continue;
    // 1 = WebSocket.OPEN
    if (client.ws.readyState === 1) {
      client.ws.send(payload);
    }
  }
}

/**
 * Gets online user list for a room (safe metadata only).
 * @param {string} roomId
 * @returns {Array<{userId, username}>}
 */
function getRoomUserList(roomId) {
  const room = getRoomClients(roomId);
  return Array.from(room).map(c => ({
    userId: c.userId,
    username: c.username,
  }));
}

/**
 * Returns stats for all active rooms (for server monitoring).
 */
function getStats() {
  const stats = {};
  for (const [roomId, clients] of rooms.entries()) {
    stats[roomId] = clients.size;
  }
  return stats;
}

module.exports = {
  joinRoom,
  leaveRoom,
  broadcastToRoom,
  getRoomClients,
  getRoomUserList,
  getStats,
};
