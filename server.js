const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID } = require('crypto');
function startServer({ port = 8787, host = '127.0.0.1' } = {}) {
const rooms = new Map();
const server = new WebSocketServer({ port, host, maxPayload: 128 * 1024 });
server.on('error', error => { if (error.code !== 'EADDRINUSE') console.error('Erro no servidor de sinalização:', error.message); });
function send(ws, data) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
server.on('connection', ws => {
  ws.id = randomUUID();
  ws.on('error', () => {});
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.type === 'join' && !ws.room && typeof m.room === 'string' && typeof m.name === 'string') {
      const room = m.room.trim().slice(0, 64), name = m.name.trim().slice(0, 32);
      if (!room || !name) return;
      if (!rooms.has(room)) rooms.set(room, new Map());
      const peers = rooms.get(room);
      if (peers.size >= 6) return send(ws, { type: 'error', message: 'Esta sala está cheia (limite de 6 pessoas).' });
      ws.room = room; ws.name = name;
      send(ws, { type: 'joined', id: ws.id, peers: [...peers.values()].map(p => ({ id: p.id, name: p.name })) });
      peers.set(ws.id, ws);
      for (const peer of peers.values()) if (peer !== ws) send(peer, { type: 'peer-joined', id: ws.id, name: ws.name });
    } else if (m.type === 'signal' && ws.room && typeof m.to === 'string') {
      const peer = rooms.get(ws.room)?.get(m.to);
      if (peer && m.data && typeof m.data === 'object') send(peer, { type: 'signal', from: ws.id, data: m.data });
    }
  });
  ws.on('close', () => {
    const peers = rooms.get(ws.room); if (!peers) return;
    peers.delete(ws.id); for (const peer of peers.values()) send(peer, { type: 'peer-left', id: ws.id });
    if (!peers.size) rooms.delete(ws.room);
  });
});
server.on('listening', () => console.log(`Sinalização ativa em ${host}:${port}`));
return server;
}
if (require.main === module) startServer({ port: Number(process.env.PORT || 8787), host: process.env.HOST || '0.0.0.0' });
module.exports = { startServer };
