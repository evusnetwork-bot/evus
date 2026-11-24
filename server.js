const { WebSocketServer, WebSocket } = require('ws');
const { randomUUID } = require('crypto');
const http = require('http');
const { createSocialHub } = require('./social-server');
const MAX_FILE_BYTES = 4 * 1024 * 1024;
const MAX_PAYLOAD_BYTES = 6 * 1024 * 1024;
function startServer({ port = 8787, host = '127.0.0.1', socialPool, heartbeatMs = 30000 } = {}) {
  const rooms = new Map();
  const social = createSocialHub({pool: socialPool, getVoiceMembers: channelId => {
    const peers=rooms.get('group-'+channelId);if(!peers)return [];
    const seen=new Set(),result=[];for(const peer of peers.values()){const person=peer.voiceIdentity;if(!person||seen.has(person.userId))continue;seen.add(person.userId);result.push({id:person.userId,displayName:person.displayName,avatar:person.avatar});}return result;
  }});
  const httpServer = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    if (req.method === 'GET' && req.url === '/') { res.writeHead(200); res.end('Servidor CONECTA EVUS ativo'); }
    else { res.writeHead(404); res.end('Não encontrado'); }
  });
  const server = new WebSocketServer({ server: httpServer, maxPayload: MAX_PAYLOAD_BYTES });
  const heartbeat=setInterval(()=>{for(const client of server.clients){if(client.readyState!==WebSocket.OPEN)continue;if(!client.isAlive){client.terminate();continue;}client.isAlive=false;client.ping();if(client.voiceIdentity)social.authorizeVoice(client.voiceSession,client.voiceChannel).then(m=>{if(!m||m.userId!==client.voiceIdentity.userId)client.close(4003,'Acesso revogado');}).catch(()=>client.close(1011,'Falha de autorização'));}},heartbeatMs);heartbeat.unref?.();
  const closeWebSocket = server.close.bind(server);
  server.close = callback => { clearInterval(heartbeat); closeWebSocket(callback); httpServer.close(); social.close()?.catch(() => {}); };
  httpServer.on('error', error => server.emit('error', error));
  httpServer.listen(port, host);
  server.on('error', error => { if (error.code !== 'EADDRINUSE') console.error('Erro no servidor de sinalização:', error.message); });
  function send(ws, data) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
  function reject(ws, message, clientId) { send(ws, { type: 'chat-error', message, ...(clientId ? { clientId } : {}) }); }
  server.on('connection', ws => {
    ws.isAlive=true;ws.on('pong',()=>{ws.isAlive=true;});
    social.attach(ws);
    ws.id = randomUUID(); ws.lastChatTime = 0;
    ws.on('error', () => {});
    ws.on('message', async raw => {
      let m; try { m = JSON.parse(raw.toString()); } catch { return; }
      if (!m || typeof m !== 'object' || Array.isArray(m)) return;
      if (m.type === 'join' && !ws.room && !ws.joining && typeof m.room === 'string' && typeof m.name === 'string') {
        const room = m.room.trim().slice(0, 64), name = m.name.trim().slice(0, 32);
        if (!room || !name) return;
        if (room.startsWith('group-')) {
          ws.joining=true;const channelId=room.slice(6);if(!/^[0-9a-f-]{36}$/.test(channelId))return send(ws,{type:'error',message:'Canal inválido.'});
          try { const identity=await social.authorizeVoice(m.session,channelId);if(!identity||ws.readyState!==WebSocket.OPEN||ws.room)return send(ws,{type:'error',message:'Acesso ao canal de voz negado.'});ws.voiceIdentity=identity;ws.voiceSession=m.session;ws.voiceChannel=channelId;ws.joining=false; } catch {return send(ws,{type:'error',message:'Não foi possível verificar o canal de voz.'});}
        }
        if (!rooms.has(room)) rooms.set(room, new Map());
        const peers = rooms.get(room);
        if (peers.size >= 6) return send(ws, { type: 'error', message: 'Esta sala está cheia (limite de 6 pessoas).' });
        ws.room = room; ws.name = ws.voiceIdentity?.displayName || name;
        send(ws, { type: 'joined', id: ws.id, peers: [...peers.values()].map(p => ({ id: p.id, name: p.name, ...(p.voiceIdentity?{avatar:p.voiceIdentity.avatar}: {}) })), ...(ws.voiceIdentity?{profile:{id:ws.voiceIdentity.userId,displayName:ws.voiceIdentity.displayName,avatar:ws.voiceIdentity.avatar}}:{}), capabilities: { chat: true, maxFileBytes: MAX_FILE_BYTES } });
        peers.set(ws.id, ws);
        for (const peer of peers.values()) if (peer !== ws) send(peer, { type: 'peer-joined', id: ws.id, name: ws.name, ...(ws.voiceIdentity?{avatar:ws.voiceIdentity.avatar}: {}) });
        if(ws.voiceIdentity)social.notifyVoice(ws.voiceIdentity.groupId,ws.voiceChannel).catch(()=>{});
      } else if (m.type === 'signal' && ws.room && typeof m.to === 'string') {
        const peer = rooms.get(ws.room)?.get(m.to);
        if (peer && raw.length <= 128 * 1024 && m.data && typeof m.data === 'object') send(peer, { type: 'signal', from: ws.id, data: m.data });
      } else if (m.type === 'chat' && ws.room) {
        const now = Date.now();
        const clientId = typeof m.clientId === 'string' && /^[a-zA-Z0-9-]{1,80}$/.test(m.clientId) ? m.clientId : '';
        if (now - ws.lastChatTime < 350) return reject(ws, 'Aguarde um instante antes de enviar outra mensagem.', clientId);
        const text = typeof m.text === 'string' ? m.text.trim() : '';
        if (text.length > 4000) return reject(ws, 'A mensagem pode ter até 4.000 caracteres.', clientId);
        let attachment;
        if (m.attachment != null) {
          const a = m.attachment;
          if (!a || typeof a !== 'object' || typeof a.name !== 'string' || typeof a.data !== 'string' || typeof a.mime !== 'string' || a.data.length > Math.ceil(MAX_FILE_BYTES / 3) * 4 || (a.data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(a.data))) return reject(ws, 'Arquivo inválido ou maior que 4 MB.', clientId);
          const decoded = Buffer.from(a.data, 'base64');
          if (!decoded.length || decoded.length > MAX_FILE_BYTES || decoded.toString('base64') !== a.data) return reject(ws, 'Arquivo vazio ou maior que 4 MB.', clientId);
          const name = a.name.replace(/[\x00-\x1f\x7f/\\]/g, '_').trim().slice(0, 120) || 'arquivo';
          const mime = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'].includes(a.mime.toLowerCase()) ? a.mime.toLowerCase() : 'application/octet-stream';
          attachment = { name, mime, size: decoded.length, data: a.data };
        }
        if (!text && !attachment) return reject(ws, 'Digite uma mensagem ou escolha um arquivo.', clientId);
        ws.lastChatTime = now;
        const message = { type: 'chat-message', id: randomUUID(), from: ws.id, name: ws.name, text, time: now, ...(clientId ? { clientId } : {}), ...(attachment ? { attachment } : {}) };
        for (const peer of rooms.get(ws.room).values()) send(peer, message);
      }
    });
    ws.on('close', () => {
      const peers = rooms.get(ws.room); if (!peers) return;
      peers.delete(ws.id); for (const peer of peers.values()) send(peer, { type: 'peer-left', id: ws.id });
      if (!peers.size) rooms.delete(ws.room);
      if(ws.voiceIdentity)social.notifyVoice(ws.voiceIdentity.groupId,ws.voiceChannel).catch(()=>{});
    });
  });
  server.on('listening', () => console.log(`Sinalização ativa em ${host}:${port}`));
  return server;
}
if (require.main === module) startServer({ port: Number(process.env.PORT || 8787), host: process.env.HOST || '0.0.0.0' });
module.exports = { startServer };
