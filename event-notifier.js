const http = require('http');
const net = require('net');

// Somente estas origens podem acionar eventos a partir de navegadores.
const allowedOrigins = new Set(['https://lanelkrav.com.br', 'https://www.lanelkrav.com.br', 'https://conecta-evus-download.spryparsnip3.chatgpt.site']);
function createEventHandler({ webhook = process.env.DISCORD_WEBHOOK_URL, fetchImpl = globalThis.fetch, render = Boolean(process.env.RENDER) } = {}) {
  const lastByIp = new Map(); let windowStart = 0; let count = 0;
  const validWebhook = typeof webhook === 'string' && /^https:\/\/(discord\.com|discordapp\.com)\/api\/webhooks\/\d+\/[A-Za-z0-9._-]+$/.test(webhook);
  return async function eventHandler(req, res) {
    const respond = (code, data) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }); res.end(JSON.stringify(data)); };
    if (req.method === 'GET' && req.url === '/') return respond(200, { status: 'ok' });
    const origin = req.headers.origin;
    if (origin && !allowedOrigins.has(origin)) return respond(403, { error: 'Origem não permitida' });
    if (origin) { res.setHeader('Access-Control-Allow-Origin', origin); res.setHeader('Vary', 'Origin'); }
    if (req.method === 'OPTIONS' && req.url === '/api/notification') { res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS'); res.setHeader('Access-Control-Allow-Headers', 'Content-Type'); res.writeHead(204); return res.end(); }
    if (req.method !== 'POST' || req.url !== '/api/notification') return respond(404, { error: 'Não encontrado' });
    if (!validWebhook) return respond(503, { error: 'Notificações não configuradas' });
    const forwarded = render ? String(req.headers['cf-connecting-ip'] || '').trim() : '';
    const ip = net.isIP(forwarded) ? forwarded : (req.socket.remoteAddress || 'indisponível');
    const now = Date.now(); if (now-windowStart >= 60000) { count=0; windowStart=now; }
    for (const [key,time] of lastByIp) if (now-time > 60000) lastByIp.delete(key);
    let body = ''; for await (const chunk of req) { body += chunk; if (body.length > 512) return respond(413, { error: 'Requisição muito grande' }); }
    let payload; try { payload=JSON.parse(body); } catch { return respond(400, { error:'Evento inválido' }); }
    if (!payload || !['download','first-open'].includes(payload.event) || Object.keys(payload).some(key => key !== 'event')) return respond(400, { error:'Evento inválido' });
    const rateKey = `${ip}:${payload.event}`;
    if (count >= 30 || now-(lastByIp.get(rateKey) || 0) < 10000) return respond(429, { error: 'Aguarde antes de tentar novamente' });
    count++; lastByIp.set(rateKey, now);
    const label = payload.event === 'download' ? 'Download do instalador concluído' : 'Primeira abertura do aplicativo';
    try {
      const response = await fetchImpl(webhook, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({content:`📦 **CONECTA EVUS**\n${label}\nIP informado pela conexão: ${ip}\nHorário: ${new Date(now).toISOString()}`,allowed_mentions:{parse:[]}}), signal:AbortSignal.timeout(5000) });
      if (!response.ok) return respond(502, { error:'Falha ao enviar notificação' });
      return respond(202, { ok:true });
    } catch { return respond(502, { error:'Falha ao enviar notificação' }); }
  };
}
module.exports={createEventHandler};
