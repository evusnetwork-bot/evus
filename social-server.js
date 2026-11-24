'use strict';
const { randomUUID, createHash } = require('crypto');
const { Pool } = require('pg');
const MAX_IMAGE = 350000;
const isImage = x => typeof x === 'string' && (x === '' || (x.length <= MAX_IMAGE && /^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(x)));
const trimmed = (value, n) => typeof value === 'string' ? value.trim().slice(0,n) : '';
const publicUser = u => ({id:u.id, tag:u.tag, displayName:u.display_name, bio:u.bio, avatar:u.avatar, banner:u.banner});
function createSocialHub({pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, max: 5, connectionTimeoutMillis: 10000 }) : null} = {}) {
  const online = new Map();
  let registrationWindow = 0, registrationCount = 0;
  let initialization = null;
  function ready() {
    if (!initialization) initialization = pool.query(`CREATE TABLE IF NOT EXISTS evus_users (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, tag TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, bio TEXT NOT NULL DEFAULT '', avatar TEXT NOT NULL DEFAULT '', banner TEXT NOT NULL DEFAULT '')`).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_friendships (a TEXT NOT NULL REFERENCES evus_users(id), b TEXT NOT NULL REFERENCES evus_users(id), requester TEXT NOT NULL REFERENCES evus_users(id), status TEXT NOT NULL, PRIMARY KEY (a,b))`)).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_messages (id TEXT PRIMARY KEY, sender TEXT NOT NULL REFERENCES evus_users(id), recipient TEXT NOT NULL REFERENCES evus_users(id), body TEXT NOT NULL, created_at BIGINT NOT NULL)`)).catch(e => { initialization = null; console.error('Banco social indisponível:',e.message); throw e; });
    return initialization;
  }
  const send=(ws,x)=>{if(ws.readyState===1)ws.send(JSON.stringify(x));};
  const emit=(id,event)=>{for(const ws of online.get(id)||[])send(ws,{type:'social-event',event});};
  const pair=(a,b)=>[a,b].sort();
  async function userById(id){return (await pool.query('SELECT * FROM evus_users WHERE id = $1',[id])).rows[0];}
  async function relationship(a,b){const [x,y]=pair(a,b);return (await pool.query('SELECT * FROM evus_friendships WHERE a = $1 AND b = $2',[x,y])).rows[0];}
  async function list(userId){
    const links=(await pool.query('SELECT * FROM evus_friendships WHERE a = $1 OR b = $1',[userId])).rows;
    const friends=[],incoming=[],outgoing=[];
    for(const link of links){ const other=await userById(link.a===userId?link.b:link.a); if(!other)continue;const item={...publicUser(other), online:online.has(other.id)}; if(link.status==='accepted')friends.push(item);else if(link.requester===userId)outgoing.push(item);else incoming.push(item); }
    return {friends,incoming,outgoing};
  }
  async function run(ws,msg){
    if(!pool)throw Error('O servidor ainda não tem banco de dados configurado.'); await ready();
    const action=msg.action;
    if(action==='register'||action==='login'){
      if(ws.socialId)throw Error('Esta conexão já entrou em uma conta.');
      if(typeof msg.code!=='string'||!/^evus_[A-Za-z0-9_-]{43}$/.test(msg.code))throw Error('Código pessoal inválido.');
      const hash=createHash('sha256').update(msg.code).digest('hex');let user;
      if(action==='register'){
        const now=Date.now();if(now-registrationWindow>60000){registrationWindow=now;registrationCount=0;}if(++registrationCount>30)throw Error('Muitas contas criadas agora. Tente novamente mais tarde.');
        const name=trimmed(msg.displayName,32);if(!name)throw Error('Informe seu nome de exibição.');
        const uid=randomUUID(),tag='EVUS-'+randomUUID().replaceAll('-','').slice(0,12).toUpperCase();
        try { await pool.query('INSERT INTO evus_users (id, token_hash, tag, display_name, bio, avatar, banner) VALUES ($1,$2,$3,$4,$5,$6,$7)',[uid,hash,tag,name,'','','']); } catch(e){ if(e.code==='23505')throw Error('Este código já está em uso. Tente entrar com ele.');throw e; }
        user=await userById(uid);
      } else {user=(await pool.query('SELECT * FROM evus_users WHERE token_hash = $1',[hash])).rows[0];if(!user)throw Error('Código pessoal não encontrado.');}
      if(ws.readyState !== 1) throw Error('A conexão foi encerrada.');
      ws.socialId=user.id;if(!online.has(user.id))online.set(user.id,new Set());online.get(user.id).add(ws);
      const listing=await list(user.id);for(const f of listing.friends)emit(f.id,{kind:'refresh'});
      return {user:publicUser(user),...listing};
    }
    if(!ws.socialId)throw Error('Entre na sua conta para continuar.');const self=ws.socialId;
    if(action==='list')return list(self);
    if(action==='profile'){
      const name=trimmed(msg.displayName,32),bio=trimmed(msg.bio,250);if(!name||!isImage(msg.avatar)||!isImage(msg.banner))throw Error('Confira o nome e as imagens do perfil.');
      await pool.query('UPDATE evus_users SET display_name = $1, bio = $2, avatar = $3, banner = $4 WHERE id = $5',[name,bio,msg.avatar,msg.banner,self]);
      const profile=publicUser(await userById(self));emit(self,{kind:'profile',user:profile});for(const f of (await list(self)).friends)emit(f.id,{kind:'refresh'});return {user:profile};
    }
    if(action==='request'){
      const tag=trimmed(msg.tag,32).toUpperCase();if(!/^EVUS-[0-9A-F]{12}$/.test(tag))throw Error('Código de amigo inválido.');
      const other=(await pool.query('SELECT * FROM evus_users WHERE tag = $1',[tag])).rows[0];if(!other||other.id===self)throw Error('Não foi possível encontrar essa pessoa.');
      const [a,b]=pair(self,other.id);const found=await relationship(self,other.id);if(found)throw Error('Vocês já são amigos ou existe uma solicitação pendente.');
      await pool.query('INSERT INTO evus_friendships (a,b,requester,status) VALUES ($1,$2,$3,$4)',[a,b,self,'pending']);emit(other.id,{kind:'refresh'});return list(self);
    }
    if(action==='accept'||action==='remove'){
      const target=trimmed(msg.userId,64),link=await relationship(self,target);if(!link)throw Error('Solicitação não encontrada.');
      const [a,b]=pair(self,target);
      if(action==='accept'){if(link.status!=='pending'||link.requester===self)throw Error('Não é possível aceitar esta solicitação.');await pool.query('UPDATE evus_friendships SET status = $1 WHERE a = $2 AND b = $3',['accepted',a,b]);}
      else await pool.query('DELETE FROM evus_friendships WHERE a = $1 AND b = $2',[a,b]);
      emit(target,{kind:'refresh'});return list(self);
    }
    if(action==='history'||action==='message'){
      const other=trimmed(msg.userId,64),link=await relationship(self,other);if(!link||link.status!=='accepted')throw Error('Esta conversa está disponível somente para amigos.');
      if(action==='message'){
        const body=trimmed(msg.text,4000);if(!body)throw Error('Escreva uma mensagem.');const item={id:randomUUID(),sender:self,recipient:other,body,createdAt:Date.now()};
        await pool.query('INSERT INTO evus_messages (id,sender,recipient,body,created_at) VALUES ($1,$2,$3,$4,$5)',[item.id,self,other,body,item.createdAt]);emit(self,{kind:'message',message:item});emit(other,{kind:'message',message:item});return {message:item};
      }
      const rows=(await pool.query('SELECT * FROM evus_messages WHERE (sender = $1 AND recipient = $2) OR (sender = $2 AND recipient = $1) ORDER BY created_at DESC LIMIT 100',[self,other])).rows;
      return {messages:rows.reverse().map(m=>({id:m.id,sender:m.sender,recipient:m.recipient,body:m.body,createdAt:Number(m.created_at)}))};
    }
    throw Error('Operação desconhecida.');
  }
  function attach(ws){
    let count=0,started=Date.now();
    ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}if(m?.type!=='social')return;
      if(Date.now()-started>60000){started=Date.now();count=0}if(++count>100){send(ws,{type:'social-response',rid:m.rid,ok:false,error:'Muitas solicitações. Aguarde um pouco.'});return;}
      if(typeof m.rid!=='string'||m.rid.length>64)return;
      (ws.socialQueue ||= Promise.resolve());ws.socialQueue=ws.socialQueue.then(()=>run(ws,m)).then(result=>send(ws,{type:'social-response',rid:m.rid,ok:true,result})).catch(e=>{const known=['Código','Informe','Esta','Não','O servidor','Vocês','Solicitação','Escreva','Confira','Operação','Muitas','A conexão'];const error=known.some(x=>e.message?.startsWith(x))?e.message:'Não foi possível concluir. Tente novamente.';send(ws,{type:'social-response',rid:m.rid,ok:false,error});});
    });
    ws.on('close',()=>{const id=ws.socialId;if(!id)return;const connections=online.get(id);connections?.delete(ws);if(!connections?.size){online.delete(id);if(pool)list(id).then(({friends})=>friends.forEach(f=>emit(f.id,{kind:'refresh'}))).catch(()=>{});}});
  }
  return {attach,ready,close:()=>pool?.end()};
}
module.exports={createSocialHub};
