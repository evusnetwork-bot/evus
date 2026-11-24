'use strict';
const { randomUUID, randomBytes, createHash, scrypt, timingSafeEqual } = require('crypto');
const { promisify } = require('util');
const derivePassword = promisify(scrypt);
const normalizeUsername = value => typeof value === 'string' ? value.trim().toLowerCase() : '';
const validUsername = value => /^[a-z0-9_]{3,24}$/.test(value);
const validPassword = value => typeof value === 'string' && value.length >= 10 && value.length <= 128;
async function passwordDigest(password, salt = randomBytes(16)) { const hash = await derivePassword(password, salt, 64, {N:16384,r:8,p:1,maxmem:64*1024*1024}); return `scrypt$${salt.toString('base64url')}$${hash.toString('base64url')}`; }
async function passwordMatches(password, encoded) { const parts = typeof encoded === 'string' ? encoded.split('$') : []; const salt = parts.length===3 ? Buffer.from(parts[1],'base64url') : Buffer.alloc(16); const expected = parts.length===3 ? Buffer.from(parts[2],'base64url') : Buffer.alloc(64); const valid = parts[0]==='scrypt' && salt.length===16 && expected.length===64; const computed = await derivePassword(password, valid?salt:Buffer.alloc(16), 64, {N:16384,r:8,p:1,maxmem:64*1024*1024}); return valid && timingSafeEqual(computed, expected); }
const { Pool } = require('pg');
const MAX_IMAGE = 350000;
const isImage = x => typeof x === 'string' && (x === '' || (x.length <= MAX_IMAGE && /^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(x)));
const trimmed = (value, n) => typeof value === 'string' ? value.trim().slice(0,n) : '';
const publicUser = u => ({id:u.id, tag:u.tag, username:u.username, displayName:u.display_name, bio:u.bio, avatar:u.avatar, banner:u.banner});
function createSocialHub({pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, max: 5, connectionTimeoutMillis: 10000 }) : null} = {}) {
  const online = new Map();
  let registrationWindow = 0, registrationCount = 0, authWindow = 0, authCount = 0; const attempts = new Map();
  let initialization = null;
  function ready() {
    if (!initialization) initialization = pool.query(`CREATE TABLE IF NOT EXISTS evus_users (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, tag TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, bio TEXT NOT NULL DEFAULT '', avatar TEXT NOT NULL DEFAULT '', banner TEXT NOT NULL DEFAULT '')`).then(() => pool.query('ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS username TEXT')).then(() => pool.query('ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS password_hash TEXT')).then(() => pool.query('CREATE UNIQUE INDEX IF NOT EXISTS evus_users_username_idx ON evus_users(username)')).then(() => pool.query('CREATE TABLE IF NOT EXISTS evus_sessions (token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES evus_users(id), expires_at BIGINT NOT NULL)')).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_friendships (a TEXT NOT NULL REFERENCES evus_users(id), b TEXT NOT NULL REFERENCES evus_users(id), requester TEXT NOT NULL REFERENCES evus_users(id), status TEXT NOT NULL, PRIMARY KEY (a,b))`)).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_messages (id TEXT PRIMARY KEY, sender TEXT NOT NULL REFERENCES evus_users(id), recipient TEXT NOT NULL REFERENCES evus_users(id), body TEXT NOT NULL, created_at BIGINT NOT NULL)`)).catch(e => { initialization = null; console.error('Banco social indisponível:',e.message); throw e; });
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
    if(['register','login','resume','migrate'].includes(action)){
      if(ws.socialId)throw Error('Esta conexão já entrou em uma conta.');
      const now=Date.now();if(now-authWindow>60000){authWindow=now;authCount=0;}if(++authCount>120)throw Error('Muitas tentativas. Aguarde um pouco.');
      let user, token, expiresAt;
      if(action==='resume'){
        if(typeof msg.session!=='string'||!/^sess_[A-Za-z0-9_-]{43}$/.test(msg.session))throw Error('Sessão expirada. Entre novamente.');
        const hash=createHash('sha256').update(msg.session).digest('hex');
        const session=(await pool.query('SELECT * FROM evus_sessions WHERE token_hash = $1',[hash])).rows[0];
        if(!session||Number(session.expires_at)<=now)throw Error('Sessão expirada. Entre novamente.');
        user=await userById(session.user_id);if(!user)throw Error('Sessão expirada. Entre novamente.');token=msg.session;expiresAt=Number(session.expires_at);
      } else {
        const username=normalizeUsername(msg.username);
        if(!validUsername(username))throw Error('Usuário deve ter de 3 a 24 caracteres: letras, números ou _.');
        if(!validPassword(msg.password))throw Error('Senha deve ter de 10 a 128 caracteres.');
        if(action==='login'){
          const bucket=attempts.get(username);if(bucket && bucket.until>now && bucket.count>=8)throw Error('Muitas tentativas. Aguarde alguns minutos.');
          user=(await pool.query('SELECT * FROM evus_users WHERE username = $1',[username])).rows[0];
          if(!await passwordMatches(msg.password,user?.password_hash)){
            if(attempts.size>2000)attempts.clear();const prev=attempts.get(username);attempts.set(username,{count:prev&&prev.until>now?prev.count+1:1,until:now+10*60000});throw Error('Usuário ou senha incorretos.');
          }
          attempts.delete(username);
        } else {
          if(now-registrationWindow>60000){registrationWindow=now;registrationCount=0;}if(++registrationCount>30)throw Error('Muitas contas criadas agora. Tente novamente mais tarde.');
          const encoded=await passwordDigest(msg.password);
          if(action==='register'){
            const name=trimmed(msg.displayName,32);if(!name)throw Error('Informe seu nome de exibição.');
            const uid=randomUUID(),tag='EVUS-'+randomUUID().replaceAll('-','').slice(0,12).toUpperCase(),unusableHash=randomBytes(32).toString('hex');
            try{await pool.query('INSERT INTO evus_users (id, token_hash, tag, display_name, bio, avatar, banner, username, password_hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',[uid,unusableHash,tag,name,'','','',username,encoded]);}catch(e){if(e.code==='23505')throw Error('Usuário já está em uso. Escolha outro.');throw e;}
            user=await userById(uid);
          } else {
            if(typeof msg.code!=='string'||!/^evus_[A-Za-z0-9_-]{43}$/.test(msg.code))throw Error('Código anterior inválido.');
            const oldHash=createHash('sha256').update(msg.code).digest('hex');user=(await pool.query('SELECT * FROM evus_users WHERE token_hash = $1',[oldHash])).rows[0];
            if(!user||user.username)throw Error('Código anterior não encontrado ou já migrado.');
            try{const result=await pool.query('UPDATE evus_users SET username = $1, password_hash = $2, token_hash = $3 WHERE id = $4 AND username IS NULL',[username,encoded,randomBytes(32).toString('hex'),user.id]);if(result.rowCount!==1)throw Error('Conta já foi migrada.');}catch(e){if(e.code==='23505')throw Error('Usuário já está em uso. Escolha outro.');throw e;}user=await userById(user.id);
          }
        }
        token='sess_'+randomBytes(32).toString('base64url');const tokenHash=createHash('sha256').update(token).digest('hex');
        expiresAt=now+30*86400000;await pool.query('INSERT INTO evus_sessions (token_hash,user_id,expires_at) VALUES ($1,$2,$3)',[tokenHash,user.id,expiresAt]);
      }
      if(ws.readyState !== 1) throw Error('A conexão foi encerrada.');
      ws.socialId=user.id;ws.sessionExpiresAt=expiresAt;ws.sessionHash=createHash('sha256').update(token).digest('hex');if(!online.has(user.id))online.set(user.id,new Set());online.get(user.id).add(ws);
      const listing=await list(user.id);for(const f of listing.friends)emit(f.id,{kind:'refresh'});
      return {user:publicUser(user),session:token,...listing};
    }
    if(!ws.socialId)throw Error('Entre na sua conta para continuar.');if(Date.now()>=ws.sessionExpiresAt){ws.close();throw Error('Sessão expirada. Entre novamente.');}const self=ws.socialId;
    if(action==='logout'){if(ws.sessionHash)await pool.query('DELETE FROM evus_sessions WHERE token_hash = $1',[ws.sessionHash]);ws.socialId=null;const conns=online.get(self);conns?.delete(ws);if(!conns?.size){online.delete(self);for(const f of (await list(self)).friends)emit(f.id,{kind:'refresh'});}return {}; }
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
      (ws.socialQueue ||= Promise.resolve());ws.socialQueue=ws.socialQueue.then(()=>run(ws,m)).then(result=>send(ws,{type:'social-response',rid:m.rid,ok:true,result})).catch(e=>{const known=['Código','Informe','Esta','Não','O servidor','Vocês','Solicitação','Escreva','Confira','Operação','Muitas','A conexão','Usuário','Senha','Sessão','Conta'];const error=known.some(x=>e.message?.startsWith(x))?e.message:'Não foi possível concluir. Tente novamente.';send(ws,{type:'social-response',rid:m.rid,ok:false,error});});
    });
    ws.on('close',()=>{const id=ws.socialId;if(!id)return;const connections=online.get(id);connections?.delete(ws);if(!connections?.size){online.delete(id);if(pool)list(id).then(({friends})=>friends.forEach(f=>emit(f.id,{kind:'refresh'}))).catch(()=>{});}});
  }
  return {attach,ready,close:()=>pool?.end()};
}
module.exports={createSocialHub};
