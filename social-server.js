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
const { createGroups } = require('./groups-server');
const MAX_IMAGE = 350000;
const MAX_FILE = 4 * 1024 * 1024;
const presenceValues = new Set(['online','away','busy']);
const validColor = value => typeof value === 'string' && /^#[0-9a-f]{6}$/i.test(value);
const mimeFor = name => { const ext = name.toLowerCase().split('.').pop(); return ({png:'image/png',jpg:'image/jpeg',jpeg:'image/jpeg',gif:'image/gif',webp:'image/webp'})[ext] || 'application/octet-stream'; };
function cleanAttachment(input) { if(!input || typeof input !== 'object' || typeof input.name !== 'string' || typeof input.data !== 'string' || input.data.length > Math.ceil(MAX_FILE/3)*4 || !/^[A-Za-z0-9+/]*={0,2}$/.test(input.data)) throw Error('Arquivo inválido ou maior que 4 MB.'); const bytes=Buffer.from(input.data,'base64'); if(!bytes.length || bytes.length>MAX_FILE || bytes.toString('base64')!==input.data) throw Error('Arquivo inválido ou maior que 4 MB.'); const name=input.name.replace(/[\x00-\x1f\x7f/\\]/g,'_').trim().slice(0,120)||'arquivo'; return {name,mime:mimeFor(name),size:bytes.length,data:input.data}; }
const isImage = x => typeof x === 'string' && (x === '' || (x.length <= MAX_IMAGE && /^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(x)));
const trimmed = (value, n) => typeof value === 'string' ? value.trim().slice(0,n) : '';
const publicUser = u => ({id:u.id, tag:u.tag, username:u.username, displayName:u.display_name, bio:u.bio, avatar:u.avatar, banner:u.banner, pronouns:u.pronouns||'', color:validColor(u.color)?u.color:'#ec8053', presence:presenceValues.has(u.presence)?u.presence:'online'});
function createSocialHub({pool = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL, max: 5, connectionTimeoutMillis: 10000 }) : null, getVoiceMembers=()=>[]} = {}) {
  const online = new Map();
  let groups;
  let registrationWindow = 0, registrationCount = 0, authWindow = 0, authCount = 0; const attempts = new Map();
  let initialization = null;
  function ready() {
    if (!initialization) initialization = pool.query(`CREATE TABLE IF NOT EXISTS evus_users (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, tag TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, bio TEXT NOT NULL DEFAULT '', avatar TEXT NOT NULL DEFAULT '', banner TEXT NOT NULL DEFAULT '')`).then(() => pool.query('ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS username TEXT')).then(() => pool.query('ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS password_hash TEXT')).then(() => pool.query('CREATE UNIQUE INDEX IF NOT EXISTS evus_users_username_idx ON evus_users(username)')).then(() => pool.query("ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS presence TEXT NOT NULL DEFAULT 'online'")).then(() => pool.query("ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS pronouns TEXT NOT NULL DEFAULT ''")).then(() => pool.query("ALTER TABLE evus_users ADD COLUMN IF NOT EXISTS color TEXT NOT NULL DEFAULT '#ec8053'")).then(() => pool.query('CREATE TABLE IF NOT EXISTS evus_sessions (token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES evus_users(id), expires_at BIGINT NOT NULL)')).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_friendships (a TEXT NOT NULL REFERENCES evus_users(id), b TEXT NOT NULL REFERENCES evus_users(id), requester TEXT NOT NULL REFERENCES evus_users(id), status TEXT NOT NULL, PRIMARY KEY (a,b))`)).then(() => pool.query(`CREATE TABLE IF NOT EXISTS evus_messages (id TEXT PRIMARY KEY, sender TEXT NOT NULL REFERENCES evus_users(id), recipient TEXT NOT NULL REFERENCES evus_users(id), body TEXT NOT NULL, created_at BIGINT NOT NULL)`)).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS read_at BIGINT')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS file_name TEXT')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS file_mime TEXT')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS file_size INTEGER')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS file_data TEXT')).then(() => pool.query('CREATE TABLE IF NOT EXISTS evus_blocks (blocker TEXT NOT NULL REFERENCES evus_users(id), blocked TEXT NOT NULL REFERENCES evus_users(id), PRIMARY KEY (blocker,blocked))')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS reply_to TEXT')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS edited_at BIGINT')).then(() => pool.query('ALTER TABLE evus_messages ADD COLUMN IF NOT EXISTS deleted_at BIGINT')).then(() => pool.query('CREATE TABLE IF NOT EXISTS evus_message_reactions (message_id TEXT NOT NULL REFERENCES evus_messages(id) ON DELETE CASCADE, user_id TEXT NOT NULL REFERENCES evus_users(id), emoji TEXT NOT NULL, PRIMARY KEY(message_id,user_id,emoji))')).then(() => groups.initialize()).catch(e => { initialization = null; console.error('Banco social indisponível:',e.message); throw e; });
    return initialization;
  }
  const send=(ws,x)=>{if(ws.readyState===1)ws.send(JSON.stringify(x));};
  const emit=(id,event)=>{for(const ws of online.get(id)||[])send(ws,{type:'social-event',event});};
  groups=createGroups({pool,emit,cleanAttachment,getVoiceMembers});
  const pair=(a,b)=>[a,b].sort();
  async function userById(id){return (await pool.query('SELECT * FROM evus_users WHERE id = $1',[id])).rows[0];}
  async function relationship(a,b){const [x,y]=pair(a,b);return (await pool.query('SELECT * FROM evus_friendships WHERE a = $1 AND b = $2',[x,y])).rows[0];}
  async function isBlocked(a,b){return (await pool.query('SELECT blocker FROM evus_blocks WHERE (blocker = $1 AND blocked = $2) OR (blocker = $2 AND blocked = $1)',[a,b])).rows.length>0;}
  const messageInfo=m=>({id:m.id,sender:m.sender,recipient:m.recipient,body:m.body,createdAt:Number(m.created_at),replyTo:m.reply_to||null,editedAt:m.edited_at?Number(m.edited_at):null,deletedAt:m.deleted_at?Number(m.deleted_at):null,attachment:m.file_name?{name:m.file_name,mime:m.file_mime,size:Number(m.file_size)}:null});
  async function list(userId){
    const links=(await pool.query('SELECT * FROM evus_friendships WHERE a = $1 OR b = $1',[userId])).rows;
    const friends=[],incoming=[],outgoing=[];
    for(const link of links){ const other=await userById(link.a===userId?link.b:link.a); if(!other||await isBlocked(userId,other.id))continue;const unread=(await pool.query('SELECT id FROM evus_messages WHERE sender = $1 AND recipient = $2 AND read_at IS NULL LIMIT 100',[other.id,userId])).rows.length;const item={...publicUser(other), online:online.has(other.id), unread}; if(link.status==='accepted')friends.push(item);else if(link.requester===userId)outgoing.push(item);else incoming.push(item); }
    return {friends,incoming,outgoing};
  }
  async function run(ws,msg){
    if(ws.readyState!==1)throw Error('A conexão foi encerrada.');if(!pool)throw Error('O servidor ainda não tem banco de dados configurado.'); await ready();
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
    if(action.startsWith('group-'))return groups.run(action,msg,self);
    if(action==='list')return list(self);
    if(action==='profile'){
      const current=await userById(self),name=trimmed(msg.displayName,32),bio=trimmed(msg.bio,500),pronouns=msg.pronouns === undefined ? current.pronouns : trimmed(msg.pronouns,40),color=msg.color === undefined ? current.color : msg.color;if(!name||!isImage(msg.avatar)||!isImage(msg.banner)||!validColor(color))throw Error('Confira o nome e as imagens do perfil.');
      await pool.query('UPDATE evus_users SET display_name = $1, bio = $2, avatar = $3, banner = $4, pronouns = $5, color = $6 WHERE id = $7',[name,bio,msg.avatar,msg.banner,pronouns,color,self]);
      const profile=publicUser(await userById(self));emit(self,{kind:'profile',user:profile});for(const f of (await list(self)).friends)emit(f.id,{kind:'refresh'});return {user:profile};
    }
    if(action==='request'){
      const tag=trimmed(msg.tag,32).toUpperCase();if(!/^EVUS-[0-9A-F]{12}$/.test(tag))throw Error('Código de amigo inválido.');
      const other=(await pool.query('SELECT * FROM evus_users WHERE tag = $1',[tag])).rows[0];if(!other||other.id===self||await isBlocked(self,other.id))throw Error('Não foi possível encontrar essa pessoa.');
      const [a,b]=pair(self,other.id);const found=await relationship(self,other.id);if(found)throw Error('Vocês já são amigos ou existe uma solicitação pendente.');
      await pool.query('INSERT INTO evus_friendships (a,b,requester,status) VALUES ($1,$2,$3,$4)',[a,b,self,'pending']);emit(other.id,{kind:'friend-request',from:publicUser(await userById(self))});return list(self);
    }
    if(action==='accept'||action==='remove'){
      const target=trimmed(msg.userId,64),link=await relationship(self,target);if(!link||await isBlocked(self,target))throw Error('Solicitação não encontrada.');
      const [a,b]=pair(self,target);
      if(action==='accept'){if(link.status!=='pending'||link.requester===self)throw Error('Não é possível aceitar esta solicitação.');await pool.query('UPDATE evus_friendships SET status = $1 WHERE a = $2 AND b = $3',['accepted',a,b]);}
      else await pool.query('DELETE FROM evus_friendships WHERE a = $1 AND b = $2',[a,b]);
      emit(target,{kind:'refresh'});return list(self);
    }
    if(action==='presence'){
      if(!presenceValues.has(msg.value))throw Error('Status inválido.');await pool.query('UPDATE evus_users SET presence = $1 WHERE id = $2',[msg.value,self]);const current=publicUser(await userById(self));emit(self,{kind:'profile',user:current});for(const f of (await list(self)).friends)emit(f.id,{kind:'refresh'});return {user:current};
    }
    if(action==='change-password'){
      const current=await userById(self);if(!validPassword(msg.currentPassword)||!validPassword(msg.newPassword))throw Error('Senha deve ter de 10 a 128 caracteres.');
      const now=Date.now();const bucket=attempts.get(current.username);if(bucket&&bucket.until>now&&bucket.count>=8)throw Error('Muitas tentativas. Aguarde alguns minutos.');
      if(!await passwordMatches(msg.currentPassword,current.password_hash)){const prev=attempts.get(current.username);attempts.set(current.username,{count:prev&&prev.until>now?prev.count+1:1,until:now+600000});throw Error('Senha atual incorreta.');}
      attempts.delete(current.username);const encoded=await passwordDigest(msg.newPassword);await pool.query('WITH changed AS (UPDATE evus_users SET password_hash = $1 WHERE id = $2 RETURNING id) DELETE FROM evus_sessions WHERE user_id IN (SELECT id FROM changed) AND token_hash <> $3',[encoded,self,ws.sessionHash]);for(const peer of online.get(self)||[])if(peer!==ws)peer.close(4003,'Senha alterada');return {};
    }
    if(action==='blocked-list'){
      const rows=(await pool.query('SELECT blocked FROM evus_blocks WHERE blocker = $1',[self])).rows;const users=[];for(const row of rows){const person=await userById(row.blocked);if(person)users.push(publicUser(person));}return {blocked:users};
    }
    if(action==='block'||action==='unblock'){
      const target=trimmed(msg.userId,64);if(!target||target===self||!await userById(target))throw Error('Usuário não encontrado.');
      if(action==='block'){await pool.query('INSERT INTO evus_blocks (blocker,blocked) VALUES ($1,$2) ON CONFLICT DO NOTHING',[self,target]);const [a,b]=pair(self,target);await pool.query('DELETE FROM evus_friendships WHERE a = $1 AND b = $2',[a,b]);emit(target,{kind:'refresh'});}
      else await pool.query('DELETE FROM evus_blocks WHERE blocker = $1 AND blocked = $2',[self,target]);
      return list(self);
    }
    if(action==='invite'){
      const other=trimmed(msg.userId,64),link=await relationship(self,other),room=trimmed(msg.room,64);if(!link||link.status!=='accepted'||await isBlocked(self,other))throw Error('Convites estão disponíveis somente para amigos.');
      if(!room)throw Error('Informe o nome da sala.');if(!online.has(other))throw Error('Seu amigo está offline. Combine a sala quando ele entrar.');
      emit(other,{kind:'invite',from:publicUser(await userById(self)),room});return {};
    }
    if(['history','message','mark-read','file','message-edit','message-delete','message-react','message-search'].includes(action)){
      const other=trimmed(msg.userId,64),link=await relationship(self,other);if(!link||link.status!=='accepted'||await isBlocked(self,other))throw Error('Esta conversa está disponível somente para amigos.');
      if(action==='message'){
        const body=trimmed(msg.text,4000),attachment=msg.attachment==null?null:cleanAttachment(msg.attachment);if(!body&&!attachment)throw Error('Escreva uma mensagem ou escolha um arquivo.');const now=Date.now(),id=randomUUID(),reply=trimmed(msg.replyTo,64)||null;if(reply){const original=(await pool.query('SELECT sender,recipient FROM evus_messages WHERE id = $1',[reply])).rows[0];if(!original||!((original.sender===self&&original.recipient===other)||(original.sender===other&&original.recipient===self)))throw Error('Mensagem para resposta não encontrada.');}
        await pool.query('INSERT INTO evus_messages (id,sender,recipient,body,created_at,file_name,file_mime,file_size,file_data,reply_to) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',[id,self,other,body,now,attachment?.name||null,attachment?.mime||null,attachment?.size||null,attachment?.data||null,reply]);
        const item={id,sender:self,recipient:other,body,createdAt:now,replyTo:reply,reactions:[],attachment:attachment?{name:attachment.name,mime:attachment.mime,size:attachment.size}:null};emit(self,{kind:'message',message:item});emit(other,{kind:'message',message:item});return {message:item};
      }
      if(['message-edit','message-delete','message-react'].includes(action)){
        const id=trimmed(msg.messageId,64), m=(await pool.query('SELECT * FROM evus_messages WHERE id = $1',[id])).rows[0];
        if(!m||!((m.sender===self&&m.recipient===other)||(m.sender===other&&m.recipient===self))||m.deleted_at)throw Error('Mensagem não encontrada.');
        if(action==='message-edit'){if(m.sender!==self)throw Error('Não é possível editar a mensagem de outra pessoa.');const body=trimmed(msg.text,4000);if(!body)throw Error('Escreva uma mensagem.');await pool.query('UPDATE evus_messages SET body=$1,edited_at=$2 WHERE id=$3 AND sender=$4',[body,Date.now(),id,self]);}
        if(action==='message-delete'){if(m.sender!==self)throw Error('Não é possível excluir a mensagem de outra pessoa.');await pool.query("UPDATE evus_messages SET body='',file_name=NULL,file_mime=NULL,file_size=NULL,file_data=NULL,deleted_at=$1 WHERE id=$2 AND sender=$3",[Date.now(),id,self]);await pool.query('DELETE FROM evus_message_reactions WHERE message_id=$1',[id]);}
        if(action==='message-react'){const emoji=msg.emoji;if(!['👍','❤️','😂','😮','😢','🎉'].includes(emoji))throw Error('Reação inválida.');const found=await pool.query('DELETE FROM evus_message_reactions WHERE message_id=$1 AND user_id=$2 AND emoji=$3 RETURNING emoji',[id,self,emoji]);if(!found.rowCount)await pool.query('INSERT INTO evus_message_reactions (message_id,user_id,emoji) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING',[id,self,emoji]);}
        emit(self,{kind:'conversation-changed',userId:other});emit(other,{kind:'conversation-changed',userId:self});return {};
      }
      if(action==='message-search'){
        const term=trimmed(msg.query,100);if(!term)throw Error('Informe o texto para pesquisar.');
        const rows=(await pool.query("SELECT * FROM evus_messages WHERE ((sender=$1 AND recipient=$2) OR (sender=$2 AND recipient=$1)) AND deleted_at IS NULL AND strpos(lower(body),lower($3))>0 ORDER BY created_at DESC LIMIT 100",[self,other,term])).rows;return {messages:rows.reverse().map(messageInfo)};
      }
      if(action==='file'){
        const id=trimmed(msg.messageId,64);const m=(await pool.query('SELECT * FROM evus_messages WHERE id = $1',[id])).rows[0];if(!m||!m.file_data||!((m.sender===self&&m.recipient===other)||(m.sender===other&&m.recipient===self)))throw Error('Arquivo não encontrado.');return {attachment:{name:m.file_name,mime:m.file_mime,size:Number(m.file_size),data:m.file_data}};
      }
      await pool.query('UPDATE evus_messages SET read_at = $1 WHERE sender = $2 AND recipient = $3 AND read_at IS NULL',[Date.now(),other,self]);
      if(action==='mark-read')return {};
      const rows=(await pool.query('SELECT id,sender,recipient,body,created_at,file_name,file_mime,file_size,reply_to,edited_at,deleted_at FROM evus_messages WHERE (sender = $1 AND recipient = $2) OR (sender = $2 AND recipient = $1) ORDER BY created_at DESC LIMIT 100',[self,other])).rows;
      const items=rows.reverse().map(messageInfo);for(const item of items){item.reactions=(await pool.query('SELECT user_id,emoji FROM evus_message_reactions WHERE message_id=$1',[item.id])).rows.map(r=>({userId:r.user_id,emoji:r.emoji}));}return {messages:items};
    }
    throw Error('Operação desconhecida.');
  }
  function attach(ws){
    let count=0,started=Date.now();
    ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return}if(m?.type!=='social')return;
      if(Date.now()-started>60000){started=Date.now();count=0}if(++count>100){send(ws,{type:'social-response',rid:m.rid,ok:false,error:'Muitas solicitações. Aguarde um pouco.'});return;}
      if(typeof m.rid!=='string'||m.rid.length>64)return;
      (ws.socialQueue ||= Promise.resolve());ws.socialQueue=ws.socialQueue.then(()=>run(ws,m)).then(result=>send(ws,{type:'social-response',rid:m.rid,ok:true,result})).catch(e=>{const known=['Código','Informe','Esta','Não','O servidor','Vocês','Solicitação','Escreva','Confira','Operação','Muitas','A conexão','Usuário','Senha','Sessão','Conta','Status','Arquivo','Convites','Seu amigo'];const error=known.some(x=>e.message?.startsWith(x))?e.message:'Não foi possível concluir. Tente novamente.';send(ws,{type:'social-response',rid:m.rid,ok:false,error});});
    });
    ws.on('close',()=>{const id=ws.socialId;if(!id)return;const connections=online.get(id);connections?.delete(ws);if(!connections?.size){online.delete(id);if(pool)list(id).then(({friends})=>friends.forEach(f=>emit(f.id,{kind:'refresh'}))).catch(()=>{});}});
  }
  return {attach,ready,authorizeVoice:async(session,channelId)=>{if(!pool)return false;await ready();return groups.authorizeVoice(session,channelId);},membership:groups.membership,notifyVoice:(groupId,channelId)=>groups.broadcast(groupId,{kind:'group-voice-changed',groupId,channelId}),close:()=>pool?.end()};
}
module.exports={createSocialHub};
