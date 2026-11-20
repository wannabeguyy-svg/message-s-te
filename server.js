const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PUB = path.join(__dirname, "public");
const DATA = process.env.DATA_DIR || __dirname;
const DB_FILE = path.join(DATA, "data.json"), AUD = path.join(DATA, "audio");
const VERSION = "5.2";
const NAMES = ["Stannis", "Harry"];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let pool = null, lastDbError = "";

// ---- Sorular (cevaplar sadece sunucuda durur) ----
const QUIZ = [
  { q: "hem ağır hem?", a: "Sağlam", o: ["Sağlam", "Sağlık", "Sağır", "Salam"] },
  { q: "ilim çeşidi?", a: "Ledun", o: ["Ledun", "Ledin", "Ladun", "Lüdun"] },
  { q: "değil işte değil cici kardeşim sol yanım bir tanem ……………'ım.", a: "Justin Bieber", o: ["Justin Bieber", "Justin Biber", "Justin Timberlake", "Justine Bieber"] }
];
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };

// ---- Sabit kişi PIN'leri (Render > Environment: HARRY_PIN, STANNIS_PIN) ----
const ENVPIN = { Stannis: process.env.STANNIS_PIN, Harry: process.env.HARRY_PIN };
const validPin = p => typeof p === "string" && /^\d{4,8}$/.test(p);
const ENV_BOTH = validPin(ENVPIN.Stannis) && validPin(ENVPIN.Harry) && ENVPIN.Stannis !== ENVPIN.Harry;
if (process.env.STANNIS_PIN || process.env.HARRY_PIN) {
  if (!ENV_BOTH) console.log("UYARI: HARRY_PIN ve STANNIS_PIN ikisi de tanimli, 4-8 rakam ve birbirinden farkli olmali");
}

// ---- Kayıt (Postgres varsa orada, yoksa dosyada) ----
let db = { members: [], who: {}, devs: {}, pins: {}, ntok: {}, win: null, v: 0, msgs: [], seq: 0, audios: {}, secret: "" };
let timer = null, chain = Promise.resolve();
const UPSERT = "INSERT INTO kv(k,v) VALUES('state',$1) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v";
async function pq(sql, params) {            // başarısız olursa birkaç kez yeniden dener
  for (let i = 0; ; i++) {
    try { const r = await pool.query(sql, params); lastDbError = ""; return r; }
    catch (e) { lastDbError = String(e.message).slice(0, 120); if (i >= 3) throw e; await sleep(700 * (i + 1)); }
  }
}
async function doFlush() {
  const s = JSON.stringify(db);
  try {
    if (pool) await pq(UPSERT, [s]);
    else await fs.promises.writeFile(DB_FILE, s);
  } catch (e) { console.log("Kayit hatasi:", e.message); clearTimeout(timer); timer = setTimeout(flush, 5000); }
}
const flush = () => (chain = chain.then(doFlush));
function save() { clearTimeout(timer); timer = setTimeout(flush, 400); }
const nameList = () => NAMES;

async function connectDb() {
  const { Pool } = require("pg");
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3, idleTimeoutMillis: 20000, connectionTimeoutMillis: 15000, keepAlive: true });
  pool.on("error", () => {});
  await pq("CREATE TABLE IF NOT EXISTS kv(k text PRIMARY KEY, v jsonb NOT NULL)");
  await pq("CREATE TABLE IF NOT EXISTS blobs(id text PRIMARY KEY, mime text NOT NULL, data bytea NOT NULL)");
  const r = await pq("SELECT v FROM kv WHERE k='state'");
  if (r.rows[0]) db = Object.assign(db, r.rows[0].v);
}
async function init() {
  if (process.env.DATABASE_URL) {
    for (let i = 0; i < 4; i++) {
      try { await connectDb(); console.log("Veritabani bagli"); break; }
      catch (e) {
        console.log("DB baglanamadi (deneme " + (i + 1) + "):", e.message);
        lastDbError = String(e.message).slice(0, 120);
        try { await pool.end(); } catch (x) {}
        pool = null; await sleep(3000);
      }
    }
  } else lastDbError = "DATABASE_URL tanımlı değil";
  if (!pool) {
    fs.mkdirSync(AUD, { recursive: true });
    try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, "utf8"))); } catch (e) {}
    console.log("UYARI: Veritabani yok, kayitlar kalici degil");
  }
  if (!Array.isArray(db.msgs)) db.msgs = [];
  if (!Array.isArray(db.members)) db.members = [];
  if (!db.who || typeof db.who !== "object") db.who = {};
  for (const k of ["devs", "pins", "ntok"]) if (!db[k] || typeof db[k] !== "object") db[k] = {};
  if (db.v !== 5) {   // eski sürümden geçiş: cihaz sırası yerine kişi sırası (Stannis=0, Harry=1)
    const map = db.members.map(c => NAMES.indexOf(db.who[c]));
    db.devs = {};   // eski cihaz-kişi eşleşmeleri karışık olabilir, herkes baştan seçsin
    db.msgs.forEach(x => {
      if (map[x.f] >= 0) x.f = map[x.f];
      if (x.h) x.h = x.h.map(i => (map[i] >= 0 ? map[i] : i));
    });
    db.v = 5; save();
  }
  if (!db.audios || typeof db.audios !== "object") db.audios = {};
  if (!db.secret) { db.secret = crypto.randomBytes(32).toString("hex"); save(); }
  applyEnvPins();
}
function applyEnvPins() {            // PIN'ler değiştiyse bütün cihaz bağlantıları ve oturumlar sıfırlanır
  let sg = "";
  for (const n of NAMES) {
    const p = ENVPIN[n];
    if (!validPin(p)) continue;
    const salt = crypto.createHash("sha256").update("pin|" + n + "|" + db.secret).digest("hex").slice(0, 16);
    db.pins[n] = { salt, hash: hashPin(p, salt), env: 1 };
    sg += n + ":" + db.pins[n].hash + ";";
  }
  if (sg && db.envSig !== sg) { db.devs = {}; db.win = null; db.envSig = sg; save(); }
}
async function putBlob(id, mime, buf) {
  if (pool) await pq("INSERT INTO blobs(id,mime,data) VALUES($1,$2,$3)", [id, mime, buf]);
  else await fs.promises.writeFile(path.join(AUD, id), buf);
}
async function getBlob(id) {
  if (pool) { const r = await pq("SELECT mime,data FROM blobs WHERE id=$1", [id]); return r.rows[0] || null; }
  try { return { mime: db.audios[id], data: await fs.promises.readFile(path.join(AUD, id)) }; } catch (e) { return null; }
}
function dropAudio(id) {
  delete db.audios[id];
  if (pool) pq("DELETE FROM blobs WHERE id=$1", [id]).catch(() => {});
  else fs.unlink(path.join(AUD, id), () => {});
}
function trim() {
  while (db.msgs.length > 2000) { const o = db.msgs.shift(); if (o.a) dropAudio(o.a); }
  for (const [k, cap, label] of [["a", 400, "eski ses mesajı"], ["i", 300, "eski fotoğraf"]]) {
    let n = db.msgs.filter(x => x.k === k && x.a).length;
    for (const x of db.msgs) {
      if (n <= cap) break;
      if (x.k === k && x.a) { dropAudio(x.a); x.a = null; x.k = "t"; x.x = "(" + label + " silindi)"; n--; }
    }
  }
}

// ---- Yardımcılar ----
const TYPES = { ".html": "text/html; charset=utf-8", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp", ".mp4": "video/mp4", ".webm": "video/webm", ".js": "text/javascript; charset=utf-8", ".json": "application/manifest+json" };
const AUDIO_OK = ["audio/webm", "audio/mp4", "audio/ogg", "audio/mpeg", "audio/aac", "audio/x-m4a"];
const IMG_OK = ["image/jpeg", "image/png", "image/webp", "image/gif"];
const clamp = (s, n) => String(s || "").slice(0, n);
const sig = s => crypto.createHmac("sha256", db.secret).update(s).digest("hex").slice(0, 32);
const hashPin = (pin, salt) => crypto.scryptSync(pin, salt, 32).toString("hex");
const pinOk = (p, pin) => { const a = Buffer.from(hashPin(pin, p.salt)), b = Buffer.from(p.hash); return a.length === b.length && crypto.timingSafeEqual(a, b); };
function newWin() { return (db.win = { id: crypto.randomBytes(6).toString("hex"), end: Date.now() + 36e5 }); }
const mkToken = name => { const p = name + "." + db.win.id + "." + db.win.end; return p + "." + sig(p); };
function sess(t) {
  if (typeof t !== "string") return null;
  const a = t.split(".");
  if (a.length !== 4) return null;
  const good = Buffer.from(sig(a.slice(0, 3).join("."))), got = Buffer.from(a[3]);
  if (good.length !== got.length || !crypto.timingSafeEqual(good, got)) return null;
  if (!db.win || a[1] !== db.win.id || Date.now() >= db.win.end) return null;
  const idx = NAMES.indexOf(a[0]);
  return idx < 0 ? null : { name: a[0], idx, exp: db.win.end };
}
const fails = new Map();
const failed = ip => { const t = fails.get(ip); return t && Date.now() < t.reset && t.n >= 6; };
function addFail(ip) {
  let t = fails.get(ip);
  if (!t || Date.now() > t.reset) t = { n: 0, reset: Date.now() + 15 * 60 * 1000 };
  t.n++; fails.set(ip, t);
}
const json = (res, code, o) => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(o)); };
function readBody(req, cb) {
  let b = "";
  req.setEncoding("utf8");
  req.on("data", d => { b += d; if (b.length > 2000) req.destroy(); });
  req.on("end", () => { try { cb(JSON.parse(b)); } catch (e) { cb(null); } });
}
function parseRange(rg, size) {
  const m = /bytes=(\d*)-(\d*)/.exec(rg || "");
  if (!m) return null;
  let a = 0, b = size - 1;
  if (m[1]) { a = +m[1]; if (m[2]) b = +m[2]; } else if (m[2]) a = Math.max(0, size - +m[2]);
  b = Math.min(b, size - 1);
  return a > b || a >= size ? "bad" : [a, b];
}
function sendBuf(req, res, buf, type, cache) {
  const h = { "Content-Type": type, "Accept-Ranges": "bytes", "Cache-Control": cache };
  const r = req.headers.range ? parseRange(req.headers.range, buf.length) : null;
  if (r === "bad") { res.writeHead(416, { "Content-Range": "bytes */" + buf.length }); return res.end(); }
  if (r) { res.writeHead(206, Object.assign(h, { "Content-Range": `bytes ${r[0]}-${r[1]}/${buf.length}`, "Content-Length": r[1] - r[0] + 1 })); return res.end(buf.subarray(r[0], r[1] + 1)); }
  res.writeHead(200, Object.assign(h, { "Content-Length": buf.length })); res.end(buf);
}
function sendFile(req, res, f, type, cache) {
  fs.stat(f, (e, s) => {
    if (e || !s.isFile()) { res.writeHead(404, { "Cache-Control": "public, max-age=120" }); return res.end("Yok"); }
    const h = { "Content-Type": type, "Accept-Ranges": "bytes", "Cache-Control": cache };
    const r = req.headers.range ? parseRange(req.headers.range, s.size) : null;
    if (r === "bad") { res.writeHead(416, { "Content-Range": "bytes */" + s.size }); return res.end(); }
    if (r) { res.writeHead(206, Object.assign(h, { "Content-Range": `bytes ${r[0]}-${r[1]}/${s.size}`, "Content-Length": r[1] - r[0] + 1 })); return fs.createReadStream(f, { start: r[0], end: r[1] }).pipe(res); }
    res.writeHead(200, Object.assign(h, { "Content-Length": s.size })); fs.createReadStream(f).pipe(res);
  });
}

// ---- Bildirim yoklaması ve toplu çıkış ----
const pollers = new Set();
function answerPoll(p) {
  if (p.done) return;
  p.done = true; clearTimeout(p.t); pollers.delete(p);
  const items = db.msgs.filter(x => x.i > p.since && x.f !== p.idx && !(x.h && x.h.includes(p.idx))).map(x => ({ from: NAMES[x.f] }));
  json(p.res, 200, { last: db.seq, items });
}
function expireStale() {
  online.forEach(w => { if (!db.win || w.exp !== db.win.end || Date.now() >= db.win.end) { send(w, { t: "expired" }); w.close(); } });
}

// ---- HTTP ----
const server = http.createServer((req, res) => {
  let u; try { u = new URL(req.url, "http://x"); } catch (e) { res.writeHead(400); return res.end(); }
  const url = u.pathname, q = u.searchParams;
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();

  if (req.method === "GET" && url === "/api/quiz") {
    return json(res, 200, { qs: QUIZ.map(x => ({ q: x.q, o: shuffle(x.o) })) });
  }
  if (req.method === "GET" && url === "/api/poll") {
    const n = db.ntok[q.get("d")];
    if (!n) return json(res, 401, { error: "Geçersiz" });
    const p = { res, idx: NAMES.indexOf(n.name), since: +q.get("since") || 0, done: false };
    if (db.msgs.some(x => x.i > p.since && x.f !== p.idx && !(x.h && x.h.includes(p.idx)))) return answerPoll(p);
    p.t = setTimeout(() => answerPoll(p), 25000);
    pollers.add(p);
    res.on("close", () => { p.done = true; clearTimeout(p.t); pollers.delete(p); });
    return;
  }
  if (req.method === "POST") {
    if (url === "/api/devtoken") {
      const s = sess(q.get("t"));
      if (!s) return json(res, 401, { error: "Giriş gerekli" });
      return readBody(req, body => {
        const cid = clamp(body && body.cid, 32);
        let tok = Object.keys(db.ntok).find(k => db.ntok[k].cid === cid && db.ntok[k].name === s.name);
        if (!tok) {
          tok = crypto.randomBytes(20).toString("hex");
          db.ntok[tok] = { name: s.name, cid };
          const ks = Object.keys(db.ntok); if (ks.length > 8) delete db.ntok[ks[0]];
          save();
        }
        json(res, 200, { token: tok, last: db.seq });
      });
    }
    if (url === "/api/setpin") {
      const s = sess(q.get("t"));
      if (!s) return json(res, 401, { error: "Giriş gerekli" });
      return readBody(req, body => {
        const pin = clamp(body && body.pin, 12);
        if (validPin(ENVPIN[s.name])) return json(res, 403, { error: "Bu PIN Render ayarlarından yönetiliyor." });
        if (!/^\d{4,8}$/.test(pin)) return json(res, 400, { error: "PIN 4-8 rakam olmalı." });
        const salt = crypto.randomBytes(8).toString("hex");
        db.pins[s.name] = { salt, hash: hashPin(pin, salt) }; save();
        json(res, 200, {});
      });
    }
    if (url === "/api/unbind") {
      const s = sess(q.get("t"));
      if (!s) return json(res, 401, { error: "Giriş gerekli" });
      return readBody(req, body => {
        const cid = clamp(body && body.cid, 32);
        if (db.devs[cid] === s.name) { delete db.devs[cid]; save(); }
        json(res, 200, {});
      });
    }
    if (url === "/api/logoutall") {
      const s = sess(q.get("t"));
      if (!s) return json(res, 401, { error: "Giriş gerekli" });
      db.win.end = Date.now(); db.ntok = {}; save(); expireStale();
      return json(res, 200, {});
    }
    if (url === "/api/up") {
      const s = sess(q.get("t"));
      if (!s) return json(res, 401, { error: "Giriş gerekli" });
      const mime = (req.headers["content-type"] || "").split(";")[0].trim();
      const max = AUDIO_OK.includes(mime) ? 1300000 : IMG_OK.includes(mime) ? 3500000 : 0;
      if (!max) return json(res, 400, { error: "Dosya biçimi desteklenmiyor" });
      const chunks = []; let n = 0, bad = false;
      req.on("data", d => { n += d.length; if (n > max) { bad = true; req.destroy(); } else chunks.push(d); });
      req.on("end", async () => {
        if (bad || !n) return json(res, 400, { error: "Dosya çok büyük veya boş" });
        const id = crypto.randomBytes(12).toString("hex");
        try { await putBlob(id, mime, Buffer.concat(chunks)); } catch (e) { return json(res, 500, { error: "Kaydedilemedi" }); }
        db.audios[id] = mime; save();
        json(res, 200, { id });
      });
      return;
    }
    if (url === "/api/enter") {
      return readBody(req, body => {
        if (!body) return json(res, 400, { error: "Geçersiz istek" });
        if (failed(ip)) return json(res, 429, { error: "Çok fazla yanlış deneme. 15 dakika sonra tekrar dene." });
        const ans = Array.isArray(body.ans) ? body.ans : [], cid = clamp(body.cid, 32), who = clamp(body.who, 12), pin = clamp(body.pin, 12);
        if (!QUIZ.every((x, i) => ans[i] === x.a)) { addFail(ip); return json(res, 401, { error: "Sikildin ve atıldın." }); }
        if (!/^[a-z0-9]{8,32}$/.test(cid)) return json(res, 400, { error: "Geçersiz istek" });
        const okPin = /^\d{4,8}$/.test(pin);
        const setPin = n => { const salt = crypto.randomBytes(8).toString("hex"); db.pins[n] = { salt, hash: hashPin(pin, salt) }; };
        let name = db.devs[cid];
        if (!name && ENV_BOTH) {                         // sabit PIN'ler: PIN kişiyi belirler
          if (!okPin) return json(res, 200, { need: "pin" });
          name = NAMES.find(n => db.pins[n] && pinOk(db.pins[n], pin));
          if (!name) { addFail(ip); return json(res, 422, { error: "PIN yanlış." }); }
          db.devs[cid] = name;
        } else if (!name) {                              // yeni cihaz: kişi ve PIN gerekir
          if (!NAMES.includes(who)) return json(res, 200, { need: "who", names: NAMES });
          if (!db.pins[who]) {
            if (Object.values(db.devs).includes(who)) return json(res, 403, { error: "Önce bu kişinin kayıtlı cihazından PIN belirlenmeli." });
            if (!okPin) return json(res, 200, { need: "setpin", who });
            setPin(who);
          } else {
            if (!okPin) return json(res, 200, { need: "pin", who });
            if (!pinOk(db.pins[who], pin)) { addFail(ip); return json(res, 422, { error: "PIN yanlış." }); }
          }
          db.devs[cid] = name = who;
        } else if (!db.pins[name]) {                     // kayıtlı cihaz ama PIN henüz yok
          if (!okPin) return json(res, 200, { need: "setpin", who: name });
          setPin(name);
        }
        if (!db.win || Date.now() >= db.win.end) newWin();
        save();
        json(res, 200, { token: mkToken(name) });
      });
    }
    res.writeHead(404); return res.end();
  }

  if (url.startsWith("/a/")) {
    const id = url.slice(3);
    if (!/^[a-f0-9]{24}$/.test(id) || !sess(q.get("t")) || !db.audios[id]) { res.writeHead(404); return res.end("Yok"); }
    return getBlob(id).then(b => {
      if (!b) { res.writeHead(404); return res.end("Yok"); }
      sendBuf(req, res, b.data, b.mime, "private, max-age=86400");
    }).catch(() => { res.writeHead(500); res.end(); });
  }

  const p = url === "/" ? "/index.html" : url;
  const f = path.join(PUB, path.normalize(p));
  const type = TYPES[path.extname(f).toLowerCase()];
  if (!f.startsWith(PUB) || !type) { res.writeHead(404); return res.end("Bulunamadi"); }
  sendFile(req, res, f, type, type.startsWith("text/") || type.includes("json") ? "no-cache" : "public, max-age=3600");
});

// ---- WebSocket ----
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const online = new Set();
const send = (w, o) => { if (w.readyState === 1) w.send(JSON.stringify(o)); };
const broadcast = o => online.forEach(w => send(w, o));
const presence = () => online.forEach(w => send(w, { t: "p", other: [...online].some(o => o.name !== w.name) }));

wss.on("connection", ws => {
  ws.alive = true; ws.win = [];
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!ws.name) {
      const s = m.t === "auth" ? sess(m.token) : null;
      if (s) {
        ws.name = s.name; ws.idx = s.idx; ws.exp = s.exp; online.add(ws);
        const mine = db.msgs.filter(x => !(x.h && x.h.includes(ws.idx))).slice(-100).map(x => { const { h, ...y } = x; return y; });
        send(ws, { t: "ready", v: VERSION, exp: ws.exp, me: ws.idx, names: nameList(), store: pool ? "db" : "file", dberr: lastDbError, msgs: mine });
        presence();
      } else { send(ws, { t: "bad" }); ws.close(); }
      return;
    }
    const now = Date.now();
    ws.win = ws.win.filter(x => now - x < 5000);
    if (ws.win.length >= 15) return;
    ws.win.push(now);

    if (m.t === "msg") {
      const x = { i: ++db.seq, f: ws.idx, s: now, r: 0 };
      if (m.kind === "audio" || m.kind === "image") {
        const mt = typeof m.aid === "string" ? db.audios[m.aid] : null;
        if (typeof mt !== "string" || !mt.startsWith(m.kind === "audio" ? "audio/" : "image/")) return;
        x.k = m.kind === "audio" ? "a" : "i"; x.a = m.aid;
        if (x.k === "a") x.d = Math.max(0, Math.min(60, +m.dur || 0));
      } else {
        const text = clamp(m.text, 1000).trim();
        if (!text) return;
        x.k = "t"; x.x = text;
      }
      db.msgs.push(x); trim(); save();
      broadcast({ t: "msg", m: x });
      pollers.forEach(p => { if (p.idx !== x.f) answerPoll(p); });
    } else if (m.t === "read") {
      const ids = [];
      db.msgs.forEach(x => { if (x.f !== ws.idx && !x.r) { x.r = 1; ids.push(x.i); } });
      if (ids.length) { save(); broadcast({ t: "read", ids }); }
    } else if (m.t === "del") {
      const j = db.msgs.findIndex(y => y.i === +m.id), x = db.msgs[j];
      if (!x) return;
      if (m.mode === "all") {
        if (x.f !== ws.idx || x.del) return;
        if (x.a) dropAudio(x.a);
        x.a = null; x.k = "t"; x.x = "Bu mesaj silindi."; x.del = 1; delete x.d;
        save(); const { h, ...y } = x; broadcast({ t: "upd", m: y });
      } else if (m.mode === "me") {
        x.h = x.h || [];
        if (!x.h.includes(ws.idx)) x.h.push(ws.idx);
        if (x.h.length >= 2) { if (x.a) dropAudio(x.a); db.msgs.splice(j, 1); }
        save(); online.forEach(w => { if (w.name === ws.name) send(w, { t: "gone", id: x.i }); });
      }
    }
  });
  ws.on("close", () => { online.delete(ws); presence(); });
});

setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
  expireStale();
}, 30000);

setInterval(expireStale, 5000);

process.on("SIGTERM", async () => { clearTimeout(timer); setTimeout(() => process.exit(0), 8000); await flush(); process.exit(0); });

init().then(() => server.listen(process.env.PORT || 3000, () => console.log("Hazir")));
