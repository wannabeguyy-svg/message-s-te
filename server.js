const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PUB = path.join(__dirname, "public");
const DATA = process.env.DATA_DIR || __dirname;
const DB_FILE = path.join(DATA, "data.json"), AUD = path.join(DATA, "audio");
let pool = null;

// ---- Sorular (cevaplar sadece sunucuda durur) ----
const QUIZ = [
  { q: "hem ağır hem?", a: "Sağlam", o: ["Sağlam", "Sağlık", "Sağır", "Salam"] },
  { q: "ilim çeşidi?", a: "Ledun", o: ["Ledun", "Ledin", "Ladun", "Lüdun"] },
  { q: "değil işte değil cici kardeşim sol yanım bir tanem ……………'ım.", a: "Justin Bieber", o: ["Justin Bieber", "Justin Biber", "Justin Timberlake", "Justine Bieber"] }
];
const shuffle = a => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };

// ---- Kayıt (Postgres varsa orada, yoksa dosyada) ----
let db = { members: [], msgs: [], seq: 0, audios: {}, secret: "" };
let timer = null;
function flush() {
  const s = JSON.stringify(db);
  if (pool) pool.query("INSERT INTO kv(k,v) VALUES('state',$1) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v", [s]).catch(e => console.log("Kayit hatasi:", e.message));
  else fs.writeFile(DB_FILE, s, () => {});
}
function save() { clearTimeout(timer); timer = setTimeout(flush, 400); }
async function init() {
  if (process.env.DATABASE_URL) {
    try {
      const { Pool } = require("pg");
      pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3 });
      pool.on("error", () => {});
      await pool.query("CREATE TABLE IF NOT EXISTS kv(k text PRIMARY KEY, v jsonb NOT NULL)");
      await pool.query("CREATE TABLE IF NOT EXISTS blobs(id text PRIMARY KEY, mime text NOT NULL, data bytea NOT NULL)");
      const r = await pool.query("SELECT v FROM kv WHERE k='state'");
      if (r.rows[0]) db = Object.assign(db, r.rows[0].v);
      console.log("Veritabani bagli");
    } catch (e) { console.log("DB baglanamadi, dosyaya donuluyor:", e.message); pool = null; }
  }
  if (!pool) {
    fs.mkdirSync(AUD, { recursive: true });
    try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, "utf8"))); } catch (e) {}
    console.log("UYARI: DATABASE_URL yok, kayitlar kalici degil");
  }
  if (!Array.isArray(db.msgs)) db.msgs = [];
  if (!Array.isArray(db.members)) db.members = [];
  if (!db.audios || typeof db.audios !== "object") db.audios = {};
  if (!db.secret) { db.secret = crypto.randomBytes(32).toString("hex"); save(); }
}
async function putBlob(id, mime, buf) {
  if (pool) await pool.query("INSERT INTO blobs(id,mime,data) VALUES($1,$2,$3)", [id, mime, buf]);
  else await fs.promises.writeFile(path.join(AUD, id), buf);
}
async function getBlob(id) {
  if (pool) { const r = await pool.query("SELECT mime,data FROM blobs WHERE id=$1", [id]); return r.rows[0] || null; }
  try { return { mime: db.audios[id], data: await fs.promises.readFile(path.join(AUD, id)) }; } catch (e) { return null; }
}
function dropAudio(id) {
  delete db.audios[id];
  if (pool) pool.query("DELETE FROM blobs WHERE id=$1", [id]).catch(() => {});
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
const TYPES = { ".html": "text/html; charset=utf-8", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif", ".webp": "image/webp", ".mp4": "video/mp4", ".webm": "video/webm" };
const AUDIO_OK = ["audio/webm", "audio/mp4", "audio/ogg", "audio/mpeg", "audio/aac", "audio/x-m4a"];
const IMG_OK = ["image/jpeg", "image/png", "image/webp", "image/gif"];
const clamp = (s, n) => String(s || "").slice(0, n);
const sig = s => crypto.createHmac("sha256", db.secret).update(s).digest("hex").slice(0, 32);
const mkToken = cid => { const p = cid + "." + (Date.now() + 864e5); return p + "." + sig(p); };
function sess(t) {
  if (typeof t !== "string") return null;
  const a = t.split(".");
  if (a.length !== 3) return null;
  const good = Buffer.from(sig(a[0] + "." + a[1])), got = Buffer.from(a[2]);
  if (good.length !== got.length || !crypto.timingSafeEqual(good, got) || +a[1] < Date.now()) return null;
  const i = db.members.indexOf(a[0]);
  return i < 0 ? null : { cid: a[0], idx: i };
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
    if (e || !s.isFile()) { res.writeHead(404); return res.end("Yok"); }
    const h = { "Content-Type": type, "Accept-Ranges": "bytes", "Cache-Control": cache };
    const r = req.headers.range ? parseRange(req.headers.range, s.size) : null;
    if (r === "bad") { res.writeHead(416, { "Content-Range": "bytes */" + s.size }); return res.end(); }
    if (r) { res.writeHead(206, Object.assign(h, { "Content-Range": `bytes ${r[0]}-${r[1]}/${s.size}`, "Content-Length": r[1] - r[0] + 1 })); return fs.createReadStream(f, { start: r[0], end: r[1] }).pipe(res); }
    res.writeHead(200, Object.assign(h, { "Content-Length": s.size })); fs.createReadStream(f).pipe(res);
  });
}

// ---- HTTP ----
const server = http.createServer((req, res) => {
  let u; try { u = new URL(req.url, "http://x"); } catch (e) { res.writeHead(400); return res.end(); }
  const url = u.pathname, q = u.searchParams;
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();

  if (req.method === "GET" && url === "/api/quiz") {
    return json(res, 200, { qs: QUIZ.map(x => ({ q: x.q, o: shuffle(x.o) })) });
  }
  if (req.method === "POST") {
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
        const ans = Array.isArray(body.ans) ? body.ans : [], cid = clamp(body.cid, 32);
        if (!QUIZ.every((x, i) => ans[i] === x.a)) { addFail(ip); return json(res, 401, { error: "Yanlış cevap" }); }
        if (!/^[a-z0-9]{8,32}$/.test(cid)) return json(res, 400, { error: "Geçersiz istek" });
        if (!db.members.includes(cid)) {
          if (db.members.length >= 2) return json(res, 403, { error: "Bu sohbet dolu" });
          db.members.push(cid); save();
        }
        json(res, 200, { token: mkToken(cid) });
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
  sendFile(req, res, f, type, type.startsWith("text/html") ? "no-cache" : "public, max-age=3600");
});

// ---- WebSocket ----
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const online = new Set();
const send = (w, o) => { if (w.readyState === 1) w.send(JSON.stringify(o)); };
const broadcast = o => online.forEach(w => send(w, o));
const presence = () => online.forEach(w => send(w, { t: "p", other: [...online].some(o => o.cid !== w.cid) }));

wss.on("connection", ws => {
  ws.alive = true; ws.win = [];
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!ws.cid) {
      const s = m.t === "auth" ? sess(m.token) : null;
      if (s) {
        ws.cid = s.cid; ws.idx = s.idx; online.add(ws);
        send(ws, { t: "ready", me: ws.idx, msgs: db.msgs.slice(-100) });
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
    } else if (m.t === "read") {
      const ids = [];
      db.msgs.forEach(x => { if (x.f !== ws.idx && !x.r) { x.r = 1; ids.push(x.i); } });
      if (ids.length) { save(); broadcast({ t: "read", ids }); }
    }
  });
  ws.on("close", () => { online.delete(ws); presence(); });
});

setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
}, 30000);

process.on("SIGTERM", () => { clearTimeout(timer); flush(); setTimeout(() => process.exit(0), 1000); });

init().then(() => server.listen(process.env.PORT || 3000, () => console.log("Hazir")));
