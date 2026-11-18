const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PUB = path.join(__dirname, "public");
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, "data.json");
let db = { users: {}, sessions: {}, msgs: {} };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, "utf8"))); } catch (e) {}
let timer = null;
function save() { clearTimeout(timer); timer = setTimeout(() => fs.writeFile(DB_FILE, JSON.stringify(db), () => {}), 500); }

// --- yardımcılar ---
const NICK = /^[A-Za-z0-9_]{3,20}$/;
const hash = (pw, salt) => crypto.scryptSync(pw, salt, 64).toString("hex");
const sameHash = (a, b) => { const x = Buffer.from(a, "hex"), y = Buffer.from(b, "hex"); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const clamp = (s, n) => String(s || "").slice(0, n);
const convKey = (a, b) => [a, b].sort().join(":");

const tries = new Map();
function limited(ip) {
  const now = Date.now(); let t = tries.get(ip);
  if (!t || now > t.reset) t = { n: 0, reset: now + 15 * 60 * 1000 };
  t.n++; tries.set(ip, t);
  return t.n > 15;
}

function readBody(req, cb) {
  let b = "";
  req.on("data", d => { b += d; if (b.length > 2000) req.destroy(); });
  req.on("end", () => { try { cb(JSON.parse(b)); } catch (e) { cb(null); } });
}
function json(res, code, o) { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(o)); }

// --- HTTP ---
const server = http.createServer((req, res) => {
  const url = req.url.split("?")[0];
  if (req.method === "POST" && url.startsWith("/api/")) {
    const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
    return readBody(req, body => {
      if (!body) return json(res, 400, { error: "Geçersiz istek" });
      if (url === "/api/logout") { delete db.sessions[body.token]; save(); return json(res, 200, {}); }
      if (url !== "/api/login" && url !== "/api/register") return json(res, 404, { error: "Yok" });
      if (limited(ip)) return json(res, 429, { error: "Çok fazla deneme. Biraz sonra tekrar dene." });
      const nick = clamp(body.nick, 20), pw = clamp(body.pw, 100), key = nick.toLowerCase();
      if (url === "/api/register") {
        if (!NICK.test(nick)) return json(res, 400, { error: "Takma ad 3-20 karakter, harf/rakam/_ olmalı" });
        if (pw.length < 6) return json(res, 400, { error: "Şifre en az 6 karakter olmalı" });
        if (db.users[key]) return json(res, 409, { error: "Bu takma ad alınmış" });
        const salt = crypto.randomBytes(16).toString("hex");
        db.users[key] = { nick, salt, hash: hash(pw, salt) };
      } else {
        const u = db.users[key];
        if (!u || !sameHash(u.hash, hash(pw, u.salt))) return json(res, 401, { error: "Takma ad veya şifre yanlış" });
      }
      const token = crypto.randomBytes(24).toString("hex");
      db.sessions[token] = key; save();
      json(res, 200, { token });
    });
  }
  let p = url === "/" ? "/index.html" : url;
  const f = path.join(PUB, path.normalize(p));
  if (!f.startsWith(PUB)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end("Bulunamadi"); }
    res.writeHead(200, { "Content-Type": path.extname(f) === ".html" ? "text/html; charset=utf-8" : "text/plain" });
    res.end(d);
  });
});

// --- WebSocket ---
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const online = new Map();
const send = (w, o) => { if (w.readyState === 1) w.send(JSON.stringify(o)); };
const nickOf = k => (db.users[k] ? db.users[k].nick : k);
function sendTo(key, o) { (online.get(key) || []).forEach(w => send(w, o)); }

function convos(me) {
  const out = [];
  for (const k in db.msgs) {
    const parts = k.split(":");
    if (!parts.includes(me)) continue;
    const other = parts.find(x => x !== me), arr = db.msgs[k], last = arr[arr.length - 1];
    if (other && last && db.users[other]) out.push({ nick: nickOf(other), last: last.x, ts: last.s });
  }
  return out.sort((a, b) => b.ts - a.ts);
}

wss.on("connection", ws => {
  ws.alive = true; ws.win = [];
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!ws.me) {
      if (m.t === "auth" && typeof m.token === "string" && db.sessions[m.token]) {
        ws.me = db.sessions[m.token];
        if (!online.has(ws.me)) online.set(ws.me, new Set());
        online.get(ws.me).add(ws);
        send(ws, { t: "ready", me: nickOf(ws.me), convos: convos(ws.me) });
      } else { send(ws, { t: "bad" }); ws.close(); }
      return;
    }
    const now = Date.now();
    ws.win = ws.win.filter(x => now - x < 5000);
    if (ws.win.length >= 12) return;
    ws.win.push(now);

    if (m.t === "find") {
      const k = clamp(m.q, 20).toLowerCase();
      send(ws, { t: "found", nick: db.users[k] && k !== ws.me ? db.users[k].nick : null });
    } else if (m.t === "hist") {
      const k = clamp(m.with, 20).toLowerCase();
      const arr = db.msgs[convKey(ws.me, k)] || [];
      send(ws, { t: "hist", with: nickOf(k), msgs: arr.slice(-100).map(x => ({ from: nickOf(x.f), text: x.x, ts: x.s })) });
    } else if (m.t === "msg") {
      const to = clamp(m.to, 20).toLowerCase(), text = clamp(m.text, 1000).trim();
      if (!text || !db.users[to] || to === ws.me) return;
      const ck = convKey(ws.me, to);
      const arr = db.msgs[ck] || (db.msgs[ck] = []);
      arr.push({ f: ws.me, x: text, s: now });
      if (arr.length > 500) arr.shift();
      save();
      const out = { t: "msg", from: nickOf(ws.me), to: nickOf(to), text, ts: now };
      sendTo(ws.me, out); sendTo(to, out);
    }
  });
  ws.on("close", () => {
    const s = ws.me && online.get(ws.me);
    if (s) { s.delete(ws); if (!s.size) online.delete(ws.me); }
  });
});

setInterval(() => {
  wss.clients.forEach(w => { if (!w.alive) return w.terminate(); w.alive = false; w.ping(); });
}, 30000);

server.listen(process.env.PORT || 3000, () => console.log("Hazir"));
