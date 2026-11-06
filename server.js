// ============================================================
// Run Game Leaderboard API v1.0
// Node.js + Express + MongoDB
//
//  - Oyuncu başına TEK doküman (anonim, rastgele playerID). Yeni skor sadece daha yüksekse üzerine yazılır.
//  - Skor leaderboard'u: en iyi 10 skor.
//  - Süre leaderboard'u (oyun içi): en iyi süreler (oyuncunun en uzun run süresi, skordan bağımsız).
//  - İsim YOK. IP / user-agent veritabanına YAZILMAZ.
// ============================================================

import express from "express";
import { MongoClient } from "mongodb";
import cors from "cors";
import crypto from "crypto";
import rateLimit from "express-rate-limit";

// ---- ENV ----

for (const key of ["MONGO_URI", "API_SECRET", "ADMIN_KEY"]) {
  if (!process.env[key]) {
    console.error(`Missing required env: ${key}`);
    process.exit(1);
  }
}

const MONGO_URI  = process.env.MONGO_URI;
const API_SECRET = String(process.env.API_SECRET).trim();
const ADMIN_KEY  = process.env.ADMIN_KEY;
const DB_NAME    = process.env.DB_NAME || "RunGame";
const PORT       = parseInt(process.env.PORT || "3000", 10);

const num = (name, def) => {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && process.env[name] !== undefined ? v : def;
};

// ---- HİLE KORUMASI KURALLARI ----
// Skor formülü sabitleri GameOverStats'taki Inspector değerleriyle uyumlu olmalı (varsayılanlar aynı).
// Değiştirdiysen env ile güncelle ya da tolerance'ı artır.
const RULES = {
  minTimeMs:         500,
  maxTimeMs:         24 * 3600 * 1000,
  minMsPerLevel:     2000,                              // bir bölümü en az 2 sn'de geçebilir
  maxKillsPerStage:  num("MAX_KILLS_PER_STAGE", 80),    // LevelEnemySpawner.maxEnemies (50) + wall'lar + pay
  pointsPerKill:     num("POINTS_PER_KILL", 100),
  pointsPerHeadshot: num("POINTS_PER_HEADSHOT", 50),
  pointsPerSecond:   num("POINTS_PER_SECOND", 10),
  pointsPerLevel:    num("POINTS_PER_LEVEL", 500),
  maxAccuracyPoints: num("MAX_ACCURACY_POINTS", 1000),  // %100 x pointsPerAccuracyPercent(10)
  maxNoHitBonus:     num("MAX_NOHIT_BONUS", 1000),
  scoreTolerance:    num("SCORE_TOLERANCE", 1.25),
  maxHitsPerSecond:  4,
};

// Replay koruması: imza 10 dk hatırlanır (zaman penceresi ±5 dk)
const TIMESTAMP_TOLERANCE_S = 300;
const SIGNATURE_MEMORY_MS   = 10 * 60_000;
const seenSignatures = new Map(); // sig -> expiresAt
setInterval(() => {
  const now = Date.now();
  for (const [sig, exp] of seenSignatures) if (now > exp) seenSignatures.delete(sig);
}, 60_000);

// ---- DATABASE ----

const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000, connectTimeoutMS: 10000 });
let scores, logs, counts;

async function initDatabase() {
  await client.connect();
  const db = client.db(DB_NAME);
  scores = db.collection("RunScores");
  logs   = db.collection("RequestLogs");
  counts = db.collection("RequestCounts");

  await scores.createIndex({ playerID: 1 }, { unique: true });
  await scores.createIndex({ flagged: 1, score: -1 });
  await scores.createIndex({ flagged: 1, bestTimeMs: -1 });

  await logs.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3 * 24 * 60 * 60 });
  await logs.createIndex({ type: 1, playerID: 1, createdAt: -1 });
  await counts.createIndex({ endpoint: 1 }, { unique: true });
  console.log("Database ready.");
}

// ---- YARDIMCILAR ----

const ID_RE = /^[\w-]{8,64}$/;
const cleanId = (v) => (typeof v === "string" && ID_RE.test(v) ? v : null);

const ENTRY_PROJECTION = { _id: 0, playerID: 1, score: 1, timeMs: 1, kills: 1, accuracy: 1, hitsTaken: 1, levelsPassed: 1, continued: 1 };

function toEntry(doc, rank, isYou) {
  return {
    rank,
    score: doc.score,
    timeMs: doc.timeMs,
    kills: doc.kills,
    accuracy: doc.accuracy,
    hitsTaken: doc.hitsTaken,
    levelsPassed: doc.levelsPassed,
    continued: !!doc.continued,
    isYou: !!isYou,
  };
}
const EMPTY_ENTRY = toEntry({ score: 0, timeMs: 0, kills: 0, accuracy: 0, hitsTaken: 0, levelsPassed: 0, continued: false }, 0, false);

function checkViolations(r) {
  const v = [];
  const sec = r.timeMs / 1000;

  if (r.timeMs < RULES.minTimeMs || r.timeMs > RULES.maxTimeMs) v.push("time_range");
  if (r.accuracy < 0 || r.accuracy > 1) v.push("accuracy_range");
  if (r.kills > (r.levelsPassed + 1) * RULES.maxKillsPerStage) v.push("kills_per_stage");
  if (r.timeMs < r.levelsPassed * RULES.minMsPerLevel) v.push("levels_too_fast");
  if (r.hitsTaken > sec * RULES.maxHitsPerSecond + 5) v.push("hits_rate");

  const maxScore = Math.ceil(
    (r.kills * (RULES.pointsPerKill + RULES.pointsPerHeadshot) +
      sec * RULES.pointsPerSecond +
      r.levelsPassed * RULES.pointsPerLevel +
      RULES.maxAccuracyPoints +
      RULES.maxNoHitBonus) * RULES.scoreTolerance
  );
  if (r.score > maxScore) v.push("score_too_high");

  return v;
}

async function isPlayerRateLimited(playerID) {
  const n = await logs.countDocuments({
    type: "score_submit", playerID, createdAt: { $gt: new Date(Date.now() - 60_000) },
  });
  return n >= 5;
}

async function handleSuspicious(playerID, violations) {
  await logs.insertOne({ type: "suspicious_score", playerID, violations, createdAt: new Date() });
  const recent = await logs.countDocuments({
    type: "suspicious_score", playerID, createdAt: { $gt: new Date(Date.now() - 10 * 60_000) },
  });
  if (recent >= 3) await scores.updateMany({ playerID }, { $set: { flagged: true } });
}

// ---- ÖNBELLEK (30 sn) ----

const CACHE_TTL_MS = 30_000;
let topScoresCache = { data: null, ts: 0 };
let topTimesCache  = { data: null, ts: 0 };
const invalidateCaches = () => { topScoresCache = { data: null, ts: 0 }; topTimesCache = { data: null, ts: 0 }; };

async function getTopScores() {
  if (topScoresCache.data && Date.now() - topScoresCache.ts < CACHE_TTL_MS) return topScoresCache.data;
  const data = await scores
    .find({ flagged: false, score: { $gte: 0 } })
    .sort({ score: -1, lastUpdate: 1 })
    .limit(10)
    .project(ENTRY_PROJECTION)
    .toArray();
  topScoresCache = { data, ts: Date.now() };
  return data;
}

// Oyuncu kendi kaydını listeden çıkarabilsin diye 11 kayıt tutulur.
async function getTopTimes() {
  if (topTimesCache.data && Date.now() - topTimesCache.ts < CACHE_TTL_MS) return topTimesCache.data;
  const data = await scores
    .find({ flagged: false, bestTimeMs: { $gt: 0 } })
    .sort({ bestTimeMs: -1, lastUpdate: 1 })
    .limit(11)
    .project({ _id: 0, playerID: 1, bestTimeMs: 1 })
    .toArray();
  topTimesCache = { data, ts: Date.now() };
  return data;
}

// ---- APP ----

const app = express();
app.set("trust proxy", 1);
app.use(cors());
app.use(express.json({ limit: "10kb" }));

app.use(rateLimit({
  windowMs: 60_000, max: 2500, standardHeaders: true, legacyHeaders: false,
  handler: (_req, res) => res.status(429).json({ error: "Too many requests" }),
}));
const scoreLimiter = rateLimit({
  windowMs: 60_000, max: 30, standardHeaders: true, legacyHeaders: false,
  handler: (_req, res) => res.status(429).json({ error: "Too many requests" }),
});

// Sadece endpoint sayacı (IP / user-agent kaydedilmez)
app.use((req, res, next) => {
  if (req.path === "/ping") return next();
  res.on("finish", () => {
    const endpoint = `${req.method} ${req.route?.path || req.path}`;
    counts.updateOne(
      { endpoint },
      {
        $inc: { total: 1, [`byStatus.s${res.statusCode}`]: 1 },
        $set: { lastSeenAt: new Date() },
        $setOnInsert: { firstSeenAt: new Date() },
      },
      { upsert: true }
    ).catch(() => {});
  });
  next();
});

// ---- İMZA DOĞRULAMA ----
// İstemci mesajı: playerID|score|timeMs|kills|round(accuracy*10000)|hitsTaken|levelsPassed|continued(0/1)|timestamp
function verifySignature(req, res, next) {
  const ts  = parseInt(req.headers["x-timestamp"] || "0", 10);
  const sig = String(req.headers["x-signature"] || "").toLowerCase().trim();
  if (!ts || !sig) return res.status(401).json({ error: "Unauthorized" });

  const nowS = Date.now() / 1000;
  if (Math.abs(nowS - ts) > TIMESTAMP_TOLERANCE_S) return res.status(401).json({ error: "Unauthorized" });
  if (seenSignatures.has(sig)) return res.status(401).json({ error: "Unauthorized" });

  const b = req.body || {};
  const playerID = cleanId(b.playerID);
  const run = {
    score:        Number(b.score),
    timeMs:       Number(b.timeMs),
    kills:        Number(b.kills),
    accuracy:     Number(b.accuracy),
    hitsTaken:    Number(b.hitsTaken),
    levelsPassed: Number(b.levelsPassed),
    continued:    b.continued === true || b.continued === 1,
  };

  const ints = [run.score, run.timeMs, run.kills, run.hitsTaken, run.levelsPassed];
  if (!playerID || !ints.every((n) => Number.isSafeInteger(n) && n >= 0) || !Number.isFinite(run.accuracy)) {
    return res.status(400).json({ error: "Malformed payload" });
  }

  const message = [
    playerID, run.score, run.timeMs, run.kills, Math.round(run.accuracy * 10000),
    run.hitsTaken, run.levelsPassed, run.continued ? 1 : 0, ts,
  ].join("|");
  const expected = crypto.createHmac("sha256", API_SECRET).update(message).digest("hex");

  const a = Buffer.from(sig), e = Buffer.from(expected);
  if (a.length !== e.length || !crypto.timingSafeEqual(a, e)) {
    console.log("[signature] mismatch for", playerID, "| expected message:", message);
    return res.status(401).json({ error: "Unauthorized" });
  }

  seenSignatures.set(sig, Date.now() + SIGNATURE_MEMORY_MS);
  req.playerID = playerID;
  req.run = run;
  next();
}

// ---- ENDPOINTLER ----

// POST /api/score
app.post("/api/score", scoreLimiter, verifySignature, async (req, res) => {
  try {
    const id = req.playerID;
    const run = req.run;

    logs.insertOne({ type: "score_submit", playerID: id, createdAt: new Date() }).catch(() => {});
    if (await isPlayerRateLimited(id)) return res.status(429).json({ error: "Too many requests" });

    const violations = checkViolations(run);
    if (violations.length > 0) {
      await handleSuspicious(id, violations);
      // Hileciye sebep söylenmez.
      return res.json({ ok: true, improved: false, rank: 9999 });
    }

    const now = new Date();

    // 1) Kayıt yoksa boş bir kayıt aç (unique index yarışı güvenle çözer)
    await scores.updateOne(
      { playerID: id },
      { $setOnInsert: {
          score: -1, timeMs: 0, kills: 0, accuracy: 0, hitsTaken: 0, levelsPassed: 0,
          continued: false, bestTimeMs: 0, flagged: false, createdAt: now, lastUpdate: now,
      } },
      { upsert: true }
    );

    // 2) Skor sadece DAHA YÜKSEKSE üzerine yazılır
    const upd = await scores.updateOne(
      { playerID: id, score: { $lt: run.score } },
      { $set: {
          score: run.score, timeMs: run.timeMs, kills: run.kills, accuracy: run.accuracy,
          hitsTaken: run.hitsTaken, levelsPassed: run.levelsPassed, continued: run.continued,
          lastUpdate: now,
      } }
    );

    // 3) En iyi süre skordan bağımsız (süre leaderboard'u için)
    await scores.updateOne({ playerID: id }, { $max: { bestTimeMs: run.timeMs } });

    invalidateCaches();

    const doc = await scores.findOne({ playerID: id });
    let rank = 9999;
    if (doc && !doc.flagged) {
      rank = (await scores.countDocuments({ flagged: false, score: { $gt: doc.score } })) + 1;
    }
    return res.json({ ok: true, improved: upd.modifiedCount > 0, rank });
  } catch (err) {
    console.error("[score] error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

// GET /api/leaderboard?playerID=...   -> en iyi 10 skor + oyuncunun kendi sırası
app.get("/api/leaderboard", async (req, res) => {
  try {
    const id = cleanId(req.query.playerID);
    const top = (await getTopScores()).map((d, i) => toEntry(d, i + 1, id && d.playerID === id));

    let me = EMPTY_ENTRY, myRank = 0;
    if (id) {
      const doc = await scores.findOne({ playerID: id, flagged: false, score: { $gte: 0 } });
      if (doc) {
        myRank = (await scores.countDocuments({ flagged: false, score: { $gt: doc.score } })) + 1;
        me = toEntry(doc, myRank, true);
      }
    }
    return res.json({ top, me, myRank });
  } catch (err) {
    console.error("[leaderboard] error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

// GET /api/run-times?playerID=...&limit=5   -> oyun içi süre tablosu (en uzun süreler)
// Oyuncunun kendi kaydı isYou=true ile işaretlenir; istemci onu listeden çıkarır (canlı sayaç yerine geçer).
app.get("/api/run-times", async (req, res) => {
  try {
    const id = cleanId(req.query.playerID);
    const limit = Math.min(10, Math.max(1, parseInt(req.query.limit, 10) || 5));
    const times = (await getTopTimes())
      .slice(0, limit + 1)
      .map((d) => ({ timeMs: d.bestTimeMs, isYou: !!id && d.playerID === id }));
    return res.json({ times });
  } catch (err) {
    console.error("[run-times] error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

// ---- ADMIN ----

const isAdmin = (req) => {
  const a = Buffer.from(String(req.headers["x-admin-key"] || ""));
  const b = Buffer.from(String(ADMIN_KEY));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
};

app.get("/admin/stats", async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: "Forbidden" });
  try {
    return res.json(await counts.find({}).sort({ total: -1 }).toArray());
  } catch (err) {
    console.error("[admin/stats] error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

// Veri silme talebi (gizlilik): DELETE /admin/player/:playerID
app.delete("/admin/player/:playerID", async (req, res) => {
  if (!isAdmin(req)) return res.status(403).json({ error: "Forbidden" });
  const id = cleanId(req.params.playerID);
  if (!id) return res.status(400).json({ error: "Invalid playerID" });
  try {
    const r = await scores.deleteMany({ playerID: id });
    await logs.deleteMany({ playerID: id });
    invalidateCaches();
    return res.json({ ok: true, deleted: r.deletedCount });
  } catch (err) {
    console.error("[admin/delete] error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

app.get("/ping", (_req, res) => res.status(200).send("OK"));

// ---- SHUTDOWN / BOOT ----

async function gracefulExit() {
  try { await client.close(); process.exit(0); } catch { process.exit(1); }
}
process.on("SIGINT", gracefulExit);
process.on("SIGTERM", gracefulExit);

initDatabase()
  .then(() => app.listen(PORT, () => console.log(`Server running on port ${PORT}`)))
  .catch((err) => { console.error("Fatal boot error:", err); process.exit(1); });
