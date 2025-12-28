import fs from "fs";
import path from "path";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { KiteConnect } from "kiteconnect";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 8008;
const { KITE_API_KEY, KITE_ACCESS_TOKEN } = process.env;

if (!KITE_API_KEY || !KITE_ACCESS_TOKEN) {
  console.error("❌ Missing KITE_API_KEY or KITE_ACCESS_TOKEN in backend/.env");
  process.exit(1);
}

const kite = new KiteConnect({ api_key: KITE_API_KEY });
kite.setAccessToken(KITE_ACCESS_TOKEN);

// ---------------- CSV ----------------
function normalizeHeader(h) {
  return String(h || "").trim().toLowerCase();
}

function parseCsv(filePath) {
  const raw = fs.readFileSync(filePath, "utf-8").trim();
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) throw new Error("CSV empty or invalid");

  const header = lines[0].split(",").map(normalizeHeader);

  const idxSym =
    header.indexOf("tradingsymbol") !== -1
      ? header.indexOf("tradingsymbol")
      : header.indexOf("symbol");

  const idxToken = header.indexOf("instrument_token");

  if (idxSym === -1 || idxToken === -1) {
    throw new Error(
      `CSV must contain tradingsymbol(or symbol) and instrument_token. Found: ${header.join(
        ", "
      )}`
    );
  }

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",").map((c) => c.trim());
    const sym = cols[idxSym];
    const tokenRaw = cols[idxToken];
    const token = Number(String(tokenRaw || "").replace(/[^\d]/g, ""));

    if (!sym || !Number.isFinite(token) || token <= 0) continue;

    rows.push({
      symbol: sym,
      instrument_token: token,
      kiteSymbol: `NSE:${sym}`,
      _line: i + 1,
    });
  }
  return rows;
}

const CSV_PATH = path.join(process.cwd(), "data", "nifty50_tokens.csv");
const NIFTY = parseCsv(CSV_PATH);

// Dedup by token
const seen = new Set();
const deduped = [];
for (const r of NIFTY) {
  if (seen.has(r.instrument_token)) continue;
  seen.add(r.instrument_token);
  deduped.push(r);
}

console.log(`✅ Loaded ${deduped.length} UNIQUE stocks from ${CSV_PATH}`);
const STOCKS = deduped;

// ---------------- IST helpers ----------------
const IST_OFFSET_MIN = 330;

function toIST(d) {
  return new Date(d.getTime() + IST_OFFSET_MIN * 60 * 1000);
}
function pad2(n) {
  return String(n).padStart(2, "0");
}
function istDayYYYYMMDD(now = new Date()) {
  const ist = toIST(now);
  return `${ist.getUTCFullYear()}-${pad2(ist.getUTCMonth() + 1)}-${pad2(
    ist.getUTCDate()
  )}`;
}
function istTimeHHMM(now = new Date()) {
  const ist = toIST(now);
  return { hh: ist.getUTCHours(), mm: ist.getUTCMinutes(), ss: ist.getUTCSeconds() };
}
function isWeekendIST(now = new Date()) {
  const ist = toIST(now);
  const d = ist.getUTCDay(); // 0 Sun, 6 Sat
  return d === 0 || d === 6;
}
function fmtLabel(sh, sm, eh, em) {
  return `${pad2(sh)}:${pad2(sm)} - ${pad2(eh)}:${pad2(em)}`;
}

function generate5mSlots() {
  const slots = [];
  let sh = 9, sm = 15;
  const endH = 15, endM = 30;

  const toMin = (h, m) => h * 60 + m;
  const fromMin = (mins) => ({ h: Math.floor(mins / 60), m: mins % 60 });

  while (true) {
    const sMin = toMin(sh, sm);
    const eMin = sMin + 5;
    const e = fromMin(eMin);
    if (eMin > toMin(endH, endM)) break;

    slots.push({
      label: fmtLabel(sh, sm, e.h, e.m),
      startMinFromOpen: sMin - toMin(9, 15),
    });

    sh = e.h;
    sm = e.m;
  }
  return slots;
}

function lastCompleted5mEndLabel(now = new Date()) {
  const { hh, mm } = istTimeHHMM(now);
  const mins = hh * 60 + mm;

  const open = 9 * 60 + 15;
  const close = 15 * 60 + 30;

  if (mins < open) return null;

  const capped = Math.min(mins, close);
  const delta = capped - open;
  const completed = Math.floor(delta / 5);
  if (completed < 1) return null;

  const lastEndMin = open + completed * 5;
  const h = Math.floor(lastEndMin / 60);
  const m = lastEndMin % 60;

  return `${pad2(h)}:${pad2(m)}`;
}

function buildSlotIndex(slots5) {
  const map = new Map();
  for (let i = 0; i < slots5.length; i++) map.set(slots5[i].startMinFromOpen, i);
  return map;
}

function toOffsetFromOpenFromCandleDate(dateStr) {
  const d = new Date(dateStr);
  const ist = toIST(d);
  const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  return mins - (9 * 60 + 15);
}

function normalizeCandle(c) {
  if (Array.isArray(c)) return { date: c[0], open: c[1], close: c[4] };
  return { date: c.date, open: c.open, close: c.close };
}

async function asyncPool(limit, items, fn) {
  const ret = [];
  const executing = [];
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item));
    ret.push(p);
    const e = p.then(() => executing.splice(executing.indexOf(e), 1));
    executing.push(e);
    if (executing.length >= limit) await Promise.race(executing);
  }
  return Promise.all(ret);
}


// ---------------- DIRECT REST historical (breadth) ----------------
async function kiteHistoricalCandles(token, interval, fromStr, toStr) {
  const url = new URL(
    `https://api.kite.trade/instruments/historical/${token}/${interval}`
  );
  url.searchParams.set("from", fromStr);
  url.searchParams.set("to", toStr);
  url.searchParams.set("continuous", "0");
  url.searchParams.set("oi", "0");

  const resp = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-Kite-Version": "3",
      Authorization: `token ${KITE_API_KEY}:${KITE_ACCESS_TOKEN}`,
    },
  });

  const json = await resp.json();

  if (!resp.ok || json?.status === "error") {
    const msg = json?.message || `HTTP ${resp.status}`;
    const err = new Error(msg);
    err.kite = json;
    throw err;
  }

  return json?.data?.candles || [];
}

// ---------------- NSE warmup + fetch JSON (contributors) ----------------
async function nseWarmupAndFetchJson(url) {
  const warm = await fetch("https://www.nseindia.com/", {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });

  const setCookie = warm.headers.get("set-cookie") || "";
  const cookieHeader = setCookie
    .split(",")
    .map((c) => c.split(";")[0])
    .join("; ");

  const resp = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      Accept: "application/json,text/plain,*/*",
      "Accept-Language": "en-US,en;q=0.9",
      Referer: "https://www.nseindia.com/",
      Cookie: cookieHeader,
    },
  });

  return await resp.json();
}

// ---------------- Kite quote batching ----------------
async function getQuotesBatched(symbols, batchSize = 80) {
  const out = {};
  for (let i = 0; i < symbols.length; i += batchSize) {
    const batch = symbols.slice(i, i + batchSize);
    const q = await kite.getQuote(batch);
    Object.assign(out, q);
    await new Promise((r) => setTimeout(r, 150));
  }
  return out;
}

// ---------------- API ----------------
app.get("/api/health", (req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

app.get("/api/debug/tokens", (req, res) => {
  res.json({
    ok: true,
    total: STOCKS.length,
    first5: STOCKS.slice(0, 5).map((s) => ({
      symbol: s.symbol,
      instrument_token: s.instrument_token,
      line: s._line,
    })),
  });
});

// ✅ Contributors
app.get("/api/contributors", async (req, res) => {
  try {
    const nseUrl =
      "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050";
    const data = await nseWarmupAndFetchJson(nseUrl);

    if (!data?.data?.length) {
      return res.status(500).json({
        ok: false,
        error: "NSE API returned no data (blocked?). Try again.",
      });
    }

    const stocks = data.data
      .filter((x) => x?.symbol && x?.ffmc && x?.lastPrice)
      .map((x) => ({
        symbol: x.symbol,
        shares: x.lastPrice ? x.ffmc / x.lastPrice : 0,
      }));

    const symbols = stocks.map((s) => `NSE:${s.symbol}`);
    const indexSymbol = "NSE:NIFTY 50";
    const quotes = await getQuotesBatched([indexSymbol, ...symbols]);

    const indexPrice = quotes[indexSymbol]?.last_price;
    if (!indexPrice) {
      return res.status(500).json({ ok: false, error: "Index LTP missing" });
    }

    let totalMarketCap = 0;
    const marketCaps = new Map();

    for (const s of stocks) {
      const k = `NSE:${s.symbol}`;
      const ltp = quotes[k]?.last_price;
      if (!ltp || !s.shares) continue;
      const mc = ltp * s.shares;
      marketCaps.set(s.symbol, mc);
      totalMarketCap += mc;
    }

    if (totalMarketCap <= 0) {
      return res.status(500).json({
        ok: false,
        error: "Total market cap computed as 0. NSE/Kite data issue.",
      });
    }

    const items = [];
    let totalContribution = 0;

    for (const s of stocks) {
      const k = `NSE:${s.symbol}`;
      const q = quotes[k];
      if (!q?.last_price || !q?.ohlc?.close) continue;

      const ltp = q.last_price;
      const prevClose = q.ohlc.close;

      const pctChange = (ltp - prevClose) / prevClose;
      const mc = marketCaps.get(s.symbol) || 0;
      const weight = mc / totalMarketCap;

      const contribution = pctChange * weight * indexPrice;

      totalContribution += contribution;
      items.push({
        symbol: s.symbol,
        contribution: Number(contribution.toFixed(2)),
      });
    }

    items.sort((a, b) => b.contribution - a.contribution);

    res.json({
      ok: true,
      index: "NIFTY50",
      indexLtp: Number(indexPrice.toFixed(2)),
      updateHint: "Updates every 5 seconds",
      totalContribution: Number(totalContribution.toFixed(2)),
      items,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---------------- Breadth caches ----------------
const breadthCache = new Map(); // live today: key = day|lastCompleted
const histCache = new Map();    // historical: key = day
let cooldownUntil = 0;

// ✅ LIVE: /today (unchanged)
app.get("/api/breadth/nifty50/today", async (req, res) => {
  try {
    const now = new Date();
    const day = istDayYYYYMMDD(now);

    if (Date.now() < cooldownUntil) {
      return res.status(429).json({
        ok: false,
        error: "Too many requests (cooldown). Please retry in a bit.",
      });
    }

    if (isWeekendIST(now)) {
      return res.json({
        ok: true,
        day,
        market: "closed",
        message: "🚫 Weekend",
        intervals: { "5": [], "15": [], "60": [] },
        meta: { totalStocks: STOCKS.length, usedStocks: 0, skipped: [] },
      });
    }

    const { hh, mm } = istTimeHHMM(now);
    const mins = hh * 60 + mm;
    const open = 9 * 60 + 15;

    if (mins < open) {
      return res.json({
        ok: true,
        day,
        market: "preopen",
        message: "⏳ Market opens at 09:15",
        intervals: { "5": [], "15": [], "60": [] },
        meta: { totalStocks: STOCKS.length, usedStocks: 0, skipped: [] },
      });
    }

    const lastCompleted = lastCompleted5mEndLabel(now);
    if (!lastCompleted) {
      return res.json({
        ok: true,
        day,
        market: "open",
        message: "Waiting for first 5m candle to complete…",
        intervals: { "5": [], "15": [], "60": [] },
        meta: { totalStocks: STOCKS.length, usedStocks: 0, skipped: [] },
      });
    }

    const cacheKey = `${day}|${lastCompleted}`;
    if (breadthCache.has(cacheKey)) return res.json(breadthCache.get(cacheKey));

    const fromStr = `${day} 09:15:00`;
    const toStr = `${day} ${lastCompleted}:00`;

    const slots5 = generate5mSlots();
    const slotIndex = buildSlotIndex(slots5);

    const green5 = Array(slots5.length).fill(0);
    const red5 = Array(slots5.length).fill(0);
    const perStock = [];

    const skipped = [];
    const CONCURRENCY = 1;

    await asyncPool(CONCURRENCY, STOCKS, async (stk) => {
      await new Promise((r) => setTimeout(r, 220));
      try {
        const candles = await kiteHistoricalCandles(
          stk.instrument_token,
          "5minute",
          fromStr,
          toStr
        );

        const stockSlots = Array(slots5.length).fill(null);

        for (const raw of candles) {
          const c = normalizeCandle(raw);
          const offset = toOffsetFromOpenFromCandleDate(c.date);
          if (!slotIndex.has(offset)) continue;

          const idx = slotIndex.get(offset);
          stockSlots[idx] = { o: c.open, c: c.close };

          if (c.close > c.open) green5[idx] += 1;
          else if (c.close < c.open) red5[idx] += 1;
        }

        perStock.push(stockSlots);
      } catch (e) {
        const msg = String(e?.message || "");
        const kiteMsg = String(e?.kite?.message || "");

        if (msg.includes("Too many requests") || kiteMsg.includes("Too many requests")) {
          throw e;
        }

        skipped.push({
          symbol: stk.symbol,
          instrument_token: stk.instrument_token,
          line: stk._line,
          error: kiteMsg || msg,
        });
      }
    });

    const upto = slots5.findIndex((s) => s.label.endsWith(lastCompleted));
    const endCount = upto === -1 ? slots5.length : upto + 1;

    const out5 = [];
    for (let i = 0; i < endCount; i++) {
      out5.push({ slot_label: slots5[i].label, green: green5[i], red: red5[i] });
    }

    function derive(groupSize, minutes) {
      const grouped = [];
      const groups = Math.floor(endCount / groupSize);

      for (let gi = 0; gi < groups; gi++) {
        const startIdx = gi * groupSize;
        const endIdx = startIdx + groupSize - 1;

        const startMin = slots5[startIdx].startMinFromOpen;
        const startTotal = (9 * 60 + 15) + startMin;
        const endTotal = startTotal + minutes;

        const sh = Math.floor(startTotal / 60);
        const sm = startTotal % 60;
        const eh = Math.floor(endTotal / 60);
        const em = endTotal % 60;

        let g = 0, r = 0;

        for (const s of perStock) {
          const first = s[startIdx];
          const last = s[endIdx];
          if (!first || !last) continue;
          if (last.c > first.o) g++;
          else if (last.c < first.o) r++;
        }

        grouped.push({ slot_label: fmtLabel(sh, sm, eh, em), green: g, red: r });
      }

      return grouped;
    }

    const out15 = derive(3, 15);
    const out60 = derive(12, 60);

    const payload = {
      ok: true,
      day,
      market: "open",
      lastCompleted,
      intervals: { "5": out5, "15": out15, "60": out60 },
      meta: { totalStocks: STOCKS.length, usedStocks: perStock.length, skipped },
    };

    breadthCache.set(cacheKey, payload);
    return res.json(payload);
  } catch (e) {
    const msg = String(e?.message || e);
    const kiteMsg = String(e?.kite?.message || "");

    if (kiteMsg.includes("Too many requests") || msg.includes("Too many requests")) {
      cooldownUntil = Date.now() + 60 * 1000;
      return res.status(429).json({ ok: false, error: "Too many requests" });
    }

    return res.status(500).json({ ok: false, error: msg });
  }
});

// ---------------- ✅ NEW: Historical by-day multi ----------------

function isWeekendDayStrIST(dayStr) {
  const d = new Date(`${dayStr}T00:00:00+05:30`);
  const dow = d.getUTCDay(); // correct for IST midnight
  return dow === 0 || dow === 6;
}

function prevTradingDayIST(dayStr) {
  let d = new Date(`${dayStr}T00:00:00+05:30`);
  // step back until Mon-Fri
  while (true) {
    const dow = d.getUTCDay();
    if (dow !== 0 && dow !== 6) break;
    d.setUTCDate(d.getUTCDate() - 1);
  }
  const y = d.getUTCFullYear();
  const m = pad2(d.getUTCMonth() + 1);
  const dd = pad2(d.getUTCDate());
  return `${y}-${m}-${dd}`;
}

app.get("/api/breadth/nifty50/by-day-multi", async (req, res) => {
  try {
    if (Date.now() < cooldownUntil) {
      return res.status(429).json({
        ok: false,
        error: "Too many requests (cooldown). Please retry in a bit.",
      });
    }

    const requestedDay = String(req.query.day || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(requestedDay)) {
      return res.status(400).json({
        ok: false,
        error: "Invalid or missing day. Use day=YYYY-MM-DD",
      });
    }

    // ✅ weekend fallback → previous trading day
    const usedDay = isWeekendDayStrIST(requestedDay)
      ? prevTradingDayIST(requestedDay)
      : requestedDay;

    const cacheKey = usedDay;
    if (histCache.has(cacheKey)) {
      return res.json({
        ...histCache.get(cacheKey),
        requestedDay,
      });
    }

    const fromStr = `${usedDay} 09:15:00`;
    const toStr = `${usedDay} 15:30:00`;

    const slots5 = generate5mSlots();
    const slotIndex = buildSlotIndex(slots5);

    const green5 = Array(slots5.length).fill(0);
    const red5 = Array(slots5.length).fill(0);
    const perStock = [];

    const skipped = [];
    const CONCURRENCY = 1;

    await asyncPool(CONCURRENCY, STOCKS, async (stk) => {
      await new Promise((r) => setTimeout(r, 220));
      try {
        const candles = await kiteHistoricalCandles(
          stk.instrument_token,
          "5minute",
          fromStr,
          toStr
        );

        const stockSlots = Array(slots5.length).fill(null);

        for (const raw of candles) {
          const c = normalizeCandle(raw);
          const offset = toOffsetFromOpenFromCandleDate(c.date);
          if (!slotIndex.has(offset)) continue;

          const idx = slotIndex.get(offset);
          stockSlots[idx] = { o: c.open, c: c.close };

          if (c.close > c.open) green5[idx] += 1;
          else if (c.close < c.open) red5[idx] += 1;
        }

        perStock.push(stockSlots);
      } catch (e) {
        const msg = String(e?.message || "");
        const kiteMsg = String(e?.kite?.message || "");

        if (msg.includes("Too many requests") || kiteMsg.includes("Too many requests")) {
          throw e;
        }

        skipped.push({
          symbol: stk.symbol,
          instrument_token: stk.instrument_token,
          line: stk._line,
          error: kiteMsg || msg,
        });
      }
    });

    // full day output
    const out5 = slots5.map((s, i) => ({
      slot_label: s.label,
      green: green5[i],
      red: red5[i],
    }));

    function derive(groupSize, minutes) {
      const grouped = [];
      const groups = Math.floor(slots5.length / groupSize);

      for (let gi = 0; gi < groups; gi++) {
        const startIdx = gi * groupSize;
        const endIdx = startIdx + groupSize - 1;

        const startMin = slots5[startIdx].startMinFromOpen;
        const startTotal = (9 * 60 + 15) + startMin;
        const endTotal = startTotal + minutes;

        const sh = Math.floor(startTotal / 60);
        const sm = startTotal % 60;
        const eh = Math.floor(endTotal / 60);
        const em = endTotal % 60;

        let g = 0, r = 0;

        for (const s of perStock) {
          const first = s[startIdx];
          const last = s[endIdx];
          if (!first || !last) continue;
          if (last.c > first.o) g++;
          else if (last.c < first.o) r++;
        }

        grouped.push({ slot_label: fmtLabel(sh, sm, eh, em), green: g, red: r });
      }

      return grouped;
    }

    const out15 = derive(3, 15);
    const out60 = derive(12, 60);

    const payload = {
      ok: true,
      requestedDay,
      day: usedDay,
      intervals: { "5": out5, "15": out15, "60": out60 },
      meta: {
        totalStocks: STOCKS.length,
        usedStocks: perStock.length,
        skipped,
        note: usedDay !== requestedDay ? "Weekend selected → showing previous trading day" : "",
      },
    };

    histCache.set(cacheKey, payload);
    return res.json(payload);
  } catch (e) {
    const msg = String(e?.message || e);
    const kiteMsg = String(e?.kite?.message || "");

    if (kiteMsg.includes("Too many requests") || msg.includes("Too many requests")) {
      cooldownUntil = Date.now() + 60 * 1000;
      return res.status(429).json({ ok: false, error: "Too many requests" });
    }

    return res.status(500).json({ ok: false, error: msg });
  }
});

app.listen(PORT, () => {
  console.log(`✅ Backend running at http://localhost:${PORT}`);
});
