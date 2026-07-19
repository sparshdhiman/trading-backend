import express from "express";
import cors from "cors";
import fetch from "node-fetch";
import { parseStringPromise } from "xml2js";
import cron from "node-cron";
import WebSocket from "ws";
import http from "http";

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const FINNHUB_KEY = process.env.FINNHUB_KEY || "";

// ── CACHE ─────────────────────────────────────────────────────────
const state = {
  crypto: {},
  markets: {},
  feargreed: { crypto: null, us: null, india: null, ts: 0 },
  news: { items: [], ts: 0 },
  global: { data: null, ts: 0 },
  whales: [],
  institutions: {},
  trackedWallets: {},
};

// ── BINANCE WEBSOCKET (crypto prices) ────────────────────────────
const CRYPTO_SYMBOLS = ["btcusdt","ethusdt","solusdt","hypeusdt","xautusdt","bnbusdt","xrpusdt","dogeusdt"];
let binanceWS = null;

function connectBinance() {
  const streams = CRYPTO_SYMBOLS.map(s => `${s}@ticker`).join("/");
  binanceWS = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);
  binanceWS.on("open", () => console.log("[binance-ws] connected"));
  binanceWS.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      const t = msg.data;
      if (!t?.s) return;
      state.crypto[t.s.toLowerCase()] = {
        price: parseFloat(t.c), change: parseFloat(t.P),
        high: parseFloat(t.h), low: parseFloat(t.l),
        vol: parseFloat(t.v), ts: Date.now(),
      };
    } catch {}
  });
  binanceWS.on("close", () => { console.log("[binance-ws] reconnecting..."); setTimeout(connectBinance, 5000); });
  binanceWS.on("error", () => binanceWS.terminate());
}

// ── HYPERLIQUID WHALE TRADES ──────────────────────────────────────
const WHALE_THRESHOLD_USD = 50000;


// ── TRACKED WALLET (Hyperliquid perp positions + PnL) ────────────
const TRACKED_WALLETS = [
  { address: "0xb83de012dba672c76a7dbbbf3e459cb59d7d6e36", label: "Tracked Trader" },
];

async function fetchWalletState(address) {
  try {
    const r = await fetch("https://api.hyperliquid.xyz/info", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type: "clearinghouseState", user: address }),
      signal: AbortSignal.timeout(10000),
    });
    if (!r.ok) return null;
    const d = await r.json();

    const positions = (d.assetPositions || []).map(p => {
      const pos = p.position;
      const szi = parseFloat(pos.szi || 0);
      const entryPx = parseFloat(pos.entryPx || 0);
      const unrealizedPnl = parseFloat(pos.unrealizedPnl || 0);
      const positionValue = parseFloat(pos.positionValue || 0);
      const leverage = pos.leverage?.value || null;
      const liqPx = pos.liquidationPx ? parseFloat(pos.liquidationPx) : null;
      return {
        coin: pos.coin,
        side: szi >= 0 ? "LONG" : "SHORT",
        size: Math.abs(szi),
        entryPx,
        positionValue,
        unrealizedPnl,
        pnlPct: entryPx > 0 ? (unrealizedPnl / (Math.abs(szi) * entryPx)) * 100 : 0,
        leverage,
        liqPx,
      };
    }).filter(p => p.size > 0);

    const marginSummary = d.marginSummary || {};
    const accountValue = parseFloat(marginSummary.accountValue || 0);
    const totalMarginUsed = parseFloat(marginSummary.totalMarginUsed || 0);
    const totalUnrealizedPnl = positions.reduce((sum, p) => sum + p.unrealizedPnl, 0);

    return {
      address,
      accountValue,
      totalMarginUsed,
      totalUnrealizedPnl,
      positions: positions.sort((a, b) => Math.abs(b.positionValue) - Math.abs(a.positionValue)),
      ts: Date.now(),
    };
  } catch (e) {
    console.error(`[wallet ${address.slice(0,8)}] error:`, e.message);
    return null;
  }
}

async function refreshTrackedWallets() {
  for (const w of TRACKED_WALLETS) {
    const result = await fetchWalletState(w.address);
    if (result) {
      state.trackedWallets[w.address] = { ...result, label: w.label };
      console.log(`[wallet] ${w.label}: ${result.positions.length} positions, PnL $${result.totalUnrealizedPnl.toFixed(0)}`);
    }
  }
}

async function fetchHyperliquidTrades() {
  try {
    const coins = ["BTC", "ETH", "SOL", "HYPE"];
    const allTrades = [];

    for (const coin of coins) {
      try {
        const r = await fetch("https://api.hyperliquid.xyz/info", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ type: "recentTrades", coin }),
          signal: AbortSignal.timeout(8000),
        });
        if (!r.ok) continue;
        const trades = await r.json();
        if (!Array.isArray(trades)) continue;

        trades.forEach(t => {
          const price = parseFloat(t.px);
          const size = parseFloat(t.sz);
          const usd = price * size;
          if (usd >= WHALE_THRESHOLD_USD) {
            allTrades.push({
              sym: coin,
              side: t.side === "A" ? "SELL" : "BUY",
              price, qty: size.toFixed(4),
              usd: Math.round(usd),
              ts: t.time || Date.now(),
              exchange: "Hyperliquid",
            });
          }
        });
      } catch {}
    }

    allTrades.sort((a, b) => b.usd - a.usd);
    if (allTrades.length > 0) {
      state.whales = allTrades.slice(0, 50);
      console.log(`[hyperliquid] ${allTrades.length} whale trades found`);
    }
  } catch (e) {
    console.error("[hyperliquid] error:", e.message);
  }
}

// ── FINNHUB WebSocket (US stocks) ────────────────────────────────
let finnhubWS = null;
function connectFinnhub() {
  if (!FINNHUB_KEY) return;
  finnhubWS = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
  finnhubWS.on("open", () => {
    ["SPY","QQQ","GLD","IWM"].forEach(sym =>
      finnhubWS.send(JSON.stringify({ type: "subscribe", symbol: sym }))
    );
  });
  finnhubWS.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type !== "trade" || !msg.data) return;
      msg.data.forEach(t => {
        if (!state.markets[t.s]) state.markets[t.s] = {};
        state.markets[t.s].price = t.p;
        state.markets[t.s].ts = t.t;
      });
    } catch {}
  });
  finnhubWS.on("close", () => setTimeout(connectFinnhub, 10000));
  finnhubWS.on("error", () => finnhubWS?.terminate());
}

// ── YAHOO FINANCE (indices) ───────────────────────────────────────
const YAHOO_SYMBOLS = {
  "^GSPC":"spx","^NSEI":"nifty","^IXIC":"nasdaq","^DJI":"dow",
  "^VIX":"vix","GC=F":"gold_fut","CL=F":"oil","DX-Y.NYB":"dxy_direct",
  "EURUSD=X":"eurusd","^TNX":"us10y","^FTSE":"ftse","^N225":"nikkei",
};

async function fetchYahoo() {
  try {
    const symbols = Object.keys(YAHOO_SYMBOLS).map(s => encodeURIComponent(s)).join(",");
    const r = await fetch(`https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}`, {
      headers: { "User-Agent": "Mozilla/5.0", "Accept": "application/json" },
      signal: AbortSignal.timeout(10000),
    });
    if (!r.ok) throw new Error(`Yahoo ${r.status}`);
    const d = await r.json();
    (d?.quoteResponse?.result || []).forEach(q => {
      const key = YAHOO_SYMBOLS[q.symbol];
      if (!key) return;
      state.markets[key] = {
        price: q.regularMarketPrice,
        change: q.regularMarketChangePercent,
        high: q.regularMarketDayHigh,
        low: q.regularMarketDayLow,
        prevClose: q.regularMarketPreviousClose,
        ts: Date.now(), live: true,
      };
    });
    if (state.markets.eurusd?.price) {
      state.markets.dxy = {
        price: +(1/state.markets.eurusd.price*100).toFixed(3),
        change: -(state.markets.eurusd.change||0),
        ts: Date.now(), live: true,
      };
    } else if (state.markets.dxy_direct?.price) {
      state.markets.dxy = state.markets.dxy_direct;
    }
    console.log("[yahoo] refreshed");
  } catch (e) {
    console.error("[yahoo] error:", e.message);
  }
}

// ── COINGECKO ─────────────────────────────────────────────────────
async function refreshCoinGecko() {
  try {
    const r = await fetch("https://api.coingecko.com/api/v3/global", {signal: AbortSignal.timeout(10000)});
    if (r.ok) state.global = { data: (await r.json()).data, ts: Date.now() };
    console.log("[coingecko] refreshed");
  } catch (e) { console.error("[coingecko] error:", e.message); }
}

// ── FEAR & GREED ──────────────────────────────────────────────────
async function refreshFearGreed() {
  try {
    const r = await fetch("https://api.alternative.me/fng/?limit=1", {signal: AbortSignal.timeout(6000)});
    if (!r.ok) return;
    const d = await r.json();
    const val = parseInt(d.data?.[0]?.value || 0);
    const label = d.data?.[0]?.value_classification || "";
    const change = state.global.data?.market_cap_change_percentage_24h_usd || 0;
    const usVal = Math.min(100, Math.max(0, 50 + change * 3));
    const indVal = Math.min(100, Math.max(0, 50 + change * 2.5));
    const lbl = v => v > 74 ? "Extreme Greed" : v > 54 ? "Greed" : v > 45 ? "Neutral" : v > 24 ? "Fear" : "Extreme Fear";
    state.feargreed = {
      crypto: { value: val, label },
      us: { value: Math.round(usVal), label: lbl(usVal) },
      india: { value: Math.round(indVal), label: lbl(indVal) },
      ts: Date.now(),
    };
    console.log(`[feargreed] crypto=${val}`);
  } catch (e) { console.error("[feargreed] error:", e.message); }
}

// ── NEWS via RSS ──────────────────────────────────────────────────
const RSS_FEEDS = [
  { url: "https://cointelegraph.com/rss",                                                                   cat: "CRYPTO"  },
  { url: "https://coindesk.com/arc/outboundfeeds/rss/",                                                     cat: "CRYPTO"  },
  { url: "https://decrypt.co/feed",                                                                         cat: "CRYPTO"  },
  { url: "https://cryptonews.com/news/feed/",                                                               cat: "CRYPTO"  },
  { url: "https://news.google.com/rss/search?q=trump+crypto+regulation&hl=en-US&gl=US&ceid=US:en",         cat: "TRUMP"   },
  { url: "https://news.google.com/rss/search?q=federal+reserve+fomc+interest+rate&hl=en-US&gl=US&ceid=US:en", cat: "MACRO"},
  { url: "https://news.google.com/rss/search?q=stock+market+nasdaq+sp500&hl=en-US&gl=US&ceid=US:en",       cat: "MARKETS" },
  { url: "https://news.google.com/rss/search?q=nifty+sensex+india+market&hl=en-US&gl=US&ceid=US:en",       cat: "INDIA"   },
  { url: "https://news.google.com/rss/search?q=trump+truth+social+economy&hl=en-US&gl=US&ceid=US:en",      cat: "TRUMP"   },
];

function tagFromTitle(t) {
  const l = (t||"").toLowerCase();
  if (/trump|white house|truth social|sec |cftc|executive|congress/.test(l)) return "TRUMP";
  if (/bitcoin|btc|ethereum|eth|solana|sol|crypto|defi|nft|blockchain|hype/.test(l)) return "CRYPTO";
  if (/fed |fomc|interest rate|inflation|gdp|cpi |pce|powell|recession/.test(l)) return "MACRO";
  if (/nifty|sensex|india|bse|nse/.test(l)) return "INDIA";
  if (/stock|market|nasdaq|s&p|dow|oil|gold|forex|dollar|rally/.test(l)) return "MARKETS";
  return "NEWS";
}

async function refreshNews() {
  try {
    const all = [];
    await Promise.allSettled(RSS_FEEDS.map(async feed => {
      try {
        const r = await fetch(feed.url, {
          headers: { "User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)" },
          signal: AbortSignal.timeout(10000),
        });
        if (!r.ok) return;
        const xml = await r.text();
        const parsed = await parseStringPromise(xml, { explicitArray: false });
        const items = parsed?.rss?.channel?.item || parsed?.feed?.entry || [];
        const arr = Array.isArray(items) ? items : [items];
        arr.forEach(item => {
          const title = (typeof item.title === "string" ? item.title : item.title?._ || "")
            .replace(/<!\[CDATA\[|\]\]>/g, "").replace(/&amp;/g,"&").trim();
          const pubDate = item.pubDate || item.published || null;
          const link = typeof item.link === "string" ? item.link.trim() : item.link?.$?.href || "";
          if (title.length > 15) all.push({ title, pubDate, link, cat: feed.cat || tagFromTitle(title) });
        });
      } catch {}
    }));
    const seen = new Set();
    const unique = all.filter(item => {
      const key = item.title.slice(0,50).toLowerCase().replace(/[^a-z]/g,"");
      if (seen.has(key)) return false;
      seen.add(key); return true;
    });
    unique.sort((a,b) => { try { return new Date(b.pubDate||0)-new Date(a.pubDate||0); } catch { return 0; } });
    state.news = {
      items: unique.slice(0,60).map(item => ({
        title: item.title,
        source: item.link,
        category: item.cat || tagFromTitle(item.title),
        ts: item.pubDate ? new Date(item.pubDate).getTime() : Date.now(),
      })),
      ts: Date.now(),
    };
    console.log(`[news] ${state.news.items.length} stories`);
  } catch (e) { console.error("[news] error:", e.message); }
}

// ── SEC EDGAR 13F INSTITUTIONAL HOLDINGS (real filings) ──────────
const INSTITUTIONS = [
  { name: "Berkshire Hathaway",       cik: "0001067983", manager: "Warren Buffett" },
  { name: "Bridgewater Associates",   cik: "0001350694", manager: "Ray Dalio"      },
  { name: "Scion Asset Mgmt (Burry)", cik: "0001649339", manager: "Michael Burry"  },
  { name: "Citadel Advisors",         cik: "0001423298", manager: "Ken Griffin"    },
  { name: "Leopold & Associates",     cik: "0001037389", manager: "—"              },
];

function fmtUsdShort(n) {
  if (n >= 1e9) return "$" + (n / 1e9).toFixed(2) + "B";
  if (n >= 1e6) return "$" + (n / 1e6).toFixed(2) + "M";
  if (n >= 1e3) return "$" + (n / 1e3).toFixed(2) + "K";
  return "$" + Math.round(n);
}

// Parse the actual 13F information table XML for real holdings
async function fetch13FHoldings(cik, accessionNumber) {
  try {
    const accNoDashes = accessionNumber.replace(/-/g, "");
    const cikNum = parseInt(cik, 10);
    const indexUrl = `https://www.sec.gov/Archives/edgar/data/${cikNum}/${accNoDashes}/`;
    const idxRes = await fetch(indexUrl, {
      headers: { "User-Agent": "MacroTerminal research@macroterminal.com" },
      signal: AbortSignal.timeout(10000),
    });
    if (!idxRes.ok) return null;
    const idxHtml = await idxRes.text();

    const xmlMatch = idxHtml.match(/href="([^"]*(?:infotable|InfoTable)[^"]*\.xml)"/i);
    if (!xmlMatch) return null;

    const xmlPath = xmlMatch[1];
    const xmlUrl = xmlPath.startsWith("http") ? xmlPath : `https://www.sec.gov${xmlPath.startsWith("/") ? "" : "/"}${xmlPath}`;

    const xmlRes = await fetch(xmlUrl, {
      headers: { "User-Agent": "MacroTerminal research@macroterminal.com" },
      signal: AbortSignal.timeout(10000),
    });
    if (!xmlRes.ok) return null;
    const xml = await xmlRes.text();

    const parsed = await parseStringPromise(xml, {
      explicitArray: false,
      tagNameProcessors: [n => n.replace(/^.*:/, "")],
    });

    let entries = parsed?.informationTable?.infoTable || [];
    if (!Array.isArray(entries)) entries = [entries];

    const holdingsMap = {};
    let totalValue = 0;
    entries.forEach(e => {
      const name = e.nameOfIssuer || "Unknown";
      const value = parseFloat(e.value || 0) * 1000; // thousands -> dollars
      totalValue += value;
      if (!holdingsMap[name]) holdingsMap[name] = { name, value: 0 };
      holdingsMap[name].value += value;
    });

    const allHoldings = Object.values(holdingsMap).sort((a, b) => b.value - a.value);

    const holdings = allHoldings.slice(0, 6).map(h => ({
      name: h.name,
      value: fmtUsdShort(h.value),
      pct: totalValue > 0 ? +((h.value / totalValue) * 100).toFixed(1) : 0,
    }));

    return { holdings, allHoldings, totalValue, totalValueFmt: fmtUsdShort(totalValue), positionCount: Object.keys(holdingsMap).length };
  } catch (e) {
    console.error("[13F parse] error:", e.message);
    return null;
  }
}

async function fetchInstitution(inst) {
  try {
    const r = await fetch(`https://data.sec.gov/submissions/CIK${inst.cik}.json`, {
      headers: { "User-Agent": "MacroTerminal research@macroterminal.com", "Accept": "application/json" },
      signal: AbortSignal.timeout(10000),
    });
    if (!r.ok) throw new Error(`SEC ${r.status}`);
    const d = await r.json();
    const forms = d.filings?.recent?.form || [];
    const dates = d.filings?.recent?.filingDate || [];
    const accNums = d.filings?.recent?.accessionNumber || [];

    // Find latest and prior 13F-HR filings
    const filings13F = [];
    for (let i = 0; i < forms.length; i++) {
      if (forms[i] === "13F-HR") filings13F.push({ date: dates[i], accNum: accNums[i] });
      if (filings13F.length >= 2) break;
    }

    if (filings13F.length === 0) {
      return { name: inst.name, manager: inst.manager, cik: inst.cik, filingDate: null, holdings: [], positionCount: 0, totalValueFmt: "—", deltas: [], error: "No 13F found" };
    }

    const [latest, prior] = filings13F;
    const parsedLatest = await fetch13FHoldings(inst.cik, latest.accNum);
    const parsedPrior = prior ? await fetch13FHoldings(inst.cik, prior.accNum) : null;

    const deltas = computeDeltas(parsedLatest?.allHoldings, parsedPrior?.allHoldings);

    return {
      name: inst.name,
      manager: inst.manager,
      cik: inst.cik,
      filingDate: latest.date,
      secUrl: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${inst.cik}&type=13F`,
      positionCount: parsedLatest?.positionCount || 0,
      totalValueFmt: parsedLatest?.totalValueFmt || "—",
      holdings: parsedLatest?.holdings || [],
      deltas,
    };
  } catch (e) {
    console.error(`[13F] ${inst.name}: ${e.message}`);
    return { name: inst.name, manager: inst.manager, cik: inst.cik, filingDate: null, holdings: [], positionCount: 0, totalValueFmt: "—", deltas: [], error: e.message };
  }
}

// Compare two quarters of holdings and generate ADD/EXIT/NEW/TRIM tags
function computeDeltas(latest, prior) {
  if (!latest || !prior) return [];

  const latestMap = new Map(latest.map(h => [h.name, h.value]));
  const priorMap = new Map(prior.map(h => [h.name, h.value]));

  const tags = [];

  // New positions (in latest, not in prior) - sorted by value, top 2
  const newPos = latest.filter(h => !priorMap.has(h.name)).sort((a,b) => b.value - a.value).slice(0, 2);
  newPos.forEach(h => tags.push(`NEW ${shortName(h.name)}`));

  // Exited positions (in prior, not in latest) - sorted by value, top 2
  const exited = prior.filter(h => !latestMap.has(h.name)).sort((a,b) => b.value - a.value).slice(0, 2);
  exited.forEach(h => tags.push(`EXIT ${shortName(h.name)}`));

  // Added (increased by >20%) - top 1
  const added = latest.filter(h => {
    const p = priorMap.get(h.name);
    return p && h.value > p * 1.2;
  }).sort((a,b) => (b.value - priorMap.get(b.name)) - (a.value - priorMap.get(a.name))).slice(0, 1);
  added.forEach(h => tags.push(`ADD ${shortName(h.name)}`));

  // Trimmed (decreased by >20%) - top 1
  const trimmed = latest.filter(h => {
    const p = priorMap.get(h.name);
    return p && h.value < p * 0.8;
  }).sort((a,b) => (priorMap.get(a.name) - a.value) - (priorMap.get(b.name) - b.value)).slice(0, 1);
  trimmed.forEach(h => tags.push(`TRIM ${shortName(h.name)}`));

  return tags.slice(0, 5);
}

function shortName(name) {
  // Simplify company names for tags: "APPLE INC" -> "APPLE"
  return name.replace(/\s+(INC|CORP|CORPORATION|CO|LTD|TR|ETF)\.?$/i, "").split(" ").slice(0,2).join(" ");
}

async function refreshInstitutions() {
  console.log("[13F] refreshing institutional holdings from SEC EDGAR...");
  const results = await Promise.allSettled(INSTITUTIONS.map(inst => fetchInstitution(inst)));
  results.forEach((r, i) => {
    if (r.status === "fulfilled" && r.value) state.institutions[INSTITUTIONS[i].name] = r.value;
  });
  console.log(`[13F] loaded ${Object.keys(state.institutions).length} institutions`);
}

// ── ROUTES ────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({
  name: "trading-backend-v2", version: "2.1.0",
  sources: ["Binance WS", "Hyperliquid", "Yahoo Finance", "CoinGecko", "alternative.me", "RSS (9 feeds)", "SEC EDGAR 13F"],
  endpoints: ["/api/news","/api/prices","/api/feargreed","/api/whales","/api/global","/api/institutions","/api/health"],
}));

app.get("/api/prices", (req, res) => res.json({
  btc: state.crypto["btcusdt"]  || null,
  eth: state.crypto["ethusdt"]  || null,
  sol: state.crypto["solusdt"]  || null,
  hype:state.crypto["hypeusdt"] || null,
  bnb: state.crypto["bnbusdt"]  || null,
  xrp: state.crypto["xrpusdt"]  || null,
  doge:state.crypto["dogeusdt"] || null,
  gold:state.crypto["xautusdt"] || null,
  spx:    state.markets.spx    || null,
  nifty:  state.markets.nifty  || null,
  nasdaq: state.markets.nasdaq || null,
  dow:    state.markets.dow    || null,
  vix:    state.markets.vix    || null,
  dxy:    state.markets.dxy    || null,
  oil:    state.markets.oil    || null,
  us10y:  state.markets.us10y  || null,
  ftse:   state.markets.ftse   || null,
  nikkei: state.markets.nikkei || null,
  eurusd: state.markets.eurusd || null,
  ts: Date.now(),
}));

app.get("/api/news",         (req, res) => res.json({ items: state.news.items, ts: state.news.ts, count: state.news.items.length }));
app.get("/api/feargreed",    (req, res) => res.json({ ...state.feargreed }));
app.get("/api/whales",       (req, res) => res.json({ trades: state.whales.slice(0,30), ts: Date.now(), exchange: "Hyperliquid" }));
app.get("/api/global",       (req, res) => res.json({ data: state.global.data, ts: state.global.ts }));
app.get("/api/institutions", (req, res) => res.json({ institutions: state.institutions, ts: Date.now() }));
app.get("/api/wallets", (req, res) => res.json({ wallets: state.trackedWallets, ts: Date.now() }));
app.get("/api/institutions/refresh", async (req, res) => {
  await refreshInstitutions();
  res.json({ institutions: state.institutions, ts: Date.now() });
});

app.get("/api/health", (req, res) => {
  const age = x => x ? Math.round((Date.now()-x)/1000)+"s ago" : "never";
  res.json({
    status: "ok",
    binance_ws: binanceWS?.readyState === 1 ? "connected" : "disconnected",
    finnhub_ws: finnhubWS?.readyState === 1 ? "connected" : "no key",
    crypto_tickers: Object.keys(state.crypto).length,
    market_tickers: Object.keys(state.markets).length,
    whale_trades: state.whales.length,
    whale_source: "Hyperliquid",
    news_stories: state.news.items.length,
    institutions: Object.keys(state.institutions).length,
    last_news: age(state.news.ts),
    last_yahoo: age(state.markets.spx?.ts),
    last_feargreed: age(state.feargreed.ts),
    last_coingecko: age(state.global.ts),
  });
});

// ── CRON ──────────────────────────────────────────────────────────
cron.schedule("*/3  * * * *", refreshNews);
cron.schedule("*/1  * * * *", fetchYahoo);
cron.schedule("*/5  * * * *", refreshCoinGecko);
cron.schedule("*/5  * * * *", refreshFearGreed);
cron.schedule("*/2  * * * *", fetchHyperliquidTrades);
cron.schedule("*/1  * * * *", refreshTrackedWallets);
cron.schedule("0 0  * * *",   refreshInstitutions);

// ── START ─────────────────────────────────────────────────────────
const server = http.createServer(app);
server.listen(PORT, async () => {
  console.log(`\n🚀 Trading Backend v2 running on port ${PORT}`);
  connectBinance();
  connectFinnhub();
  console.log("Warming up cache...");
  await Promise.allSettled([
    fetchYahoo(),
    refreshCoinGecko(),
    refreshFearGreed(),
    refreshNews(),
    fetchHyperliquidTrades(),
    refreshInstitutions(),
    refreshTrackedWallets(),
  ]);
  console.log("✅ All systems running!\n");
});
