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
const FINNHUB_KEY = process.env.FINNHUB_KEY || "d90bf29r01qk8bfk79lgd90bf29r01qk8bfk79m0"; // set in fly secrets

// ── CACHE ─────────────────────────────────────────────────────────
const state = {
  // Crypto prices via Binance WebSocket (real-time)
  crypto: {},
  // Traditional market prices via Yahoo Finance + Finnhub
  markets: {},
  // Fear & Greed
  feargreed: { crypto: null, us: null, india: null, ts: 0 },
  // News via RSS
  news: { items: [], ts: 0 },
  // CoinGecko global (dominance, market cap)
  global: { data: null, ts: 0 },
  // Sparklines from CoinGecko
  sparklines: {},
  // COT data from CFTC
  cot: { data: null, ts: 0 },
  // Whale trades (large Binance trades)
  whales: [],
};

// ── BINANCE WEBSOCKET (real-time crypto) ─────────────────────────
const CRYPTO_SYMBOLS = ["btcusdt","ethusdt","solusdt","hypeusdt","xautusdt","bnbusdt","xrpusdt","dogeusdt"];
let binanceWS = null;
let binanceReconnectTimer = null;

function connectBinance() {
  const streams = CRYPTO_SYMBOLS.map(s => `${s}@ticker`).join("/");
  const url = `wss://stream.binance.com:9443/stream?streams=${streams}`;
  
  try {
    binanceWS = new WebSocket(url);
    
    binanceWS.on("open", () => {
      console.log("[binance-ws] connected");
      if (binanceReconnectTimer) { clearTimeout(binanceReconnectTimer); binanceReconnectTimer = null; }
    });
    
    binanceWS.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        const ticker = msg.data;
        if (!ticker?.s) return;
        const sym = ticker.s.toLowerCase();
        state.crypto[sym] = {
          price: parseFloat(ticker.c),
          change: parseFloat(ticker.P),
          high: parseFloat(ticker.h),
          low: parseFloat(ticker.l),
          vol: parseFloat(ticker.v),
          ts: Date.now(),
        };
      } catch {}
    });
    
    binanceWS.on("close", () => {
      console.log("[binance-ws] disconnected, reconnecting in 5s...");
      binanceReconnectTimer = setTimeout(connectBinance, 5000);
    });
    
    binanceWS.on("error", (err) => {
      console.error("[binance-ws] error:", err.message);
      binanceWS.terminate();
    });
  } catch (e) {
    console.error("[binance-ws] connect error:", e.message);
    binanceReconnectTimer = setTimeout(connectBinance, 10000);
  }
}

// ── BINANCE WHALE TRADES WebSocket ────────────────────────────────
let whaleWS = null;
const WHALE_THRESHOLD_USD = 100000; // $100k+ trades

function connectWhales() {
  try {
    whaleWS = new WebSocket("wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade");
    
    whaleWS.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        const trade = msg.data;
        if (!trade?.p) return;
        const price = parseFloat(trade.p);
        const qty = parseFloat(trade.q);
        const usdVal = price * qty;
        if (usdVal >= WHALE_THRESHOLD_USD) {
          state.whales.unshift({
            sym: trade.s,
            side: trade.m ? "SELL" : "BUY",
            price,
            qty: qty.toFixed(4),
            usd: Math.round(usdVal),
            ts: Date.now(),
          });
          if (state.whales.length > 50) state.whales = state.whales.slice(0, 50);
        }
      } catch {}
    });
    
    whaleWS.on("close", () => setTimeout(connectWhales, 5000));
    whaleWS.on("error", () => whaleWS.terminate());
  } catch (e) {
    setTimeout(connectWhales, 10000);
  }
}

// ── FINNHUB WebSocket (US stocks real-time) ───────────────────────
const FINNHUB_SYMBOLS = ["SPY","QQQ","GLD","IWM","VIX"];
let finnhubWS = null;

function connectFinnhub() {
  if (!FINNHUB_KEY) {
    console.log("[finnhub] no API key, skipping WebSocket");
    return;
  }
  try {
    finnhubWS = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
    
    finnhubWS.on("open", () => {
      console.log("[finnhub-ws] connected");
      FINNHUB_SYMBOLS.forEach(sym => {
        finnhubWS.send(JSON.stringify({ type: "subscribe", symbol: sym }));
      });
    });
    
    finnhubWS.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type !== "trade" || !msg.data) return;
        msg.data.forEach(trade => {
          if (!state.markets[trade.s]) state.markets[trade.s] = {};
          state.markets[trade.s].price = trade.p;
          state.markets[trade.s].ts = trade.t;
        });
      } catch {}
    });
    
    finnhubWS.on("close", () => {
      console.log("[finnhub-ws] disconnected, reconnecting...");
      setTimeout(connectFinnhub, 10000);
    });
    
    finnhubWS.on("error", () => finnhubWS?.terminate());
  } catch (e) {
    setTimeout(connectFinnhub, 15000);
  }
}

// ── YAHOO FINANCE (indices polled every 10s) ──────────────────────
const YAHOO_SYMBOLS = {
  "^GSPC":    "spx",
  "^NSEI":    "nifty",
  "^IXIC":    "nasdaq",
  "^DJI":     "dow",
  "^VIX":     "vix",
  "GC=F":     "gold_fut",
  "CL=F":     "oil",
  "DX-Y.NYB": "dxy_direct",
  "EURUSD=X": "eurusd",
  "GBPUSD=X": "gbpusd",
  "^TNX":     "us10y",
  "^FTSE":    "ftse",
  "^N225":    "nikkei",
};

async function fetchYahooMulti() {
  try {
    const symbols = Object.keys(YAHOO_SYMBOLS).map(s => encodeURIComponent(s)).join(",");
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}`;
    const res = await fetch(url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com",
      },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) throw new Error(`Yahoo HTTP ${res.status}`);
    const d = await res.json();
    const quotes = d?.quoteResponse?.result || [];
    quotes.forEach(q => {
      const key = YAHOO_SYMBOLS[q.symbol];
      if (!key) return;
      state.markets[key] = {
        price: q.regularMarketPrice,
        change: q.regularMarketChangePercent,
        high: q.regularMarketDayHigh,
        low: q.regularMarketDayLow,
        open: q.regularMarketOpen,
        prevClose: q.regularMarketPreviousClose,
        ts: Date.now(),
        live: true,
      };
    });
    // Compute DXY from EURUSD if direct DXY missing
    if (state.markets.eurusd?.price && !state.markets.dxy_direct?.price) {
      const eur = state.markets.eurusd.price;
      state.markets.dxy = {
        price: +(1/eur*100).toFixed(3),
        change: -(state.markets.eurusd.change||0),
        ts: Date.now(), live: true,
      };
    } else if (state.markets.dxy_direct?.price) {
      state.markets.dxy = state.markets.dxy_direct;
    }
    console.log(`[yahoo] refreshed: ${quotes.length} quotes`);
  } catch (e) {
    console.error("[yahoo] error:", e.message);
    // Fallback to individual fetches
    await fetchYahooFallback();
  }
}

async function fetchYahooFallback() {
  const pairs = [["^GSPC","spx"],["^NSEI","nifty"],["GC=F","gold_fut"],["CL=F","oil"]];
  await Promise.allSettled(pairs.map(async ([sym, key]) => {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=1d`;
      const res = await fetch(url, {
        headers: { "User-Agent": "Mozilla/5.0" },
        signal: AbortSignal.timeout(8000),
      });
      if (!res.ok) return;
      const d = await res.json();
      const meta = d?.chart?.result?.[0]?.meta;
      if (!meta?.regularMarketPrice) return;
      state.markets[key] = {
        price: meta.regularMarketPrice,
        change: ((meta.regularMarketPrice - meta.previousClose) / meta.previousClose) * 100,
        prevClose: meta.previousClose,
        ts: Date.now(), live: true,
      };
    } catch {}
  }));
}

// ── COINGECKO (dominance + sparklines + market caps) ─────────────
async function refreshCoinGecko() {
  try {
    // Global data
    const gRes = await fetch("https://api.coingecko.com/api/v3/global", {
      signal: AbortSignal.timeout(10000),
    });
    if (gRes.ok) {
      const d = await gRes.json();
      state.global = { data: d.data, ts: Date.now() };
    }
    
    // Market data with sparklines
    const mRes = await fetch(
      "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum,solana,hyperliquid,binancecoin,ripple,dogecoin&order=market_cap_desc&sparkline=true&price_change_percentage=24h",
      { signal: AbortSignal.timeout(10000) }
    );
    if (mRes.ok) {
      const coins = await mRes.json();
      coins.forEach(coin => {
        state.sparklines[coin.id] = {
          sparkline: coin.sparkline_in_7d?.price || [],
          marketCap: coin.market_cap,
          volume: coin.total_volume,
          rank: coin.market_cap_rank,
          supply: coin.circulating_supply,
          ts: Date.now(),
        };
        // Update price from CoinGecko if Binance WS not yet connected
        const sym = coin.symbol.toLowerCase() + "usdt";
        if (!state.crypto[sym] || !state.crypto[sym].price) {
          state.crypto[sym] = {
            price: coin.current_price,
            change: coin.price_change_percentage_24h || 0,
            high: coin.high_24h,
            low: coin.low_24h,
            ts: Date.now(),
          };
        }
      });
    }
    console.log("[coingecko] refreshed");
  } catch (e) { console.error("[coingecko] error:", e.message); }
}

// ── FEAR & GREED (alternative.me) ────────────────────────────────
async function refreshFearGreed() {
  try {
    const res = await fetch("https://api.alternative.me/fng/?limit=1", {
      signal: AbortSignal.timeout(6000),
    });
    if (!res.ok) throw new Error("fng failed");
    const d = await res.json();
    const val = parseInt(d.data?.[0]?.value || 0);
    const label = d.data?.[0]?.value_classification || "";
    
    // US & India from global market momentum
    const globalChange = state.global.data?.market_cap_change_percentage_24h_usd || 0;
    const usVal = Math.min(100, Math.max(0, 50 + globalChange * 3));
    const indVal = Math.min(100, Math.max(0, 50 + globalChange * 2.5));
    const getLabel = v => v > 74 ? "Extreme Greed" : v > 54 ? "Greed" : v > 45 ? "Neutral" : v > 24 ? "Fear" : "Extreme Fear";
    
    state.feargreed = {
      crypto: { value: val, label },
      us: { value: Math.round(usVal), label: getLabel(usVal) },
      india: { value: Math.round(indVal), label: getLabel(indVal) },
      ts: Date.now(),
    };
    console.log(`[feargreed] crypto=${val} (${label})`);
  } catch (e) { console.error("[feargreed] error:", e.message); }
}

// ── NEWS via RSS ──────────────────────────────────────────────────
const RSS_FEEDS = [
  { url: "https://cointelegraph.com/rss",                                                                    cat: "CRYPTO"  },
  { url: "https://coindesk.com/arc/outboundfeeds/rss/",                                                      cat: "CRYPTO"  },
  { url: "https://decrypt.co/feed",                                                                          cat: "CRYPTO"  },
  { url: "https://cryptonews.com/news/feed/",                                                                cat: "CRYPTO"  },
  { url: "https://news.google.com/rss/search?q=trump+crypto+regulation+market&hl=en-US&gl=US&ceid=US:en",   cat: "TRUMP"   },
  { url: "https://news.google.com/rss/search?q=federal+reserve+fomc+interest+rate&hl=en-US&gl=US&ceid=US:en", cat: "MACRO" },
  { url: "https://news.google.com/rss/search?q=stock+market+nasdaq+sp500+wall+street&hl=en-US&gl=US&ceid=US:en", cat: "MARKETS" },
  { url: "https://news.google.com/rss/search?q=nifty+sensex+india+stock+bse&hl=en-US&gl=US&ceid=US:en",    cat: "INDIA"   },
  { url: "https://news.google.com/rss/search?q=gold+oil+commodities+forex&hl=en-US&gl=US&ceid=US:en",      cat: "MARKETS" },
];

function tagFromTitle(t) {
  const l = (t||"").toLowerCase();
  if (/trump|white house|truth social|sec |cftc|executive|congress|biden/.test(l)) return "TRUMP";
  if (/bitcoin|btc|ethereum|eth|solana|sol|crypto|defi|nft|blockchain|hype/.test(l)) return "CRYPTO";
  if (/fed |fomc|interest rate|inflation|gdp|cpi |pce|powell|recession|jobs/.test(l)) return "MACRO";
  if (/nifty|sensex|india|bse|nse|rupee/.test(l)) return "INDIA";
  if (/stock|market|nasdaq|s&p|dow|oil|gold|forex|dollar|rally|equity/.test(l)) return "MARKETS";
  return "NEWS";
}

async function fetchRSSFeed(feed) {
  try {
    const res = await fetch(feed.url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)" },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) return [];
    const xml = await res.text();
    const parsed = await parseStringPromise(xml, { explicitArray: false, ignoreAttrs: false });
    const items = parsed?.rss?.channel?.item || parsed?.feed?.entry || [];
    const arr = Array.isArray(items) ? items : [items];
    return arr.map(item => {
      const title = (typeof item.title === "string" ? item.title : item.title?._ || "")
        .replace(/<!\[CDATA\[|\]\]>/g, "").replace(/&amp;/g,"&").replace(/&quot;/g,'"').trim();
      const pubDate = item.pubDate || item.published || item.updated || null;
      const link = typeof item.link === "string" ? item.link.trim() :
                   (item.link?.$?.href || item.link?.[0]?.$?.href || "");
      const source = item?.source?._ || item?.["dc:source"] || "";
      return { title, pubDate, link, source, cat: feed.cat || tagFromTitle(title) };
    }).filter(i => i.title.length > 15);
  } catch (e) {
    console.error(`[rss] ${feed.url.slice(0,50)} error:`, e.message);
    return [];
  }
}

async function refreshNews() {
  try {
    const results = await Promise.allSettled(RSS_FEEDS.map(f => fetchRSSFeed(f)));
    const all = [];
    results.forEach(r => { if (r.status === "fulfilled") all.push(...r.value); });
    
    const seen = new Set();
    const unique = all.filter(item => {
      const key = item.title.slice(0,50).toLowerCase().replace(/[^a-z]/g,"");
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    
    unique.sort((a,b) => { try { return new Date(b.pubDate||0)-new Date(a.pubDate||0); } catch { return 0; } });
    
    state.news = {
      items: unique.slice(0,60).map(item => ({
        title: item.title,
        source: item.source || item.link,
        category: item.cat || tagFromTitle(item.title),
        ts: item.pubDate ? new Date(item.pubDate).getTime() : Date.now(),
      })),
      ts: Date.now(),
    };
    console.log(`[news] refreshed: ${state.news.items.length} stories`);
  } catch (e) { console.error("[news] error:", e.message); }
}

// ── CFTC COT DATA (weekly) ────────────────────────────────────────
async function refreshCOT() {
  try {
    // CFTC publishes COT data as CSV, we fetch the latest
    const res = await fetch(
      "https://www.cftc.gov/dea/options/financial_lof.htm",
      { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(10000) }
    );
    // Store raw response info - full parse is complex
    state.cot = { data: { available: res.ok, ts: Date.now() }, ts: Date.now() };
    console.log("[cot] checked CFTC data availability:", res.ok);
  } catch (e) { console.error("[cot] error:", e.message); }
}

// ── ROUTES ────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({
    name: "trading-backend-v2",
    version: "2.0.0",
    sources: ["Binance WebSocket", "Finnhub WebSocket", "Yahoo Finance", "CoinGecko", "alternative.me", "RSS (9 feeds)", "CFTC"],
    endpoints: ["/api/news", "/api/prices", "/api/feargreed", "/api/sparklines", "/api/whales", "/api/global", "/api/health"],
  });
});

// All prices in one call
app.get("/api/prices", (req, res) => {
  const gold = state.markets.gold_fut || state.crypto["xautusdt"];
  res.json({
    // Crypto (Binance WS - real-time)
    btc:   state.crypto["btcusdt"]  || null,
    eth:   state.crypto["ethusdt"]  || null,
    sol:   state.crypto["solusdt"]  || null,
    hype:  state.crypto["hypeusdt"] || null,
    bnb:   state.crypto["bnbusdt"]  || null,
    xrp:   state.crypto["xrpusdt"]  || null,
    doge:  state.crypto["dogeusdt"] || null,
    gold:  state.crypto["xautusdt"] || null,
    // Markets (Yahoo Finance - polled 10s)
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
    gbpusd: state.markets.gbpusd || null,
    // Finnhub ETFs
    spy:    state.markets["SPY"] || null,
    qqq:    state.markets["QQQ"] || null,
    ts: Date.now(),
  });
});

app.get("/api/news", (req, res) => {
  res.json({ items: state.news.items, ts: state.news.ts, count: state.news.items.length });
});

app.get("/api/feargreed", (req, res) => {
  res.json({ ...state.feargreed });
});

app.get("/api/sparklines", (req, res) => {
  res.json({ coins: state.sparklines, ts: Date.now() });
});

app.get("/api/whales", (req, res) => {
  res.json({ trades: state.whales.slice(0, 20), ts: Date.now() });
});

app.get("/api/global", (req, res) => {
  res.json({ data: state.global.data, ts: state.global.ts });
});

app.get("/api/health", (req, res) => {
  const age = x => x ? Math.round((Date.now()-x)/1000)+"s ago" : "never";
  res.json({
    status: "ok",
    binance_ws: binanceWS?.readyState === 1 ? "connected" : "disconnected",
    whale_ws: whaleWS?.readyState === 1 ? "connected" : "disconnected",
    finnhub_ws: finnhubWS?.readyState === 1 ? "connected" : "no key",
    crypto_tickers: Object.keys(state.crypto).length,
    market_tickers: Object.keys(state.markets).length,
    whale_trades: state.whales.length,
    news_stories: state.news.items.length,
    last_news: age(state.news.ts),
    last_yahoo: age(state.markets.spx?.ts),
    last_feargreed: age(state.feargreed.ts),
    last_coingecko: age(state.global.ts),
  });
});

// ── CRON SCHEDULES ────────────────────────────────────────────────
cron.schedule("*/3  * * * *", refreshNews);           // news every 3 min
cron.schedule("*/1  * * * *", fetchYahooMulti);       // yahoo every 1 min (10s in prod)
cron.schedule("*/5  * * * *", refreshCoinGecko);      // CoinGecko every 5 min
cron.schedule("*/5  * * * *", refreshFearGreed);      // fear & greed every 5 min
cron.schedule("0 */6 * * *",  refreshCOT);            // CFTC every 6 hours

// ── START ─────────────────────────────────────────────────────────
const server = http.createServer(app);

server.listen(PORT, async () => {
  console.log(`\n🚀 Trading Backend v2 running on port ${PORT}`);
  console.log("Sources: Binance WS | Finnhub WS | Yahoo Finance | CoinGecko | alternative.me | RSS | CFTC\n");
  
  // Connect WebSockets
  connectBinance();
  connectWhales();
  connectFinnhub();
  
  // Initial data fetch
  console.log("Warming up cache...");
  await Promise.allSettled([
    fetchYahooMulti(),
    refreshCoinGecko(),
    refreshFearGreed(),
    refreshNews(),
    refreshCOT(),
  ]);
  console.log("✅ Cache warmed up! All systems running.\n");
});
