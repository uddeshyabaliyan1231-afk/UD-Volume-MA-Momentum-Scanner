
const express = require("express");
const axios = require("axios");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.PORT || 3000;

const API = "https://api.coindcx.com";
const FUTURES = "https://public.coindcx.com";

const CONFIG = {
  scanInterval: 180000,
  concurrency: 3,
  fiveMinuteCandles: 300,
  hourlyCandles: 70,

  volumeMAPeriod: 20,
  flatLookback: 6,
  risingLookback: 3,
  fallingLookback: 3,

  flatTolerance: 0.006,
  minimumRise: 0.008,
  minimumFall: 0.008,

  minimumQuoteVolume: 15000,
  minimumOrderbookDepth: 10000,
  maximumSpread: 0.0015,

  atrPeriod: 14,
  stopATR: 1.3,
  riskReward: 2,

  maxSignalsPerSection: 20
};

const SCAN_OPTIONS = [100, 500, 1000];

const EXCLUDED_COINS = [
  "BTC", "ETH", "SOL", "BNB", "XRP",
  "DOGE", "ADA", "TRX", "DOT", "LINK",
  "AVAX", "TON", "BCH"
];

let selectedScanLimit = 100;
let scanning = false;

let risingSignals = [];
let fallingSignals = [];

const status = {
  running: false,
  scanned: 0,
  totalPairs: 0,
  activePairs: 0,
  errors: 0,
  lastScan: null,
  message: "Starting scanner",
  lastError: null
};

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

async function getJSON(url, params = {}) {
  const response = await axios.get(url, {
    params,
    timeout: 15000,
    headers: {
      "User-Agent": "UD-Volume-MA-Scanner/2.0"
    }
  });

  return response.data;
}

function getBase(pair) {
  return String(pair)
    .replace(/^B-/i, "")
    .replace(/_USDT$/i, "")
    .replace(/USDT$/i, "")
    .toUpperCase();
}

function formatPair(value) {
  const pair = String(value).trim().toUpperCase();

  if (pair.startsWith("B-")) return pair;

  if (pair.includes("_")) return "B-" + pair;

  if (pair.endsWith("USDT")) {
    return "B-" + pair.slice(0, -4) + "_USDT";
  }

  return pair;
}

async function getActivePairs() {
  const data = await getJSON(
    `${API}/exchange/v1/derivatives/futures/data/active_instruments`,
    { "margin_currency_short_name[]": "USDT" }
  );

  if (!Array.isArray(data)) {
    throw new Error("Invalid active instruments response");
  }

  const pairs = data.map(item => {
    if (typeof item === "string") return item;

    return (
      item.pair ||
      item.symbol ||
      item.instrument_name ||
      item.market ||
      ""
    );
  }).filter(Boolean);

  return [...new Set(pairs.map(formatPair))].filter(pair => {
    const base = getBase(pair);

    return (
      pair.includes("_USDT") &&
      base.length > 0 &&
      !EXCLUDED_COINS.includes(base)
    );
  });
}

function normalizeCandles(response) {
  const data = Array.isArray(response)
    ? response
    : Array.isArray(response?.data)
      ? response.data
      : [];

  return data.map(c => ({
    time: num(c.time ?? c.t),
    open: num(c.open ?? c.o),
    high: num(c.high ?? c.h),
    low: num(c.low ?? c.l),
    close: num(c.close ?? c.c),
    volume: num(c.volume ?? c.v)
  }))
  .filter(c =>
    c.time > 0 &&
    c.open > 0 &&
    c.high > 0 &&
    c.low > 0 &&
    c.close > 0 &&
    c.volume >= 0
  )
  .sort((a, b) => a.time - b.time);
}

// CoinDCX futures candle API uses seconds,
// resolution values and pcode=f.
async function getFuturesCandles(pair, resolution, count, intervalMs) {
  const to = Math.floor(Date.now() / 1000);
  const from = Math.floor(
    (Date.now() - count * intervalMs) / 1000
  );

  const response = await getJSON(
    `${FUTURES}/market_data/candlesticks`,
    {
      pair: formatPair(pair),
      from,
      to,
      resolution,
      pcode: "f"
    }
  );

  if (
    response &&
    response.s &&
    response.s !== "ok"
  ) {
    throw new Error(
      `Futures candle API: ${response.s}`
    );
  }

  return normalizeCandles(response);
}

// Build 15-minute candles from completed 5-minute candles.
function aggregate15m(candles) {
  const groups = new Map();
  const interval = 15 * 60 * 1000;

  for (const candle of candles) {
    const bucket = Math.floor(candle.time / interval) * interval;

    if (!groups.has(bucket)) {
      groups.set(bucket, {
        time: bucket,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume
      });
    } else {
      const item = groups.get(bucket);
      item.high = Math.max(item.high, candle.high);
      item.low = Math.min(item.low, candle.low);
      item.close = candle.close;
      item.volume += candle.volume;
    }
  }

  const now = Date.now();

  return [...groups.values()]
    .filter(c => c.time + interval <= now)
    .sort((a, b) => a.time - b.time);
}

// Futures-specific order book endpoint.
async function getOrderbook(pair) {
  const instrument = formatPair(pair);

  return getJSON(
    `${FUTURES}/market_data/v3/orderbook/${instrument}-futures/50`
  );
}

function sma(values, period) {
  const output = [];

  for (let i = 0; i < values.length; i++) {
    if (i < period - 1) {
      output.push(null);
      continue;
    }

    const part = values.slice(i - period + 1, i + 1);

    output.push(
      part.reduce((a, b) => a + b, 0) / period
    );
  }

  return output;
}

function emaSeries(values, period) {
  const output = [];
  const multiplier = 2 / (period + 1);
  let current = null;

  values.forEach((value, index) => {
    if (index === period - 1) {
      current =
        values.slice(0, period).reduce((a, b) => a + b, 0) /
        period;
    } else if (index >= period) {
      current =
        (value - current) * multiplier + current;
    }

    output.push(current);
  });

  return output;
}

function getTrend(candles) {
  if (candles.length < 60) return "UNKNOWN";

  const closes = candles.map(c => c.close);
  const fast = emaSeries(closes, 20);
  const slow = emaSeries(closes, 50);

  const last = closes.length - 1;

  const currentFast = fast[last];
  const previousFast = fast[last - 3];
  const currentSlow = slow[last];

  if (
    currentFast === null ||
    previousFast === null ||
    currentSlow === null
  ) {
    return "UNKNOWN";
  }

  if (
    closes[last] > currentFast &&
    currentFast > currentSlow &&
    currentFast > previousFast
  ) {
    return "BULLISH";
  }

  if (
    closes[last] < currentFast &&
    currentFast < currentSlow &&
    currentFast < previousFast
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

// Flat volume MA followed by confirmed upward movement.
function detectRisingVolumeMA(candles) {
  const volumes = candles.map(c => c.volume);
  const ma = sma(volumes, CONFIG.volumeMAPeriod)
    .filter(v => v !== null);

  const required =
    CONFIG.flatLookback + CONFIG.risingLookback;

  if (ma.length < required) return null;

  const flat = ma.slice(
    -(CONFIG.flatLookback + CONFIG.risingLookback),
    -CONFIG.risingLookback
  );

  const rising = ma.slice(-CONFIG.risingLookback);

  const flatAverage =
    flat.reduce((a, b) => a + b, 0) / flat.length;

  if (flatAverage <= 0) return null;

  const flatRange =
    (Math.max(...flat) - Math.min(...flat)) / flatAverage;

  if (flatRange > CONFIG.flatTolerance) return null;

  for (let i = 1; i < rising.length; i++) {
    if (rising[i] <= rising[i - 1]) return null;
  }

  const rise =
    (rising[rising.length - 1] - rising[0]) / rising[0];

  if (rise < CONFIG.minimumRise) return null;

  const recentVolume = volumes[volumes.length - 1];
  const priorVolumes = volumes.slice(-21, -1);

  const averageVolume =
    priorVolumes.reduce((a, b) => a + b, 0) /
    Math.max(priorVolumes.length, 1);

  if (
    averageVolume <= 0 ||
    recentVolume < averageVolume * 1.10
  ) {
    return null;
  }

  return {
    pattern: "RISING_VOLUME_MA",
    maRisePercent: Number((rise * 100).toFixed(2)),
    volumeRatio: Number(
      (recentVolume / averageVolume).toFixed(2)
    )
  };
}

// Volume MA peak followed by confirmed downward movement.
function detectFallingVolumeMA(candles) {
  const volumes = candles.map(c => c.volume);
  const ma = sma(volumes, CONFIG.volumeMAPeriod)
    .filter(v => v !== null);

  const falling = ma.slice(-CONFIG.fallingLookback);

  if (
    ma.length < 11 ||
    falling.length < CONFIG.fallingLookback
  ) {
    return null;
  }

  for (let i = 1; i < falling.length; i++) {
    if (falling[i] >= falling[i - 1]) return null;
  }

  const previous = ma.slice(-11, -3);
  const peak = Math.max(...previous);
  const peakIndex = previous.indexOf(peak);
  const last = falling[falling.length - 1];

  if (
    peak <= 0 ||
    peakIndex < 1 ||
    peakIndex === previous.length - 1
  ) {
    return null;
  }

  const fall = (peak - last) / peak;

  if (fall < CONFIG.minimumFall) return null;

  const recentVolume = volumes[volumes.length - 1];
  const priorVolumes = volumes.slice(-21, -1);

  const averageVolume =
    priorVolumes.reduce((a, b) => a + b, 0) /
    Math.max(priorVolumes.length, 1);

  if (
    averageVolume <= 0 ||
    recentVolume > averageVolume * 1.10
  ) {
    return null;
  }

  return {
    pattern: "FALLING_VOLUME_MA",
    maFallPercent: Number((fall * 100).toFixed(2)),
    volumeRatio: Number(
      (recentVolume / averageVolume).toFixed(2)
    )
  };
}

function calculateATR(candles, period = CONFIG.atrPeriod) {
  if (candles.length < period + 1) return null;

  const ranges = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const p = candles[i - 1];

    ranges.push(Math.max(
      c.high - c.low,
      Math.abs(c.high - p.close),
      Math.abs(c.low - p.close)
    ));
  }

  const recent = ranges.slice(-period);

  return recent.reduce((a, b) => a + b, 0) / recent.length;
}

function parseBookLevels(levels) {
  if (!levels) return [];

  if (Array.isArray(levels)) {
    return levels.map(level => {
      if (Array.isArray(level)) {
        return {
          price: num(level[0]),
          quantity: num(level[1])
        };
      }

      return {
        price: num(level.price ?? level.rate),
        quantity: num(level.quantity ?? level.amount)
      };
    }).filter(x => x.price > 0 && x.quantity > 0);
  }

  if (typeof levels === "object") {
    return Object.entries(levels).map(([price, quantity]) => ({
      price: num(price),
      quantity: num(quantity)
    })).filter(x => x.price > 0 && x.quantity > 0);
  }

  return [];
}

function checkOrderbook(book, currentPrice) {
  const data = book?.data || book;

  const bids = parseBookLevels(data?.bids);
  const asks = parseBookLevels(data?.asks);

  if (!bids.length || !asks.length) {
    return {
      passed: false,
      reason: "Orderbook unavailable"
    };
  }

  const bestBid = Math.max(...bids.map(x => x.price));
  const bestAsk = Math.min(...asks.map(x => x.price));

  if (bestAsk <= bestBid || currentPrice <= 0) {
    return {
      passed: false,
      reason: "Invalid orderbook"
    };
  }

  const spread = (bestAsk - bestBid) / currentPrice;

  const bidDepth = bids
    .filter(x => x.price >= currentPrice * 0.99)
    .reduce((sum, x) => sum + x.price * x.quantity, 0);

  const askDepth = asks
    .filter(x => x.price <= currentPrice * 1.01)
    .reduce((sum, x) => sum + x.price * x.quantity, 0);

  if (spread > CONFIG.maximumSpread) {
    return {
      passed: false,
      reason: "Spread too high"
    };
  }

  if (
    bidDepth < CONFIG.minimumOrderbookDepth ||
    askDepth < CONFIG.minimumOrderbookDepth
  ) {
    return {
      passed: false,
      reason: "Insufficient orderbook depth"
    };
  }

  const imbalance = bidDepth / Math.max(askDepth, 1);

  if (imbalance < 0.20 || imbalance > 5) {
    return {
      passed: false,
      reason: "Abnormal orderbook imbalance"
    };
  }

  return {
    passed: true,
    spreadPercent: Number((spread * 100).toFixed(4)),
    bidDepth: Number(bidDepth.toFixed(2)),
    askDepth: Number(askDepth.toFixed(2))
  };
}

function checkLiquidityTrap(candles, direction) {
  if (candles.length < 10) {
    return {
      passed: false,
      reason: "Insufficient candle history"
    };
  }

  const last = candles[candles.length - 1];
  const previous = candles.slice(-9, -1);

  const range = last.high - last.low;

  if (range <= 0) {
    return {
      passed: false,
      reason: "Invalid candle range"
    };
  }

  const body = Math.abs(last.close - last.open);
  const upperWick =
    last.high - Math.max(last.open, last.close);
  const lowerWick =
    Math.min(last.open, last.close) - last.low;

  if (body / range < 0.30) {
    return {
      passed: false,
      reason: "Weak candle body"
    };
  }

  if (
    direction === "BUY" &&
    upperWick / range > 0.45
  ) {
    return {
      passed: false,
      reason: "Upper wick rejection"
    };
  }

  if (
    direction === "SELL" &&
    lowerWick / range > 0.45
  ) {
    return {
      passed: false,
      reason: "Lower wick rejection"
    };
  }

  const priorHigh = Math.max(...previous.map(c => c.high));
  const priorLow = Math.min(...previous.map(c => c.low));

  if (
    direction === "BUY" &&
    last.high > priorHigh &&
    last.close < priorHigh
  ) {
    return {
      passed: false,
      reason: "Possible buy-side liquidity sweep"
    };
  }

  if (
    direction === "SELL" &&
    last.low < priorLow &&
    last.close > priorLow
  ) {
    return {
      passed: false,
      reason: "Possible sell-side liquidity sweep"
    };
  }

  if (
    direction === "BUY" &&
    last.close <= last.open
  ) {
    return {
      passed: false,
      reason: "No bullish candle confirmation"
    };
  }

  if (
    direction === "SELL" &&
    last.close >= last.open
  ) {
    return {
      passed: false,
      reason: "No bearish candle confirmation"
    };
  }

  return { passed: true };
}

function buildTrade(candles, direction) {
  const atr = calculateATR(candles);

  if (!atr || atr <= 0) return null;

  const entry = candles[candles.length - 1].close;
  const distance = atr * CONFIG.stopATR;

  const stopLoss = direction === "BUY"
    ? entry - distance
    : entry + distance;

  const target = direction === "BUY"
    ? entry + distance * CONFIG.riskReward
    : entry - distance * CONFIG.riskReward;

  return {
    entry: Number(entry.toPrecision(9)),
    stopLoss: Number(stopLoss.toPrecision(9)),
    target: Number(target.toPrecision(9)),
    riskReward: CONFIG.riskReward
  };
}

async function analyzeCoin(pair) {
  try {
    const [c5, c1h, book] = await Promise.all([
      getFuturesCandles(
        pair,
        "5",
        CONFIG.fiveMinuteCandles,
        5 * 60 * 1000
      ),
      getFuturesCandles(
        pair,
        "60",
        CONFIG.hourlyCandles,
        60 * 60 * 1000
      ),
      getOrderbook(pair)
    ]);

    const c15 = aggregate15m(c5);

    if (
      c5.length < 60 ||
      c15.length < 60 ||
      c1h.length < 60
    ) {
      return [];
    }

    const last = c5[c5.length - 1];
    const price = last.close;

    if (price <= 0) return [];

    const quoteVolume = last.close * last.volume;

    if (quoteVolume < CONFIG.minimumQuoteVolume) {
      return [];
    }

    const trend5 = getTrend(c5);
    const trend15 = getTrend(c15);
    const trend1h = getTrend(c1h);

    let direction = null;

    if (
      trend5 === "BULLISH" &&
      trend15 === "BULLISH" &&
      trend1h === "BULLISH"
    ) {
      direction = "BUY";
    }

    if (
      trend5 === "BEARISH" &&
      trend15 === "BEARISH" &&
      trend1h === "BEARISH"
    ) {
      direction = "SELL";
    }

    if (!direction) return [];

    const rising = detectRisingVolumeMA(c5);
    const falling = detectFallingVolumeMA(c5);

    if (!rising && !falling) return [];

    const bookCheck = checkOrderbook(book, price);
    if (!bookCheck.passed) return [];

    const trapCheck = checkLiquidityTrap(c5, direction);
    if (!trapCheck.passed) return [];

    const trade = buildTrade(c5, direction);
    if (!trade) return [];

    const common = {
      coin: getBase(pair),
      pair,
      direction,
      timeframe: "5m",
      trend5m: trend5,
      trend15m: trend15,
      trend1h,
      liquidityStatus: "PASSED",
      trapProtection: "PASSED",
      spreadPercent: bookCheck.spreadPercent,
      quoteVolume: Number(quoteVolume.toFixed(2)),
      ...trade,
      timestamp: new Date().toISOString()
    };

    const results = [];

    if (rising) {
      results.push({
        ...common,
        section: "RISING",
        ...rising
      });
    }

    if (falling) {
      results.push({
        ...common,
        section: "FALLING",
        ...falling
      });
    }

    return results;

  } catch (error) {
    status.errors++;
    status.lastError = `${pair}: ${error.response?.status || ""} ${error.message}`;
    return [];
  }
}

async function scanMarket() {
  if (scanning) return;

  scanning = true;
  status.running = true;
  status.message = "Fetching CoinDCX futures pairs";
  status.errors = 0;
  status.lastError = null;

  const newRising = [];
  const newFalling = [];

  try {
    const activePairs = await getActivePairs();
    const pairs = activePairs.slice(0, selectedScanLimit);

    status.activePairs = activePairs.length;
    status.totalPairs = pairs.length;
    status.scanned = 0;

    for (
      let i = 0;
      i < pairs.length;
      i += CONFIG.concurrency
    ) {
      const batch = pairs.slice(i, i + CONFIG.concurrency);

      const results = await Promise.all(
        batch.map(pair => analyzeCoin(pair))
      );

      for (const group of results) {
        for (const signal of group) {
          if (signal.section === "RISING") {
            newRising.push(signal);
          }

          if (signal.section === "FALLING") {
            newFalling.push(signal);
          }
        }
      }

      status.scanned += batch.length;
      status.message =
        `Scanned ${status.scanned} of ${pairs.length} pairs`;

      await sleep(200);
    }

    risingSignals = newRising
      .sort((a, b) => b.maRisePercent - a.maRisePercent)
      .slice(0, CONFIG.maxSignalsPerSection);

    fallingSignals = newFalling
      .sort((a, b) => b.maFallPercent - a.maFallPercent)
      .slice(0, CONFIG.maxSignalsPerSection);

    status.lastScan = new Date().toISOString();

    status.message =
      `Rising: ${risingSignals.length} | Falling: ${fallingSignals.length}`;

  } catch (error) {
    status.message = "Market scan failed";
    status.lastError = error.message;
    status.errors++;
  } finally {
    scanning = false;
    status.running = false;
  }
}

app.get("/api/settings", (req, res) => {
  res.json({
    selectedScanLimit,
    options: SCAN_OPTIONS
  });
});

app.post("/api/settings", (req, res) => {
  const value = Number(req.body.scanLimit);

  if (!SCAN_OPTIONS.includes(value)) {
    return res.status(400).json({
      success: false,
      message: "Allowed scan limits: 100, 500, 1000"
    });
  }

  selectedScanLimit = value;

  res.json({
    success: true,
    selectedScanLimit
  });

  scanMarket();
});

app.get("/api/status", (req, res) => {
  res.json({
    ...status,
    selectedScanLimit,
    options: SCAN_OPTIONS
  });
});

app.get("/api/signals/rising", (req, res) => {
  res.json({
    section: "RISING_VOLUME_MA",
    count: risingSignals.length,
    signals: risingSignals
  });
});

app.get("/api/signals/falling", (req, res) => {
  res.json({
    section: "FALLING_VOLUME_MA",
    count: fallingSignals.length,
    signals: fallingSignals
  });
});

app.get("/api/signals", (req, res) => {
  res.json({
    rising: risingSignals,
    falling: fallingSignals
  });
});

app.post("/api/refresh", (req, res) => {
  if (scanning) {
    return res.json({
      success: false,
      message: "Scan already running"
    });
  }

  scanMarket();

  res.json({
    success: true,
    message: "Scan started"
  });
});

app.get("/api/health", (req, res) => {
  res.json({
    online: true,
    scanner: "UD Volume MA Momentum Scanner",
    exchange: "CoinDCX Futures",
    entryTimeframe: "5m",
    confirmationTimeframes: ["15m", "1h"],
    timestamp: new Date().toISOString()
  });
});

app.get("/", (req, res) => {
  res.sendFile("index.html", { root: "public" });
});

app.listen(PORT, () => {
  console.log(`UD Volume MA Scanner running on port ${PORT}`);

  scanMarket();

  setInterval(() => {
    scanMarket();
  }, CONFIG.scanInterval);
});
