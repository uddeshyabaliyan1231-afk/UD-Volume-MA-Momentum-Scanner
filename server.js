
const express = require("express");
const axios = require("axios");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

const PORT = process.env.PORT || 3000;

const ACTIVE_URL =
  "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT";

const PUBLIC_URL = "https://public.coindcx.com";

const CONFIG = {
  scanInterval: 30000,
  concurrency: 4,
  candleLimit: 100,

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

function number(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

async function getJSON(url, params = {}) {
  const response = await axios.get(url, {
    params,
    timeout: 12000,
    headers: {
      "User-Agent": "UD-Volume-MA-Scanner/1.0"
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
  let pair = String(value).trim().toUpperCase();

  if (pair.startsWith("B-")) return pair;

  if (pair.includes("_")) return "B-" + pair;

  if (pair.endsWith("USDT")) {
    return "B-" + pair.slice(0, -4) + "_USDT";
  }

  return pair;
}

async function getActivePairs() {
  const data = await getJSON(ACTIVE_URL);

  if (!Array.isArray(data)) {
    throw new Error("Invalid CoinDCX active instruments response");
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

const INTERVAL_MS = {
  "5m": 5 * 60 * 1000,
  "15m": 15 * 60 * 1000,
  "1h": 60 * 60 * 1000
};

function normalizeCandles(data) {
  if (!Array.isArray(data)) return [];

  return data.map(c => {
    if (Array.isArray(c)) {
      return {
        time: number(c[0]),
        open: number(c[1]),
        high: number(c[2]),
        low: number(c[3]),
        close: number(c[4]),
        volume: number(c[5])
      };
    }

    return {
      time: number(c.time ?? c.t),
      open: number(c.open ?? c.o),
      high: number(c.high ?? c.h),
      low: number(c.low ?? c.l),
      close: number(c.close ?? c.c),
      volume: number(c.volume ?? c.v)
    };
  }).filter(c =>
    c.time > 0 &&
    c.open > 0 &&
    c.high > 0 &&
    c.low > 0 &&
    c.close > 0 &&
    c.volume >= 0
  ).sort((a, b) => a.time - b.time);
}

async function getCandles(pair, interval) {
  const endTime = Date.now();
  const startTime =
    endTime - INTERVAL_MS[interval] * CONFIG.candleLimit;

  const data = await getJSON(
    `${PUBLIC_URL}/market_data/candles`,
    {
      pair: formatPair(pair),
      interval,
      startTime,
      endTime
    }
  );

  const candles = normalizeCandles(data);

  // Incomplete candle is excluded.
  return candles.filter(c =>
    c.time + INTERVAL_MS[interval] <= Date.now()
  );
}

async function getOrderbook(pair) {
  return getJSON(
    `${PUBLIC_URL}/market_data/orderbook`,
    { pair: formatPair(pair) }
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

// Pattern A: flat/compressed volume MA followed by rising MA.
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

  // Every recent MA point must rise.
  for (let i = 1; i < rising.length; i++) {
    if (rising[i] <= rising[i - 1]) return null;
  }

  const rise =
    (rising[rising.length - 1] - rising[0]) / rising[0];

  if (rise < CONFIG.minimumRise) return null;

  // Current candle volume must confirm the MA movement.
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
    volumeRatio: Number((recentVolume / averageVolume).toFixed(2))
  };
}

// Pattern B: volume MA has peaked and starts falling.
function detectFallingVolumeMA(candles) {
  const volumes = candles.map(c => c.volume);
  const ma = sma(volumes, CONFIG.volumeMAPeriod)
    .filter(v => v !== null);

  const lookback = 8;
  const falling = ma.slice(-CONFIG.fallingLookback);

  if (
    ma.length < lookback + CONFIG.fallingLookback
  ) {
    return null;
  }

  // Each recent MA value must move down.
  for (let i = 1; i < falling.length; i++) {
    if (falling[i] >= falling[i - 1]) return null;
  }

  const previous = ma.slice(
    -(lookback + CONFIG.fallingLookback),
    -CONFIG.fallingLookback
  );

  const peak = Math.max(...previous);
  const last = falling[falling.length - 1];

  if (peak <= 0) return null;

  const fall = (peak - last) / peak;

  if (fall < CONFIG.minimumFall) return null;

  // The peak must be a genuine local high, not a flat MA.
  const peakIndex = previous.indexOf(peak);

  if (peakIndex < 1 || peakIndex === previous.length - 1) {
    return null;
  }

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
    volumeRatio: Number((recentVolume / averageVolume).toFixed(2))
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

  if (
    Array.isArray(levels) &&
    levels.length === 1 &&
    Array.isArray(levels[0])
  ) {
    levels = levels[0];
  }

  if (Array.isArray(levels)) {
    return levels.map(level => {
      if (Array.isArray(level)) {
        return {
          price: number(level[0]),
          quantity: number(level[1])
        };
      }

      return {
        price: number(level.price ?? level.rate),
        quantity: number(level.quantity ?? level.amount)
      };
    }).filter(x => x.price > 0 && x.quantity > 0);
  }

  if (typeof levels === "object") {
    return Object.entries(levels).map(([price, quantity]) => ({
      price: number(price),
      quantity: number(quantity)
    })).filter(x => x.price > 0 && x.quantity > 0);
  }

  return [];
}

function checkOrderbook(book, currentPrice) {
  const data = book?.data || book;

  const bids = parseBookLevels(data?.bids);
  const asks = parseBookLevels(data?.asks);

  if (!bids.length || !asks.length) {
    return { passed: false, reason: "Orderbook unavailable" };
  }

  const bestBid = Math.max(...bids.map(x => x.price));
  const bestAsk = Math.min(...asks.map(x => x.price));

  if (bestAsk <= bestBid || currentPrice <= 0) {
    return { passed: false, reason: "Invalid orderbook spread" };
  }

  const spread = (bestAsk - bestBid) / currentPrice;

  const bidDepth = bids
    .filter(x => x.price >= currentPrice * 0.99)
    .reduce((sum, x) => sum + x.price * x.quantity, 0);

  const askDepth = asks
    .filter(x => x.price <= currentPrice * 1.01)
    .reduce((sum, x) => sum + x.price * x.quantity, 0);

  if (spread > CONFIG.maximumSpread) {
    return { passed: false, reason: "Spread too high" };
  }

  if (
    bidDepth < CONFIG.minimumOrderbookDepth ||
    askDepth < CONFIG.minimumOrderbookDepth
  ) {
    return { passed: false, reason: "Insufficient orderbook depth" };
  }

  const imbalance =
    bidDepth / Math.max(askDepth, 1);

  // Avoid severely one-sided visible books.
  if (imbalance < 0.20 || imbalance > 5) {
    return { passed: false, reason: "Abnormal orderbook imbalance" };
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
    return { passed: false, reason: "Insufficient candle history" };
  }

  const last = candles[candles.length - 1];
  const previous = candles.slice(-9, -1);

  const range = last.high - last.low;

  if (range <= 0) {
    return { passed: false, reason: "Invalid candle range" };
  }

  const body = Math.abs(last.close - last.open);
  const upperWick =
    last.high - Math.max(last.open, last.close);
  const lowerWick =
    Math.min(last.open, last.close) - last.low;

  const bodyRatio = body / range;

  if (bodyRatio < 0.30) {
    return { passed: false, reason: "Weak candle body" };
  }

  if (
    direction === "BUY" &&
    upperWick / range > 0.45
  ) {
    return { passed: false, reason: "Upper wick rejection" };
  }

  if (
    direction === "SELL" &&
    lowerWick / range > 0.45
  ) {
    return { passed: false, reason: "Lower wick rejection" };
  }

  const priorHigh = Math.max(...previous.map(c => c.high));
  const priorLow = Math.min(...previous.map(c => c.low));

  if (
    direction === "BUY" &&
    last.high > priorHigh &&
    last.close < priorHigh
  ) {
    return { passed: false, reason: "Possible buy-side liquidity sweep" };
  }

  if (
    direction === "SELL" &&
    last.low < priorLow &&
    last.close > priorLow
  ) {
    return { passed: false, reason: "Possible sell-side liquidity sweep" };
  }

  if (
    direction === "BUY" &&
    last.close <= last.open
  ) {
    return { passed: false, reason: "No bullish candle confirmation" };
  }

  if (
    direction === "SELL" &&
    last.close >= last.open
  ) {
    return { passed: false, reason: "No bearish candle confirmation" };
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
    const [c5, c15, c1h, book] = await Promise.all([
      getCandles(pair, "5m"),
      getCandles(pair, "15m"),
      getCandles(pair, "1h"),
      getOrderbook(pair)
    ]);

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
      trend1h: trend1h,
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
    status.lastError = `${pair}: ${error.message}`;
    return [];
  }
}

async function scanMarket() {
  if (scanning) return;

  scanning = true;
  status.running = true;
  status.message = "Fetching CoinDCX active futures pairs";
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

      await sleep(250);
    }

    risingSignals = newRising
      .sort((a, b) =>
        b.maRisePercent - a.maRisePercent
      )
      .slice(0, CONFIG.maxSignalsPerSection);

    fallingSignals = newFalling
      .sort((a, b) =>
        b.maFallPercent - a.maFallPercent
      )
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

// API: settings
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

// API: scanner status
app.get("/api/status", (req, res) => {
  res.json({
    ...status,
    selectedScanLimit,
    options: SCAN_OPTIONS
  });
});

// API: rising volume MA signals
app.get("/api/signals/rising", (req, res) => {
  res.json({
    section: "RISING_VOLUME_MA",
    count: risingSignals.length,
    signals: risingSignals
  });
});

// API: falling volume MA signals
app.get("/api/signals/falling", (req, res) => {
  res.json({
    section: "FALLING_VOLUME_MA",
    count: fallingSignals.length,
    signals: fallingSignals
  });
});

// API: both signal groups
app.get("/api/signals", (req, res) => {
  res.json({
    rising: risingSignals,
    falling: fallingSignals
  });
});

// API: manual scan
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

// API: health check
app.get("/api/health", (req, res) => {
  res.json({
    online: true,
    scanner: "UD Volume MA Momentum Scanner",
    exchange: "CoinDCX",
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
