#!/usr/bin/env python3
"""
KoiRyu v1 — Qullamaggie Swing Breakout Strategy
Standalone | Alpaca Paper | Polygon Scanner | Telegram

The koi that leaps the waterfall becomes a dragon —
consolidation becomes breakout.

Strategy: Nightly scan for tight consolidations in top RS stocks after
          large prior moves. Buy-stop at breakout level, trail with
          10/20-EMA daily close. Hold days to weeks.

Cadence:  Two runs per day — not continuous like intraday bots.
          16:15 ET — post-close scan, position management, order placement
          09:35 ET — fill check, volume confirmation, queued sells

Phases 1-5: scaffold, scanner, consolidation, regime, order management
"""

import os, sys, json, time, ssl, signal, math, threading
import urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta, date
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = timezone(timedelta(hours=-4))

# ── Config ────────────────────────────────────────────────────────────────────
POLYGON_KEY  = os.environ.get("POLYGON_API_KEY",    "")
ALPACA_KEY   = os.environ.get("ALPACA_API_KEY",     "")
ALPACA_SEC   = os.environ.get("ALPACA_SECRET_KEY",  "")
ALPACA_URL   = os.environ.get("ALPACA_BASE_URL",    "https://paper-api.alpaca.markets")
TG_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT      = os.environ.get("TELEGRAM_CHAT_ID",   "").strip()
SHEETS_URL   = os.environ.get("SHEETS_WEBHOOK_URL", "")
DASH_TOKEN   = os.environ.get("DASHBOARD_TOKEN",    "")
FMP_KEY      = os.environ.get("FMP_API_KEY",        "")   # stubbed — buy later
FINNHUB_KEY  = os.environ.get("FINNHUB_API_KEY",    "")
DRY_RUN      = os.environ.get("DRY_RUN", "1").lower() in ("1", "true", "yes")

# ── Strategy parameters ──────────────────────────────────────────────────────
RISK_PCT          = float(os.environ.get("RISK_PCT",          "0.004"))   # 0.4%
RISK_PCT_REDUCED  = RISK_PCT / 2                                          # after 3 consecutive losses
MAX_POSITIONS     = int(os.environ.get("MAX_POSITIONS",       "6"))
MAX_EXPOSURE_PCT  = float(os.environ.get("MAX_EXPOSURE_PCT",  "0.80"))
TRAILING_MA_FAST  = int(os.environ.get("TRAILING_MA_FAST",    "10"))      # 10-EMA
TRAILING_MA_SLOW  = int(os.environ.get("TRAILING_MA_SLOW",    "20"))      # 20-EMA
ADR_THRESHOLD     = float(os.environ.get("ADR_THRESHOLD",     "0.015"))   # 1.5% (was 2.4%; lowered to allow tight VCP consolidations)
MIN_DOLLAR_VOLUME = float(os.environ.get("MIN_DOLLAR_VOLUME", "60000000"))
RS_PERCENTILE     = float(os.environ.get("RS_PERCENTILE",     "0.93"))    # top 7%
VIX_MAX           = float(os.environ.get("VIX_MAX",           "35"))
REGIME_SPY_SMA    = int(os.environ.get("REGIME_SPY_SMA",      "50"))
CONSOL_MIN_DAYS   = int(os.environ.get("CONSOL_MIN_DAYS",     "10"))      # ~2 weeks
CONSOL_MAX_DAYS   = int(os.environ.get("CONSOL_MAX_DAYS",     "45"))      # ~2 months
PRIOR_MOVE_MIN    = float(os.environ.get("PRIOR_MOVE_MIN",    "0.20"))    # 20% (was 30%; lowered to catch early-stage momentum)
PRIOR_MOVE_MONTHS = int(os.environ.get("PRIOR_MOVE_MONTHS",   "3"))       # look back 3 months
VOLUME_CONFIRM    = float(os.environ.get("VOLUME_CONFIRM",    "1.5"))     # 1.5x avg vol on breakout
PARTIAL_EXIT_DAYS = int(os.environ.get("PARTIAL_EXIT_DAYS",   "4"))       # sell 1/3 after N days
PARTIAL_EXIT_PCT  = float(os.environ.get("PARTIAL_EXIT_PCT",  "0.25"))    # sell 25%
TIME_STOP_DAYS    = int(os.environ.get("TIME_STOP_DAYS",      "10"))      # no new high → exit
CONSEC_LOSS_LIMIT = int(os.environ.get("CONSEC_LOSS_LIMIT",   "3"))       # reduce risk after N losses
EARNINGS_WARN_DAYS = int(os.environ.get("EARNINGS_WARN_DAYS", "3"))       # warn N days before earnings
AGGRESSIVE_MAX_POS = int(os.environ.get("AGGRESSIVE_MAX_POS", "10"))      # max positions in AGGRESSIVE regime

# ── Schedule (ET) ────────────────────────────────────────────────────────────
POST_CLOSE_HOUR, POST_CLOSE_MIN = 16, 15    # nightly scan + position mgmt
MORNING_HOUR, MORNING_MIN       = 9,  35    # fill check + queued sells
CYCLE_SLEEP_SEC                 = 30         # poll interval during active windows

LOG_FILE     = "koiryu.log"
STATUS_FILE  = "koiryu_status.json"
TRADES_FILE  = "koiryu_trades.json"

_ssl = ssl.create_default_context()
TG   = f"https://api.telegram.org/bot{TG_TOKEN}"

# ── Logging ───────────────────────────────────────────────────────────────────
def now_et():
    return datetime.now(ET)

def log(lvl, msg):
    line = f"{now_et().strftime('%Y-%m-%d %H:%M:%S ET')} | {lvl:<5} | {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ── HTTP helpers ──────────────────────────────────────────────────────────────
def http(method, url, data=None, headers=None, timeout=15):
    try:
        hdrs = dict(headers) if headers else {}
        body = None
        if data is not None:
            body = json.dumps(data).encode()
            hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except Exception as e:
        if "/getUpdates" not in url:
            log("ERROR", f"{method} {url[:80]} -> {e}")
        return None

def GET(url, h=None):       return http("GET",    url, headers=h)
def POST(url, d, h=None):   return http("POST",   url, d, h)
def PATCH(url, d, h=None):  return http("PATCH",  url, d, h)
def DELETE(url, h=None):    return http("DELETE", url, headers=h)

# ── Telegram ──────────────────────────────────────────────────────────────────
def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        return
    r = POST(f"{TG}/sendMessage",
             {"chat_id": TG_CHAT, "text": text[:4000], "parse_mode": "Markdown"})
    if not r:
        POST(f"{TG}/sendMessage", {"chat_id": TG_CHAT, "text": text[:4000]})

def tg_poll(offset=0, timeout=25):
    if not TG_TOKEN:
        time.sleep(timeout)
        return []
    url = f"{TG}/getUpdates?offset={offset}&timeout={timeout}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout + 10, context=_ssl) as r:
            data = json.loads(r.read().decode())
            return data.get("result", [])
    except Exception:
        return []

# ── Google Sheets ─────────────────────────────────────────────────────────────
def sheets_push(payload):
    if not SHEETS_URL:
        return
    try:
        payload["bot_version"] = "KoiRyu v1"
        payload["timestamp"]   = now_et().isoformat()
        body = json.dumps(payload, default=str).encode()
        req  = urllib.request.Request(
            SHEETS_URL, data=body,
            headers={"Content-Type": "application/json"},
            method="POST")
        urllib.request.urlopen(req, timeout=10, context=_ssl)
        log("DEBUG", "Sheets push OK: " + payload.get("type", "?"))
    except Exception as e:
        log("DEBUG", "Sheets push failed (non-fatal): " + str(e))

# ── Alpaca helpers ────────────────────────────────────────────────────────────
def alp_h():
    return {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SEC}

def alp_get(p):     return GET(f"{ALPACA_URL}{p}", h=alp_h())
def alp_post(p, d): return POST(f"{ALPACA_URL}{p}", d, h=alp_h())
def alp_patch(p, d):return PATCH(f"{ALPACA_URL}{p}", d, h=alp_h())
def alp_del(p):     return DELETE(f"{ALPACA_URL}{p}", h=alp_h())

def get_account():   return alp_get("/v2/account") or {}
def get_equity():    return float(get_account().get("equity", 0))
def get_positions(): return alp_get("/v2/positions") or []
def get_orders(status="open"):
    return alp_get(f"/v2/orders?status={status}&limit=100") or []

def cancel_order(order_id):
    return alp_del(f"/v2/orders/{order_id}")

def close_position(sym):
    return alp_del(f"/v2/positions/{sym}")

# ── Polygon helpers ───────────────────────────────────────────────────────────
def poly_get(url):
    sep = "&" if "?" in url else "?"
    return GET(f"{url}{sep}apiKey={POLYGON_KEY}")

def poly_daily_bars(sym, start_date, end_date, limit=250):
    """Fetch daily OHLCV bars from Polygon."""
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}")
    resp = poly_get(url)
    if resp and resp.get("results"):
        return resp["results"]
    return []

def poly_grouped_daily(date_str):
    """Fetch all tickers' daily bars for a single date (grouped endpoint)."""
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=true"
    resp = poly_get(url)
    if resp and resp.get("results"):
        return resp["results"]
    return []

def poly_snapshot_all():
    """Fetch snapshot for all US tickers — includes prev day + today aggregates."""
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    resp = poly_get(url)
    if resp and resp.get("tickers"):
        return resp["tickers"]
    return []

def poly_ticker_details(sym):
    """Get ticker details (market_cap, share_class_shares_outstanding, etc.)."""
    url = f"https://api.polygon.io/v3/reference/tickers/{sym}"
    return poly_get(url)

# ── Finnhub helpers ───────────────────────────────────────────────────────────
def finnhub_earnings_calendar(from_date, to_date):
    """Fetch upcoming earnings dates. Returns list of {symbol, date, ...}."""
    if not FINNHUB_KEY:
        return []
    url = (f"https://finnhub.io/api/v1/calendar/earnings"
           f"?from={from_date}&to={to_date}&token={FINNHUB_KEY}")
    resp = GET(url)
    if resp and resp.get("earningsCalendar"):
        return resp["earningsCalendar"]
    return []

# ── FMP helpers (stubbed — activate when API key purchased) ───────────────────
def fmp_fundamentals(sym):
    """Fetch EPS/revenue growth. Returns (eps_growth_qoq, eps_growth_annual) or None."""
    if not FMP_KEY:
        return None
    url = (f"https://financialmodelingprep.com/api/v3/income-statement/{sym}"
           f"?period=quarter&limit=5&apikey={FMP_KEY}")
    resp = GET(url)
    if not resp or len(resp) < 2:
        return None
    try:
        curr_eps = float(resp[0].get("eps", 0))
        prev_eps = float(resp[1].get("eps", 0))
        if prev_eps > 0:
            eps_growth_qoq = (curr_eps - prev_eps) / prev_eps
        else:
            eps_growth_qoq = 0
        # Annual: compare current Q to same Q last year (index 4 if available)
        eps_growth_annual = 0
        if len(resp) >= 5:
            yr_ago_eps = float(resp[4].get("eps", 0))
            if yr_ago_eps > 0:
                eps_growth_annual = (curr_eps - yr_ago_eps) / yr_ago_eps
        return (eps_growth_qoq, eps_growth_annual)
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: NIGHTLY SCANNER
# Scan full US equity universe for momentum stocks meeting:
#   - $60M+ avg daily dollar volume
#   - ADR% >= 2.4%
#   - Top 7% relative strength (1mo/3mo)
#   - MA alignment (price > 10-EMA > 20-EMA; 50-SMA rising)
#   - Prior move 30%+ in past 3 months
# ══════════════════════════════════════════════════════════════════════════════

def calc_sma(closes, period):
    """Simple moving average of last N closes."""
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period

def calc_ema(closes, period):
    """Exponential moving average."""
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
    return ema

def calc_adr_pct(bars):
    """Average daily range % over the bars provided."""
    if not bars:
        return 0
    ranges = []
    for b in bars:
        h, l = b.get("h", b.get("high", 0)), b.get("l", b.get("low", 0))
        mid = (h + l) / 2
        if mid > 0:
            ranges.append((h - l) / mid)
    return sum(ranges) / len(ranges) if ranges else 0

def calc_dollar_volume(bars):
    """Average daily dollar volume."""
    if not bars:
        return 0
    dvols = []
    for b in bars:
        v = b.get("v", b.get("volume", 0))
        c = b.get("c", b.get("close", 0))
        dvols.append(v * c)
    return sum(dvols) / len(dvols) if dvols else 0

def calc_relative_strength(bars_60d):
    """% gain over the bars (used for 1mo ~20d and 3mo ~60d lookback)."""
    if not bars_60d or len(bars_60d) < 2:
        return 0
    first = bars_60d[0].get("c", bars_60d[0].get("close", 0))
    last  = bars_60d[-1].get("c", bars_60d[-1].get("close", 0))
    if first <= 0:
        return 0
    return (last - first) / first

def check_ma_alignment(closes):
    """
    Check stage-2 MA alignment:
      price > 10-EMA > 20-EMA; 50-SMA rising
    Returns (aligned: bool, ema10, ema20, sma50).
    """
    if len(closes) < 55:
        return False, 0, 0, 0
    ema10 = calc_ema(closes, 10)
    ema20 = calc_ema(closes, 20)
    sma50 = calc_sma(closes, 50)
    if not all([ema10, ema20, sma50]):
        return False, 0, 0, 0
    price = closes[-1]
    # 50-SMA rising: current > 10 days ago
    sma50_prev = calc_sma(closes[:-10], 50) if len(closes) >= 60 else sma50
    sma50_rising = sma50 >= (sma50_prev or sma50)
    aligned = (price > ema10 > ema20 and sma50_rising and price > sma50)
    return aligned, ema10, ema20, sma50

def detect_prior_move(bars, min_gain=0.30):
    """
    Check if stock had a 30%+ move in the bars provided (typically 60-day lookback).
    Returns (has_move, gain_pct, move_start_idx, move_end_idx).
    """
    if len(bars) < 10:
        return False, 0, 0, 0
    closes = [b.get("c", b.get("close", 0)) for b in bars]
    best_gain = 0
    best_start = 0
    best_end = 0
    # Find the max gain from any trough to any subsequent peak
    min_price = closes[0]
    min_idx = 0
    for i in range(1, len(closes)):
        if closes[i] < min_price:
            min_price = closes[i]
            min_idx = i
        if min_price > 0:
            gain = (closes[i] - min_price) / min_price
            if gain > best_gain:
                best_gain = gain
                best_start = min_idx
                best_end = i
    return best_gain >= min_gain, best_gain, best_start, best_end

def nightly_scan():
    """
    Full US equity universe scan. Returns list of candidates:
    [{sym, close, adr_pct, dollar_vol, rs_1mo, rs_3mo, ema10, ema20, sma50, prior_gain}, ...]
    """
    log("INFO", "=== NIGHTLY SCAN START ===")
    today = now_et().date()
    end   = today.isoformat()
    # Need ~65 trading days of history for MAs + prior move detection
    start = (today - timedelta(days=PRIOR_MOVE_MONTHS * 30 + 30)).isoformat()

    # Step 1: Get all active tickers from Polygon snapshot
    log("INFO", "Fetching universe snapshot...")
    snapshots = poly_snapshot_all()
    if not snapshots:
        log("ERROR", "Snapshot returned empty — cannot scan")
        return []
    log("INFO", f"  Snapshot: {len(snapshots)} tickers")

    # Step 2: Pre-filter on snapshot data (price, volume, change)
    prefilt = []
    for t in snapshots:
        try:
            sym = t.get("ticker", "")
            if not sym or len(sym) > 5:
                continue
            day = t.get("day", {})
            prev = t.get("prevDay", {})
            close = day.get("c", 0) or prev.get("c", 0)
            volume = day.get("v", 0) or prev.get("v", 0)
            if close < 5 or close > 1500:
                continue
            if volume < 200_000:
                continue
            # Rough dollar volume filter — will refine with 20d avg
            if close * volume < MIN_DOLLAR_VOLUME * 0.5:
                continue
            prefilt.append(sym)
        except Exception:
            continue
    log("INFO", f"  Pre-filter: {len(prefilt)} passed (price $5-1500, vol > 200K)")

    # Step 3: Fetch daily bars and apply full filters
    # Polygon paid plan ($29/mo) rate limit: be conservative, ~2 calls/sec
    candidates = []
    call_count = 0
    log("INFO", f"  Fetching daily bars for {len(prefilt)} pre-filtered tickers...")
    for sym in prefilt:
        try:
            bars = poly_daily_bars(sym, start, end, limit=120)
            call_count += 1
            # Rate limit: pause every 5 calls to stay under ~100 calls/min
            if call_count % 5 == 0:
                time.sleep(3)
            if not bars or len(bars) < 30:
                continue
            closes = [b["c"] for b in bars]
            last_20 = bars[-20:] if len(bars) >= 20 else bars

            # Dollar volume (20d avg)
            dvol = calc_dollar_volume(last_20)
            if dvol < MIN_DOLLAR_VOLUME:
                continue

            # ADR% (20d)
            adr = calc_adr_pct(last_20)
            if adr < ADR_THRESHOLD:
                continue

            # MA alignment
            aligned, ema10, ema20, sma50 = check_ma_alignment(closes)
            if not aligned:
                continue

            # Prior move (30%+ in lookback window)
            has_move, prior_gain, _, _ = detect_prior_move(bars, PRIOR_MOVE_MIN)
            if not has_move:
                continue

            # Relative strength (1mo and 3mo % gain)
            rs_1mo = calc_relative_strength(bars[-20:]) if len(bars) >= 20 else 0
            rs_3mo = calc_relative_strength(bars[-60:]) if len(bars) >= 60 else calc_relative_strength(bars)

            candidates.append({
                "sym":        sym,
                "close":      closes[-1],
                "adr_pct":    round(adr * 100, 2),
                "dollar_vol": round(dvol / 1e6, 1),
                "rs_1mo":     round(rs_1mo * 100, 1),
                "rs_3mo":     round(rs_3mo * 100, 1),
                "ema10":      round(ema10, 2),
                "ema20":      round(ema20, 2),
                "sma50":      round(sma50, 2),
                "prior_gain": round(prior_gain * 100, 1),
            })
        except Exception as e:
            log("DEBUG", f"  Scan skip {sym}: {e}")
            continue
    log("INFO", f"  Bar fetches: {call_count} API calls")

    # Step 4: RS percentile filter — keep top 7%
    if candidates:
        candidates.sort(key=lambda c: c["rs_3mo"], reverse=True)
        cutoff = max(1, int(len(candidates) * (1 - RS_PERCENTILE)))
        candidates = candidates[:cutoff]

    log("INFO", f"  Scanner output: {len(candidates)} candidates after RS filter")
    for c in candidates[:10]:
        log("INFO", f"    {c['sym']}: RS3mo={c['rs_3mo']}% prior={c['prior_gain']}% ADR={c['adr_pct']}% dvol=${c['dollar_vol']}M")

    return candidates


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: CONSOLIDATION DETECTOR
# From scanner output, identify stocks in qualifying consolidations.
# Layer 1: 5-point base quality score (pattern-agnostic)
#   1. Range tightening (5d range < 10d prior range)
#   2. Higher lows (5d swing lows ascending)
#   3. Volume decline (current week vol < 50% of peak week)
#   4. BBW compression (optional)
#   5. Proximity to 10-SMA (<= 2%)
# Score >= 4 qualifies as orderly consolidation.
# ══════════════════════════════════════════════════════════════════════════════

def calc_bbw(closes, period=20):
    """Bollinger Band Width: (upper - lower) / middle."""
    if len(closes) < period:
        return None
    sma = sum(closes[-period:]) / period
    variance = sum((c - sma) ** 2 for c in closes[-period:]) / period
    std = math.sqrt(variance)
    if sma <= 0:
        return None
    return (2 * 2 * std) / sma  # 2-std bands, width as fraction

def find_consolidation_range(bars):
    """
    Find the consolidation high (breakout level) and range.
    Returns (consol_high, consol_low, consol_days) or None.
    """
    if len(bars) < CONSOL_MIN_DAYS:
        return None
    # Work backwards from the most recent bar to find the consolidation range
    # Consolidation = period where price stays within a tightening range
    recent = bars[-CONSOL_MAX_DAYS:] if len(bars) >= CONSOL_MAX_DAYS else bars
    highs = [b.get("h", b.get("high", 0)) for b in recent]
    lows  = [b.get("l", b.get("low", 0)) for b in recent]

    if not highs or not lows:
        return None

    consol_high = max(highs[-CONSOL_MIN_DAYS:])
    consol_low  = min(lows[-CONSOL_MIN_DAYS:])
    consol_days = len(recent)

    # The consolidation range should be reasonably tight
    if consol_high <= 0:
        return None
    range_pct = (consol_high - consol_low) / consol_high
    if range_pct > 0.20:  # more than 20% range = not a consolidation
        return None

    return consol_high, consol_low, consol_days

def score_consolidation(bars):
    """
    Layer 1: Pattern-agnostic base quality score (0-5).
    Returns (score, details_dict, consol_high, consol_low).
    """
    if len(bars) < 15:
        return 0, {}, 0, 0

    highs  = [b.get("h", b.get("high", 0)) for b in bars]
    lows   = [b.get("l", b.get("low", 0)) for b in bars]
    closes = [b.get("c", b.get("close", 0)) for b in bars]
    vols   = [b.get("v", b.get("volume", 0)) for b in bars]

    score = 0
    details = {}

    # 1. Range tightening: last 5d range < prior 10d range
    if len(bars) >= 15:
        range_5d  = max(highs[-5:])  - min(lows[-5:])
        range_10d = max(highs[-15:-5]) - min(lows[-15:-5])
        tight = range_5d < range_10d if range_10d > 0 else False
        if tight:
            score += 1
        details["range_tightening"] = tight

    # 2. Higher lows: compare 5d swing lows
    if len(bars) >= 10:
        low_prev5 = min(lows[-10:-5])
        low_last5 = min(lows[-5:])
        higher = low_last5 >= low_prev5
        if higher:
            score += 1
        details["higher_lows"] = higher

    # 3. Volume decline: current week's avg vol < 50% of highest week in lookback
    if len(vols) >= 10:
        # Weekly chunks
        week_vols = []
        for w in range(0, len(vols) - 4, 5):
            chunk = vols[w:w+5]
            if chunk:
                week_vols.append(sum(chunk) / len(chunk))
        curr_week_vol = sum(vols[-5:]) / 5 if len(vols) >= 5 else sum(vols) / len(vols)
        peak_week_vol = max(week_vols) if week_vols else curr_week_vol
        vol_decline = curr_week_vol < (peak_week_vol * 0.50) if peak_week_vol > 0 else False
        if vol_decline:
            score += 1
        details["volume_decline"] = vol_decline

    # 4. BBW compression: 20d BBW < stock's ADR% (volatility compressing)
    if len(closes) >= 20:
        bbw = calc_bbw(closes, 20)
        adr = calc_adr_pct(bars[-20:])
        if bbw is not None and adr > 0:
            compressed = bbw < adr
            if compressed:
                score += 1
            details["bbw_compressed"] = compressed
        else:
            details["bbw_compressed"] = False
    else:
        details["bbw_compressed"] = False

    # 5. Proximity to 10-SMA: within 3% (was 2%; widened for tight bases that sit slightly further)
    if len(closes) >= 10:
        sma10 = sum(closes[-10:]) / 10
        if sma10 > 0:
            proximity = abs(closes[-1] - sma10) / closes[-1]
            near = proximity <= 0.03
            if near:
                score += 1
            details["near_10sma"] = near
            details["proximity_pct"] = round(proximity * 100, 2)
        else:
            details["near_10sma"] = False
    else:
        details["near_10sma"] = False

    # Find consolidation high/low
    consol = find_consolidation_range(bars)
    consol_high = consol[0] if consol else max(highs[-CONSOL_MIN_DAYS:])
    consol_low  = consol[1] if consol else min(lows[-CONSOL_MIN_DAYS:])

    return score, details, consol_high, consol_low

def detect_consolidations(candidates, regime=None):
    """
    For each scanner candidate, fetch recent daily bars and score consolidation.
    Returns candidates enriched with consolidation data, filtered by score.
    Score threshold: >= 3 in AGGRESSIVE regime, >= 4 otherwise.
    """
    log("INFO", "=== CONSOLIDATION DETECTION ===")
    today = now_et().date()
    end   = today.isoformat()
    start = (today - timedelta(days=90)).isoformat()

    # In AGGRESSIVE regime (post-correction recovery), accept looser consolidations
    regime_name = regime["regime"] if regime else "FULL_RISK"
    min_score = 3 if regime_name == "AGGRESSIVE" else 4
    log("INFO", f"  Consolidation min score: {min_score}/5 (regime={regime_name})")

    qualified = []
    for cand in candidates:
        sym = cand["sym"]
        try:
            bars = poly_daily_bars(sym, start, end, limit=65)
            if not bars or len(bars) < 15:
                continue

            score, details, consol_high, consol_low = score_consolidation(bars)
            if score < min_score:
                log("DEBUG", f"  {sym}: consol score {score}/5 — skip (need {min_score})")
                continue

            closes = [b["c"] for b in bars]
            adr = calc_adr_pct(bars[-20:]) if len(bars) >= 20 else calc_adr_pct(bars)
            stop_width = consol_high - consol_low
            max_stop = closes[-1] * adr  # 1x ADR
            if stop_width > max_stop and max_stop > 0:
                log("DEBUG", f"  {sym}: stop too wide ({stop_width:.2f} > {max_stop:.2f} ADR)")
                continue

            cand["consol_score"]   = score
            cand["consol_details"] = details
            cand["consol_high"]    = round(consol_high, 2)
            cand["consol_low"]     = round(consol_low, 2)
            cand["stop_price"]     = round(consol_low, 2)
            cand["stop_width_pct"] = round((stop_width / consol_high * 100) if consol_high > 0 else 0, 2)
            qualified.append(cand)

            log("INFO", f"  {sym}: score={score}/5 breakout={consol_high:.2f} stop={consol_low:.2f} "
                         f"width={cand['stop_width_pct']}% | {details}")
        except Exception as e:
            log("DEBUG", f"  {sym} consol error: {e}")
            continue
        time.sleep(0.3)  # rate limit

    # Sort by total score: RS rank + consolidation score
    qualified.sort(key=lambda c: (c["consol_score"], c["rs_3mo"]), reverse=True)
    log("INFO", f"  Consolidation output: {len(qualified)} qualified setups")
    return qualified


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4: MARKET REGIME FILTER
# Gate new entries on market health. State machine with 4 modes:
#   AGGRESSIVE — post-correction recovery (max positions 10-12)
#   FULL_RISK  — normal bull market (max positions 6-8)
#   REDUCED    — mixed signals (max positions 3, reduced risk)
#   NO_ENTRY   — bear / high VIX (manage existing only)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_index_data(sym, days=250):
    """Fetch daily bars for index ETFs (SPY, QQQ, VIX-proxy)."""
    today = now_et().date()
    end   = today.isoformat()
    start = (today - timedelta(days=int(days * 1.5))).isoformat()
    bars = poly_daily_bars(sym, start, end, limit=days)
    return bars

def check_regime():
    """
    Evaluate market regime. Returns:
    {regime, max_positions, risk_pct, spy_above_50, qqq_above_50,
     vix, spy_correction_recovery, details}
    """
    log("INFO", "=== REGIME CHECK ===")

    regime = "FULL_RISK"
    risk = RISK_PCT
    max_pos = MAX_POSITIONS
    details = []

    # SPY check
    spy_bars = fetch_index_data("SPY", 250)
    spy_ok = False
    spy_recovery = False
    if spy_bars and len(spy_bars) >= 200:
        spy_closes = [b["c"] for b in spy_bars]
        sma50  = calc_sma(spy_closes, REGIME_SPY_SMA)
        sma200 = calc_sma(spy_closes, 200)
        sma20  = calc_sma(spy_closes, 20)
        price  = spy_closes[-1]

        # Primary check: golden cross (50 > 200) + price above 50
        spy_ok = (price > sma50 and sma50 > sma200) if sma50 and sma200 else False
        # V-recovery override: in sharp recoveries (e.g. Iran selloff reversal),
        # price can be above BOTH MAs while 50 SMA still lags below 200 SMA.
        # If price is above 200 SMA by 3%+ AND above 50 SMA, treat as OK.
        # This prevents NO_ENTRY blocking during strong V-shaped recoveries.
        if not spy_ok and sma50 and sma200 and price > sma50 and price > sma200 * 1.03:
            spy_ok = True
            details.append(f"SPY: {price:.0f} above both MAs (50sma={sma50:.0f} < 200sma={sma200:.0f} but V-recovery override)")
        else:
            details.append(f"SPY: {price:.0f} vs 50sma={sma50:.0f} 200sma={sma200:.0f} -> {'OK' if spy_ok else 'FAIL'}")

        # AGGRESSIVE detection: SPY reclaims 20-SMA after a 5%+ pullback
        if sma20 and price > sma20:
            # Check if there was a 5%+ pullback in the last 30 days
            recent_30 = spy_closes[-30:]
            recent_high = max(recent_30)
            recent_low  = min(recent_30)
            if recent_high > 0:
                pullback = (recent_high - recent_low) / recent_high
                if pullback >= 0.05:
                    # Price now above 20-SMA = reclaiming after correction
                    spy_recovery = True
                    details.append(f"SPY: post-correction recovery ({pullback*100:.1f}% pullback, now above 20-SMA)")
    else:
        details.append("SPY: insufficient data")

    # QQQ check
    qqq_bars = fetch_index_data("QQQ", 60)
    qqq_ok = False
    if qqq_bars and len(qqq_bars) >= 50:
        qqq_closes = [b["c"] for b in qqq_bars]
        qqq_sma50 = calc_sma(qqq_closes, 50)
        qqq_price = qqq_closes[-1]
        qqq_ok = qqq_price > qqq_sma50 if qqq_sma50 else False
        details.append(f"QQQ: {qqq_price:.0f} vs 50sma={qqq_sma50:.0f} -> {'OK' if qqq_ok else 'FAIL'}")
    else:
        details.append("QQQ: insufficient data")

    # VIX check — Polygon indexes use "I:" prefix (e.g. I:VIX)
    vix_val = 0
    # Method 1: Polygon index prev-day close (most reliable)
    vix_prev = poly_get("https://api.polygon.io/v2/aggs/ticker/I:VIX/prev")
    if vix_prev and vix_prev.get("results"):
        vix_val = vix_prev["results"][0].get("c", 0)
    # Method 2: Polygon index snapshot
    if vix_val <= 0:
        vix_snap = poly_get("https://api.polygon.io/v3/snapshot?ticker.any_of=I:VIX")
        if vix_snap and vix_snap.get("results"):
            for r in vix_snap["results"]:
                if r.get("session", {}).get("close"):
                    vix_val = r["session"]["close"]
                elif r.get("session", {}).get("previous_close"):
                    vix_val = r["session"]["previous_close"]
    # Method 3: VIXY ETF as rough proxy (tracks VIX futures, not VIX spot, but directionally similar)
    if vix_val <= 0:
        vixy_bars = fetch_index_data("VIXY", 5)
        if vixy_bars:
            # VIXY doesn't map 1:1 to VIX level — just check if it's elevated
            # VIXY > $25 roughly corresponds to VIX > 30
            vixy_close = vixy_bars[-1].get("c", 0)
            if vixy_close > 25:
                vix_val = 36  # treat as above threshold
                details.append(f"VIX: unavailable, VIXY={vixy_close:.1f} (elevated) -> HIGH")
            else:
                vix_val = 20  # treat as normal
                details.append(f"VIX: unavailable, VIXY={vixy_close:.1f} (normal) -> OK")
    vix_ok = vix_val < VIX_MAX if vix_val > 0 else True  # if we can't get VIX, don't block
    if not any("VIX" in d for d in details):  # don't duplicate if VIXY fallback already added
        details.append(f"VIX: {vix_val:.1f} -> {'OK' if vix_ok else 'HIGH'}")

    # Determine regime
    if not spy_ok:
        regime = "NO_ENTRY"
        max_pos = 0
        risk = 0
    elif not qqq_ok or not vix_ok:
        regime = "REDUCED"
        max_pos = 3
        risk = RISK_PCT_REDUCED
    elif spy_recovery:
        regime = "AGGRESSIVE"
        max_pos = AGGRESSIVE_MAX_POS
        risk = RISK_PCT
    else:
        regime = "FULL_RISK"
        max_pos = MAX_POSITIONS
        risk = RISK_PCT

    result = {
        "regime":       regime,
        "max_positions": max_pos,
        "risk_pct":     risk,
        "spy_ok":       spy_ok,
        "qqq_ok":       qqq_ok,
        "vix":          vix_val,
        "vix_ok":       vix_ok,
        "spy_recovery": spy_recovery,
        "details":      details,
    }

    log("INFO", f"  Regime: {regime} | max_pos={max_pos} risk={risk*100:.2f}%")
    for d in details:
        log("INFO", f"    {d}")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5: ORDER MANAGEMENT
# Full trade lifecycle:
#   - Buy-stop placement at consolidation high
#   - Position sizing: (equity * risk%) / (entry - stop)
#   - Fill confirmation + volume check (cancel if weak)
#   - Partial exit after 3-5 days (sell 25%)
#   - Trailing stop via 10-EMA or 20-EMA daily close
#   - Full exit on daily close below trailing MA
#   - Time stop: 10 days no new high
#   - Drawdown scaling: 3 consecutive losses -> reduce risk
#   - Earnings avoidance: sell or warn before earnings
#   - Re-entry: if stopped out but setup still valid, re-enters next scan
# ══════════════════════════════════════════════════════════════════════════════

class BotState:
    """Persistent state across restarts."""
    def __init__(self):
        self.positions = {}       # {sym: {entry, stop, consol_high, consol_low, shares,
                                  #         entry_date, highest_high, partial_taken,
                                  #         trailing_ma, adr_pct, consol_score}}
        self.pending_sells = []   # [{sym, reason, shares}]
        self.pending_buys  = []   # [{sym, consol_high, stop, shares, score}]
        self.regime = "FULL_RISK"
        self.regime_data = {}
        self.consecutive_losses = 0
        self.scan_results = []
        self.today_date = ""
        self.offset = 0           # Telegram offset
        self.last_scan_date = ""
        self.last_morning_date = ""
        self.last_weekly_date = ""
        self.trade_history = []   # [{sym, entry, exit, pnl, r_multiple, hold_days, exit_reason, date}]
        self.load()

    def load(self):
        try:
            with open(STATUS_FILE) as f:
                d = json.load(f)
            self.positions = d.get("positions", {})
            self.pending_sells = d.get("pending_sells", [])
            self.pending_buys = d.get("pending_buys", [])
            self.regime = d.get("regime", "FULL_RISK")
            self.regime_data = d.get("regime_data", {})
            self.consecutive_losses = d.get("consecutive_losses", 0)
            self.scan_results = d.get("scan_results", [])
            self.offset = d.get("offset", 0)
            self.last_scan_date = d.get("last_scan_date", "")
            self.last_morning_date = d.get("last_morning_date", "")
            self.last_weekly_date = d.get("last_weekly_date", "")
            self.trade_history = d.get("trade_history", [])
            log("INFO", f"State loaded: {len(self.positions)} positions, regime={self.regime}")
        except FileNotFoundError:
            log("INFO", "No saved state — starting fresh")
        except Exception as e:
            log("ERROR", f"State load failed: {e}")

    def save(self):
        try:
            d = {
                "positions":          self.positions,
                "pending_sells":      self.pending_sells,
                "pending_buys":       self.pending_buys,
                "regime":             self.regime,
                "regime_data":        self.regime_data,
                "consecutive_losses": self.consecutive_losses,
                "scan_results":       self.scan_results,
                "offset":             self.offset,
                "last_scan_date":     self.last_scan_date,
                "last_morning_date":  self.last_morning_date,
                "last_weekly_date":   self.last_weekly_date,
                "trade_history":      self.trade_history[-100:],  # keep last 100
            }
            with open(STATUS_FILE, "w") as f:
                json.dump(d, f, indent=2, default=str)
        except Exception as e:
            log("ERROR", f"State save failed: {e}")

S = BotState()

def calc_position_size(equity, entry, stop, risk_pct):
    """Position size: (equity * risk%) / (entry - stop). Returns shares (int)."""
    risk_per_share = entry - stop
    if risk_per_share <= 0:
        return 0
    dollar_risk = equity * risk_pct
    shares = int(dollar_risk / risk_per_share)
    # Max position = 15% of equity
    max_shares = int((equity * 0.15) / entry) if entry > 0 else 0
    return min(shares, max_shares) if max_shares > 0 else shares

def current_exposure(equity):
    """Total exposure as fraction of equity across all positions."""
    if equity <= 0:
        return 0
    total = 0
    for sym, pos in S.positions.items():
        total += pos.get("shares", 0) * pos.get("entry", 0)
    return total / equity

def place_buy_stop(sym, qty, stop_price, limit_price, stop_loss_price):
    """Place a buy-stop order with attached stop-loss (OTO bracket)."""
    if DRY_RUN:
        log("INFO", f"  [DRY_RUN] Would place buy-stop: {sym} qty={qty} "
                     f"stop={stop_price:.2f} limit={limit_price:.2f} SL={stop_loss_price:.2f}")
        return {"id": f"dry_{sym}_{int(time.time())}", "status": "dry_run"}

    # Alpaca: buy stop-limit order
    order = {
        "symbol":        sym,
        "qty":           str(qty),
        "side":          "buy",
        "type":          "stop_limit",
        "time_in_force": "day",          # expires EOD if not triggered
        "stop_price":    str(round(stop_price, 2)),
        "limit_price":   str(round(limit_price, 2)),
        "order_class":   "oto",          # one-triggers-other
        "stop_loss":     {"stop_price": str(round(stop_loss_price, 2))},
    }
    resp = alp_post("/v2/orders", order)
    if resp and resp.get("id"):
        log("INFO", f"  Order placed: {sym} qty={qty} stop={stop_price:.2f} SL={stop_loss_price:.2f} id={resp['id']}")
    else:
        log("ERROR", f"  Order FAILED: {sym} — {resp}")
    return resp

def place_market_sell(sym, qty):
    """Market sell order."""
    if DRY_RUN:
        log("INFO", f"  [DRY_RUN] Would sell: {sym} qty={qty}")
        return {"id": f"dry_sell_{sym}", "status": "dry_run"}
    order = {
        "symbol":        sym,
        "qty":           str(qty),
        "side":          "sell",
        "type":          "market",
        "time_in_force": "day",
    }
    resp = alp_post("/v2/orders", order)
    if resp and resp.get("id"):
        log("INFO", f"  Sell placed: {sym} qty={qty} id={resp['id']}")
    else:
        log("ERROR", f"  Sell FAILED: {sym} — {resp}")
    return resp

def check_earnings_proximity(symbols):
    """Check which held symbols have earnings within N days. Returns {sym: date_str}."""
    if not FINNHUB_KEY or not symbols:
        return {}
    today = now_et().date()
    from_d = today.isoformat()
    to_d   = (today + timedelta(days=EARNINGS_WARN_DAYS)).isoformat()
    calendar = finnhub_earnings_calendar(from_d, to_d)
    warnings = {}
    sym_set = set(s.upper() for s in symbols)
    for entry in calendar:
        s = entry.get("symbol", "").upper()
        if s in sym_set:
            warnings[s] = entry.get("date", "unknown")
    return warnings

def get_trailing_ma_value(sym, adr_pct):
    """Fetch current trailing MA value (10-EMA for ADR>5%, 20-EMA otherwise)."""
    today = now_et().date()
    start = (today - timedelta(days=60)).isoformat()
    end   = today.isoformat()
    bars = poly_daily_bars(sym, start, end, limit=30)
    if not bars or len(bars) < 20:
        return None, "20-EMA"
    closes = [b["c"] for b in bars]
    if adr_pct > 0.05:
        ma = calc_ema(closes, TRAILING_MA_FAST)
        label = f"{TRAILING_MA_FAST}-EMA"
    else:
        ma = calc_ema(closes, TRAILING_MA_SLOW)
        label = f"{TRAILING_MA_SLOW}-EMA"
    return ma, label

def manage_positions():
    """
    Post-close position management. For each open position:
    1. Check daily close vs trailing MA -> queue sell if violated
    2. Check partial exit timing (3-5 days, sell 25%)
    3. Time stop (10 days no new high)
    4. Earnings proximity warning
    5. Update trailing MA and highest high
    """
    log("INFO", "=== POSITION MANAGEMENT ===")
    if not S.positions:
        log("INFO", "  No open positions")
        return

    today = now_et().date()
    today_str = today.isoformat()
    S.pending_sells = []

    # Check earnings proximity for all held symbols
    earnings_warnings = check_earnings_proximity(list(S.positions.keys()))

    for sym, pos in list(S.positions.items()):
        log("INFO", f"  Checking {sym}...")
        try:
            # Fetch today's daily bar
            bars = poly_daily_bars(sym, (today - timedelta(days=5)).isoformat(), today_str, limit=5)
            if not bars:
                log("WARN", f"    {sym}: no recent bars — skipping")
                continue
            today_bar = bars[-1]
            today_close = today_bar["c"]
            today_high  = today_bar["h"]

            # Update highest high
            if today_high > pos.get("highest_high", 0):
                pos["highest_high"] = today_high
                pos["days_since_new_high"] = 0
            else:
                pos["days_since_new_high"] = pos.get("days_since_new_high", 0) + 1

            # Calculate days held
            entry_date = pos.get("entry_date", today_str)
            try:
                days_held = (today - date.fromisoformat(entry_date)).days
            except Exception:
                days_held = 0
            pos["days_held"] = days_held

            # 1. Trailing MA check (daily CLOSE only — ignore intraday)
            adr = pos.get("adr_pct", 0.03)
            trail_ma, trail_label = get_trailing_ma_value(sym, adr)
            if trail_ma and today_close < trail_ma:
                log("INFO", f"    {sym}: CLOSE {today_close:.2f} < {trail_label} {trail_ma:.2f} -> QUEUE SELL")
                S.pending_sells.append({
                    "sym": sym, "reason": f"close below {trail_label}",
                    "shares": pos.get("shares", 0)
                })
                S.positions[sym] = pos
                continue
            if trail_ma:
                pos["trailing_ma"] = round(trail_ma, 2)
                pos["trailing_ma_label"] = trail_label

            # 2. Time stop: 10 days no new high (check before partial — full exit overrides)
            if pos.get("days_since_new_high", 0) >= TIME_STOP_DAYS:
                log("INFO", f"    {sym}: {pos['days_since_new_high']} days no new high -> TIME STOP")
                S.pending_sells.append({
                    "sym": sym, "reason": "time stop",
                    "shares": pos.get("shares", 0)
                })
                S.positions[sym] = pos
                continue

            # 3. Earnings proximity (full exit overrides partial — check before partial)
            if sym in earnings_warnings:
                earn_date = earnings_warnings[sym]
                unrealized_pct = (today_close - pos.get("entry", today_close)) / pos.get("entry", 1)
                if unrealized_pct < 0.10:
                    log("INFO", f"    {sym}: earnings {earn_date}, only {unrealized_pct*100:.1f}% gain -> SELL")
                    S.pending_sells.append({
                        "sym": sym, "reason": f"earnings {earn_date} (no cushion)",
                        "shares": pos.get("shares", 0)
                    })
                    S.positions[sym] = pos
                    continue
                else:
                    log("INFO", f"    {sym}: earnings {earn_date} but {unrealized_pct*100:.1f}% cushion -> holding")

            # 4. Partial exit: sell 25% after PARTIAL_EXIT_DAYS if not yet taken
            #    (only reached if no full-exit trigger above)
            if days_held >= PARTIAL_EXIT_DAYS and not pos.get("partial_taken"):
                partial_shares = max(1, int(pos.get("shares", 0) * PARTIAL_EXIT_PCT))
                log("INFO", f"    {sym}: {days_held} days held -> partial exit {partial_shares} shares")
                S.pending_sells.append({
                    "sym": sym, "reason": "partial exit",
                    "shares": partial_shares
                })
                pos["partial_taken"] = True
                # After partial: move stop to breakeven
                pos["stop"] = pos.get("entry", pos.get("stop", 0))
                log("INFO", f"    {sym}: stop moved to breakeven {pos['stop']:.2f}")

            S.positions[sym] = pos
        except Exception as e:
            log("ERROR", f"    {sym} position check error: {e}")

    if S.pending_sells:
        log("INFO", f"  Queued {len(S.pending_sells)} sells for morning execution")
    else:
        log("INFO", f"  All {len(S.positions)} positions healthy")

def place_new_orders(candidates, regime):
    """
    Place buy-stop orders for top candidates within position limits.
    """
    log("INFO", "=== ORDER PLACEMENT ===")
    if regime["regime"] == "NO_ENTRY":
        log("INFO", "  Regime=NO_ENTRY — no new orders")
        return

    equity = get_equity()
    if equity <= 0:
        log("ERROR", "  Cannot get account equity")
        return

    # How many slots available?
    current_pos = len(S.positions)
    max_pos = regime["max_positions"]
    risk = regime["risk_pct"]
    slots = max_pos - current_pos

    # Check exposure limit
    exposure = current_exposure(equity)
    if exposure >= MAX_EXPOSURE_PCT:
        log("INFO", f"  Exposure {exposure*100:.1f}% >= {MAX_EXPOSURE_PCT*100:.0f}% limit — no new orders")
        return

    # Drawdown scaling
    if S.consecutive_losses >= CONSEC_LOSS_LIMIT:
        risk = RISK_PCT_REDUCED
        log("INFO", f"  {S.consecutive_losses} consecutive losses — reduced risk to {risk*100:.2f}%")

    if slots <= 0:
        log("INFO", f"  {current_pos}/{max_pos} positions — no slots available")
        return

    log("INFO", f"  {slots} slots available | equity=${equity:.0f} | risk={risk*100:.2f}% | exposure={exposure*100:.1f}%")

    # Cancel any existing open buy orders (from previous night that didn't trigger)
    existing_orders = get_orders("open")
    for o in existing_orders:
        if o.get("side") == "buy" and o.get("type") in ("stop", "stop_limit"):
            oid = o.get("id", "")
            osym = o.get("symbol", "")
            log("INFO", f"  Canceling stale buy-stop: {osym} (id={oid})")
            cancel_order(oid)

    placed = 0
    S.pending_buys = []
    for cand in candidates:
        if placed >= slots:
            break
        sym = cand["sym"]
        if sym in S.positions:
            log("DEBUG", f"  {sym}: already holding — skip")
            continue

        entry = cand["consol_high"]
        stop  = cand["stop_price"]
        if entry <= 0 or stop <= 0 or stop >= entry:
            continue

        # Check exposure room
        shares = calc_position_size(equity, entry, stop, risk)
        if shares <= 0:
            log("DEBUG", f"  {sym}: position size = 0 — skip")
            continue
        position_value = shares * entry
        if (exposure + position_value / equity) > MAX_EXPOSURE_PCT:
            log("DEBUG", f"  {sym}: would exceed exposure limit — skip")
            continue

        # Place order: buy-stop at consolidation high
        # Limit = entry + 0.5% (allow some slip on breakout gap)
        limit_price = entry * 1.005
        resp = place_buy_stop(sym, shares, entry, limit_price, stop)
        if resp:
            S.pending_buys.append({
                "sym": sym,
                "entry": entry,
                "stop": stop,
                "shares": shares,
                "score": cand.get("consol_score", 0),
                "rs_3mo": cand.get("rs_3mo", 0),
                "order_id": resp.get("id", ""),
            })
            placed += 1
            exposure += position_value / equity

    log("INFO", f"  Placed {placed} buy-stop orders")

def morning_fill_check():
    """
    Morning routine (09:35 ET):
    1. Execute queued sells from last night
    2. Check which buy-stops triggered
    3. Volume confirmation on new fills (cancel if weak)
    4. Update position tracking
    """
    log("INFO", "=== MORNING FILL CHECK ===")
    today = now_et().date()
    today_str = today.isoformat()

    # 1. Execute queued sells
    for sell in S.pending_sells:
        sym = sell["sym"]
        shares = sell["shares"]
        reason = sell["reason"]
        log("INFO", f"  Selling {sym} x{shares}: {reason}")

        resp = place_market_sell(sym, shares)

        # Track P&L — try to get actual fill price from Alpaca order response
        pos = S.positions.get(sym, {})
        entry = pos.get("entry", 0)
        exit_price = entry  # fallback
        if resp and resp.get("id") and not resp.get("id", "").startswith("dry_"):
            # Wait briefly for fill, then check order status
            time.sleep(2)
            order_check = alp_get(f"/v2/orders/{resp['id']}")
            if order_check and order_check.get("filled_avg_price"):
                exit_price = float(order_check["filled_avg_price"])
            else:
                # Fallback: use last close from Polygon
                bars = poly_daily_bars(sym, (today - timedelta(days=3)).isoformat(), today_str, limit=3)
                exit_price = bars[-1]["c"] if bars else entry
        elif DRY_RUN:
            # In dry run, use last close as estimate
            bars = poly_daily_bars(sym, (today - timedelta(days=3)).isoformat(), today_str, limit=3)
            exit_price = bars[-1]["c"] if bars else entry
        pnl = (exit_price - entry) * shares if entry > 0 else 0

        # Log trade
        sheets_push({
            "type":        "trade_exit",
            "symbol":      sym,
            "entry":       entry,
            "exit":        exit_price,
            "shares":      shares,
            "pnl":         round(pnl, 2),
            "r_multiple":  round((exit_price - entry) / (entry - pos.get("stop", entry)), 2) if (entry - pos.get("stop", entry)) > 0 else 0,
            "hold_days":   pos.get("days_held", 0),
            "exit_reason": reason,
            "consol_score": pos.get("consol_score", 0),
        })

        # Record in trade history
        S.trade_history.append({
            "sym":         sym,
            "entry":       entry,
            "exit":        exit_price,
            "shares":      shares,
            "pnl":         round(pnl, 2),
            "r_multiple":  round((exit_price - entry) / (entry - pos.get("stop", entry)), 2) if (entry - pos.get("stop", entry)) > 0 else 0,
            "hold_days":   pos.get("days_held", 0),
            "exit_reason": reason,
            "date":        today_str,
        })

        # Update consecutive losses
        if reason == "partial exit":
            # Don't count partials as losses; reduce share count
            remaining = pos.get("shares", 0) - shares
            if remaining > 0:
                S.positions[sym]["shares"] = remaining
            else:
                del S.positions[sym]
        else:
            if pnl < 0:
                S.consecutive_losses += 1
                log("INFO", f"  Loss #{S.consecutive_losses}: {sym} ${pnl:.2f}")
            else:
                S.consecutive_losses = 0
            if sym in S.positions:
                del S.positions[sym]

    S.pending_sells = []

    # 2. Check buy-stop fills
    filled_orders = get_orders("closed")  # recently filled
    for buy in S.pending_buys:
        sym = buy["sym"]
        oid = buy.get("order_id", "")
        if not oid or oid.startswith("dry_"):
            if DRY_RUN:
                # In dry run, simulate fill
                log("INFO", f"  [DRY_RUN] Simulating fill: {sym}")
                S.positions[sym] = {
                    "entry":         buy["entry"],
                    "stop":          buy["stop"],
                    "consol_high":   buy["entry"],
                    "consol_low":    buy["stop"],
                    "shares":        buy["shares"],
                    "entry_date":    today_str,
                    "highest_high":  buy["entry"],
                    "partial_taken": False,
                    "adr_pct":       0.03,
                    "consol_score":  buy.get("score", 0),
                    "days_since_new_high": 0,
                }
            continue

        # Check if this specific order filled
        order_resp = alp_get(f"/v2/orders/{oid}")
        if not order_resp:
            continue
        status = order_resp.get("status", "")
        if status == "filled":
            fill_price = float(order_resp.get("filled_avg_price", buy["entry"]))
            fill_qty   = int(order_resp.get("filled_qty", buy["shares"]))
            log("INFO", f"  FILLED: {sym} x{fill_qty} @ {fill_price:.2f}")

            # Volume confirmation: check if breakout day volume >= 1.5x avg
            # At 09:35 ET, today's bar is incomplete (only 5 min of data).
            # The buy-stop triggers intraday, so we check yesterday's COMPLETED
            # daily bar if the order was placed yesterday, or defer volume check
            # to the next post-close cycle if filled today.
            yesterday = (today - timedelta(days=1)).isoformat()
            bars = poly_daily_bars(sym, (today - timedelta(days=25)).isoformat(), yesterday, limit=25)
            if bars and len(bars) >= 2:
                breakout_vol = bars[-1].get("v", 0)  # yesterday = the breakout day
                avg_vol = sum(b.get("v", 0) for b in bars[:-1]) / max(1, len(bars) - 1)
                if avg_vol > 0 and breakout_vol < avg_vol * VOLUME_CONFIRM:
                    log("WARN", f"    {sym}: weak breakout volume ({breakout_vol:.0f} < {avg_vol*VOLUME_CONFIRM:.0f}) — CLOSING")
                    close_position(sym)
                    sheets_push({
                        "type":   "trade_exit",
                        "symbol": sym,
                        "entry":  fill_price,
                        "exit":   fill_price,
                        "shares": fill_qty,
                        "pnl":    0,
                        "exit_reason": "weak breakout volume",
                    })
                    continue

            # Track new position
            adr = calc_adr_pct(bars[-20:]) if bars and len(bars) >= 20 else 0.03
            S.positions[sym] = {
                "entry":         fill_price,
                "stop":          buy["stop"],
                "consol_high":   buy["entry"],
                "consol_low":    buy["stop"],
                "shares":        fill_qty,
                "entry_date":    today_str,
                "highest_high":  fill_price,
                "partial_taken": False,
                "adr_pct":       adr,
                "consol_score":  buy.get("score", 0),
                "days_since_new_high": 0,
            }

            sheets_push({
                "type":        "trade_entry",
                "symbol":      sym,
                "entry":       fill_price,
                "stop":        buy["stop"],
                "shares":      fill_qty,
                "risk_pct":    round((fill_price - buy["stop"]) / fill_price * 100, 2),
                "consol_score": buy.get("score", 0),
                "rs_3mo":      buy.get("rs_3mo", 0),
                "regime":      S.regime,
            })

            tg_send(f"*KoiRyu FILL* {sym}\n"
                    f"Entry: ${fill_price:.2f} | Stop: ${buy['stop']:.2f}\n"
                    f"Shares: {fill_qty} | Score: {buy.get('score', 0)}/5")
        elif status in ("canceled", "expired", "rejected"):
            log("INFO", f"  Order {status}: {sym}")

    S.pending_buys = []


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP & SCHEDULING
# ══════════════════════════════════════════════════════════════════════════════

def post_close_cycle():
    """
    Full post-close routine (16:15 ET):
    1. Manage existing positions (trailing MA, partials, time stops, earnings)
    2. Check market regime
    3. Run nightly scanner
    4. Score consolidations
    5. Place new buy-stop orders
    6. Send Telegram summary
    """
    log("INFO", "")
    log("INFO", "╔══════════════════════════════════════════════════╗")
    log("INFO", "║         KOIRYU POST-CLOSE CYCLE                 ║")
    log("INFO", "╚══════════════════════════════════════════════════╝")

    # 1. Position management
    manage_positions()

    # 2. Regime check
    regime = check_regime()
    S.regime = regime["regime"]
    S.regime_data = regime

    # 3. Nightly scan (skip if regime = NO_ENTRY)
    if regime["regime"] == "NO_ENTRY":
        log("INFO", "Regime=NO_ENTRY — skipping scan")
        S.scan_results = []
    else:
        candidates = nightly_scan()
        # 4. Consolidation scoring
        if candidates:
            qualified = detect_consolidations(candidates, regime=regime)
            S.scan_results = qualified
        else:
            S.scan_results = []

    # 5. Place orders
    if S.scan_results:
        place_new_orders(S.scan_results, regime)

    # 6. Summary
    summary = build_summary(regime)
    tg_send(summary)

    # Structured portfolio push to Sheets (per-position unrealized P&L)
    today = now_et().date()
    equity = get_equity()
    portfolio_rows = []
    for sym, pos in S.positions.items():
        bars = poly_daily_bars(sym, (today - timedelta(days=3)).isoformat(),
                               today.isoformat(), limit=3)
        last_close = bars[-1]["c"] if bars else pos["entry"]
        unrealized = (last_close - pos["entry"]) * pos.get("shares", 0)
        portfolio_rows.append({
            "symbol":       sym,
            "shares":       pos.get("shares", 0),
            "entry":        pos["entry"],
            "last_close":   round(last_close, 2),
            "unrealized":   round(unrealized, 2),
            "unrealized_pct": round((last_close - pos["entry"]) / pos["entry"] * 100, 2) if pos["entry"] > 0 else 0,
            "stop":         pos["stop"],
            "trailing_ma":  pos.get("trailing_ma", "N/A"),
            "days_held":    pos.get("days_held", 0),
            "consol_score": pos.get("consol_score", 0),
        })
    sheets_push({
        "type":        "daily_summary",
        "date":        today.isoformat(),
        "regime":      regime["regime"],
        "equity":      equity,
        "positions":   len(S.positions),
        "candidates":  len(S.scan_results),
        "portfolio":   portfolio_rows,
        "total_unrealized": round(sum(r["unrealized"] for r in portfolio_rows), 2),
        "consecutive_losses": S.consecutive_losses,
    })

    S.last_scan_date = now_et().date().isoformat()
    S.save()
    log("INFO", "Post-close cycle complete")

def morning_cycle():
    """Morning routine (09:35 ET): fills, sells, updates."""
    log("INFO", "")
    log("INFO", "╔══════════════════════════════════════════════════╗")
    log("INFO", "║         KOIRYU MORNING CHECK                    ║")
    log("INFO", "╚══════════════════════════════════════════════════╝")

    morning_fill_check()
    S.last_morning_date = now_et().date().isoformat()
    S.save()

    # Quick status update
    pos_count = len(S.positions)
    if pos_count > 0:
        pos_lines = []
        for sym, pos in S.positions.items():
            pos_lines.append(f"  {sym}: entry=${pos['entry']:.2f} stop=${pos['stop']:.2f} "
                           f"d{pos.get('days_held', 0)}")
        tg_send(f"*KoiRyu Morning* {pos_count} positions\n" + "\n".join(pos_lines))
    log("INFO", "Morning check complete")

def build_summary(regime):
    """Build Telegram summary message."""
    lines = ["*KoiRyu Daily Summary*", ""]

    # Regime
    r = regime["regime"]
    emoji = {"AGGRESSIVE": "A", "FULL_RISK": "F", "REDUCED": "R", "NO_ENTRY": "X"}.get(r, "?")
    lines.append(f"Regime: [{emoji}] {r} (max {regime['max_positions']} pos)")
    lines.append(f"VIX: {regime.get('vix', '?')}")
    lines.append("")

    # Positions
    lines.append(f"Positions: {len(S.positions)}")
    for sym, pos in S.positions.items():
        trail = pos.get("trailing_ma", "?")
        lines.append(f"  {sym}: d{pos.get('days_held', 0)} entry=${pos['entry']:.2f} "
                    f"trail={trail}")
    lines.append("")

    # Pending sells
    if S.pending_sells:
        lines.append(f"Queued sells: {len(S.pending_sells)}")
        for s in S.pending_sells:
            lines.append(f"  {s['sym']}: {s['reason']}")
        lines.append("")

    # Scan results
    lines.append(f"Scan: {len(S.scan_results)} setups")
    for c in S.scan_results[:5]:
        lines.append(f"  {c['sym']}: score={c.get('consol_score', 0)}/5 "
                    f"break=${c.get('consol_high', 0):.2f} RS={c.get('rs_3mo', 0)}%")

    # Losses
    if S.consecutive_losses >= CONSEC_LOSS_LIMIT:
        lines.append(f"\n{S.consecutive_losses} consecutive losses — REDUCED RISK")

    return "\n".join(lines)

def weekly_digest():
    """Friday post-close weekly performance digest via Telegram + Sheets."""
    log("INFO", "=== WEEKLY DIGEST ===")
    today = now_et().date()
    week_ago = (today - timedelta(days=7)).isoformat()

    # Filter trade history to this week
    week_trades = [t for t in S.trade_history
                   if t.get("date", "") >= week_ago and t.get("exit_reason") != "partial exit"]

    if not week_trades:
        msg = "*KoiRyu Weekly Digest*\nNo completed trades this week."
        tg_send(msg)
        sheets_push({"type": "weekly_digest", "date": today.isoformat(),
                     "trades": 0, "note": "no trades"})
        return

    wins   = [t for t in week_trades if t["pnl"] > 0]
    losses = [t for t in week_trades if t["pnl"] <= 0]
    total_pnl = sum(t["pnl"] for t in week_trades)
    avg_r = sum(t["r_multiple"] for t in week_trades) / len(week_trades)
    avg_hold = sum(t["hold_days"] for t in week_trades) / len(week_trades)
    win_rate = len(wins) / len(week_trades) * 100

    lines = [
        "*KoiRyu Weekly Digest*",
        f"Period: {week_ago} to {today.isoformat()}",
        "",
        f"Trades: {len(week_trades)} | W: {len(wins)} | L: {len(losses)}",
        f"Win rate: {win_rate:.0f}%",
        f"Total P&L: ${total_pnl:.2f}",
        f"Avg R-multiple: {avg_r:.2f}R",
        f"Avg hold: {avg_hold:.1f} days",
        "",
    ]

    # Best and worst trade
    if week_trades:
        best  = max(week_trades, key=lambda t: t["pnl"])
        worst = min(week_trades, key=lambda t: t["pnl"])
        lines.append(f"Best:  {best['sym']} ${best['pnl']:.2f} ({best['r_multiple']:.1f}R)")
        lines.append(f"Worst: {worst['sym']} ${worst['pnl']:.2f} ({worst['r_multiple']:.1f}R)")

    # Current open positions
    lines.append(f"\nOpen positions: {len(S.positions)}")
    lines.append(f"Regime: {S.regime}")

    msg = "\n".join(lines)
    tg_send(msg)
    sheets_push({
        "type":       "weekly_digest",
        "date":       today.isoformat(),
        "trades":     len(week_trades),
        "wins":       len(wins),
        "losses":     len(losses),
        "win_rate":   round(win_rate, 1),
        "total_pnl":  round(total_pnl, 2),
        "avg_r":      round(avg_r, 2),
        "avg_hold":   round(avg_hold, 1),
    })
    S.last_weekly_date = today.isoformat()
    S.save()
    log("INFO", "Weekly digest sent")

def is_market_day():
    """Check if today is a market day (Mon-Fri, not a major holiday)."""
    now = now_et()
    if now.weekday() >= 5:  # Sat/Sun
        return False
    # Major US holidays — simplified check
    # Full holiday calendar would use Alpaca /v2/calendar
    return True

def handle_cmd(text, chat_id):
    """Handle Telegram commands."""
    if chat_id != TG_CHAT:
        return
    cmd = text.strip().lower()

    if cmd == "/status":
        equity = get_equity()
        msg = (f"*KoiRyu Status*\n"
               f"Regime: {S.regime}\n"
               f"Equity: ${equity:.0f}\n"
               f"Positions: {len(S.positions)}\n"
               f"Consecutive losses: {S.consecutive_losses}\n"
               f"DRY_RUN: {DRY_RUN}\n"
               f"Last scan: {S.last_scan_date}\n"
               f"Last morning: {S.last_morning_date}")
        for sym, pos in S.positions.items():
            msg += f"\n  {sym}: ${pos['entry']:.2f} d{pos.get('days_held', 0)}"
        tg_send(msg)

    elif cmd == "/scan":
        tg_send("Manual scan triggered...")
        candidates = nightly_scan()
        if candidates:
            qualified = detect_consolidations(candidates, regime=S.regime_data)
            S.scan_results = qualified
            msg = f"*Manual Scan*: {len(qualified)} setups\n"
            for c in qualified[:10]:
                msg += f"  {c['sym']}: score={c.get('consol_score', 0)}/5 break=${c.get('consol_high', 0):.2f}\n"
            tg_send(msg)
        else:
            tg_send("Scan: 0 candidates passed filters")
        S.save()

    elif cmd == "/regime":
        regime = check_regime()
        S.regime = regime["regime"]
        S.regime_data = regime
        msg = f"*Regime: {regime['regime']}*\n"
        for d in regime["details"]:
            msg += f"  {d}\n"
        tg_send(msg)
        S.save()

    elif cmd == "/positions":
        if not S.positions:
            tg_send("No open positions")
        else:
            msg = f"*{len(S.positions)} Positions*\n"
            for sym, pos in S.positions.items():
                msg += (f"\n{sym}: {pos['shares']} shares @ ${pos['entry']:.2f}\n"
                       f"  Stop: ${pos['stop']:.2f} | Trail: {pos.get('trailing_ma', 'N/A')}\n"
                       f"  Days: {pos.get('days_held', 0)} | High: ${pos.get('highest_high', 0):.2f}\n"
                       f"  Partial: {'Yes' if pos.get('partial_taken') else 'No'}\n")
            tg_send(msg)

    elif cmd == "/weekly":
        weekly_digest()

    elif cmd == "/help":
        tg_send("*KoiRyu Commands*\n"
                "/status — bot status\n"
                "/scan — manual nightly scan\n"
                "/regime — check market regime\n"
                "/positions — open positions detail\n"
                "/weekly — weekly performance digest\n"
                "/help — this message")

def startup():
    """Startup checks and announcement."""
    log("INFO", "╔══════════════════════════════════════════════════╗")
    log("INFO", "║  KoiRyu v1 — Qullamaggie Swing Breakout Bot     ║")
    log("INFO", "╚══════════════════════════════════════════════════╝")
    log("INFO", f"  DRY_RUN:      {DRY_RUN}")
    log("INFO", f"  RISK_PCT:     {RISK_PCT*100:.2f}%")
    log("INFO", f"  MAX_POSITIONS: {MAX_POSITIONS}")
    log("INFO", f"  Polygon key:  {'set' if POLYGON_KEY else 'MISSING'}")
    log("INFO", f"  Alpaca key:   {'set' if ALPACA_KEY else 'MISSING'}")
    log("INFO", f"  Telegram:     {'set' if TG_TOKEN else 'MISSING'}")
    log("INFO", f"  Sheets:       {'set' if SHEETS_URL else 'MISSING'}")
    log("INFO", f"  Finnhub:      {'set' if FINNHUB_KEY else 'MISSING'}")
    log("INFO", f"  FMP:          {'stubbed' if not FMP_KEY else 'set'}")
    log("INFO", f"  Positions:    {len(S.positions)}")
    log("INFO", f"  Regime:       {S.regime}")

    equity = get_equity()
    log("INFO", f"  Equity:       ${equity:.0f}" if equity > 0 else "  Equity:       (cannot reach Alpaca)")

    tg_send(f"*KoiRyu v1 started*\n"
            f"DRY_RUN={DRY_RUN} | Positions: {len(S.positions)} | Regime: {S.regime}")

# ── Dashboard HTTP server ─────────────────────────────────────────────────────
DASHBOARD_PORT = int(os.environ.get("PORT") or os.environ.get("DASHBOARD_PORT", "8080"))

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def _json(self, obj, status=200):
        body = json.dumps(obj, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        if path in ("/", ""):
            self._json({"status": "ok"})
            return
        if path == "/health":
            self._json({"status": "ok", "time": now_et().isoformat(), "bot": "KoiRyu v1"})
            return
        if path == "/api/dashboard":
            if DASH_TOKEN:
                provided = self.path.split("token=")[-1].split("&")[0] if "token=" in self.path else ""
                if provided != DASH_TOKEN:
                    self._json({"error": "unauthorized"}, status=401)
                    return
            self._json({
                "bot":         "KoiRyu v1",
                "regime":      S.regime,
                "positions":   S.positions,
                "pending_sells": S.pending_sells,
                "pending_buys":  S.pending_buys,
                "scan_results":  len(S.scan_results),
                "consecutive_losses": S.consecutive_losses,
                "last_scan":   S.last_scan_date,
                "last_morning": S.last_morning_date,
                "dry_run":     DRY_RUN,
            })
            return
        self._json({"error": "not found"}, status=404)

def start_dashboard_server():
    try:
        class ReusableTCPServer(HTTPServer):
            allow_reuse_address = True
        server = ReusableTCPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
        log("INFO", f"Dashboard listening on port {DASHBOARD_PORT}")
        server.serve_forever()
    except Exception as e:
        log("ERROR", f"Dashboard server failed: {e}")

def _shutdown(sig, frame):
    log("INFO", f"Shutdown signal {sig}")
    tg_send("KoiRyu shutting down")
    S.save()
    sys.exit(0)

def main():
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # Dashboard in background
    t_dash = threading.Thread(target=start_dashboard_server, daemon=True)
    t_dash.start()
    time.sleep(0.5)

    startup()

    # Main loop: check time, run appropriate cycle, then sleep
    # Also listens for Telegram commands between cycles
    log("INFO", "Main loop active — schedule: 16:15 ET post-close | 09:35 ET morning check")
    while True:
        try:
            now = now_et()
            today_str = now.date().isoformat()
            h, m = now.hour, now.minute

            # Post-close cycle: 16:15 ET (run once per day)
            if (is_market_day() and h == POST_CLOSE_HOUR and m >= POST_CLOSE_MIN
                    and S.last_scan_date != today_str):
                post_close_cycle()

            # Morning cycle: 09:35 ET (run once per day)
            if (is_market_day() and h == MORNING_HOUR and m >= MORNING_MIN
                    and m < MORNING_MIN + 10  # 10-min window
                    and S.last_morning_date != today_str):
                morning_cycle()

            # Weekly digest: Friday at 16:30 ET
            if (is_market_day() and now.weekday() == 4  # Friday
                    and h == 16 and m >= 30 and m < 40
                    and S.last_weekly_date != today_str):
                weekly_digest()

            # Telegram command polling
            updates = tg_poll(S.offset, CYCLE_SLEEP_SEC)
            for u in updates:
                msg = u.get("message", {})
                txt = msg.get("text", "")
                cid = str(msg.get("chat", {}).get("id", ""))
                if txt:
                    handle_cmd(txt, cid)
                S.offset = u["update_id"] + 1

        except KeyboardInterrupt:
            log("INFO", "Keyboard interrupt")
            tg_send("KoiRyu stopped")
            break
        except Exception as e:
            log("ERROR", f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
