import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd
import websockets

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG  — edit this section only
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Config:
    # Deriv credentials
    api_token: str = "eHDQAIUyPXvtgLL"
    app_id:    str = "1089"

    # Symbol
    symbol: str = "R_75"

    # Timeframes (seconds)
    ltf_granularity: int = 1800    # M30 — entry timeframe
    htf_granularity: int = 14400   # H4  — bias timeframe

    # Candle counts
    ltf_count: int = 150
    htf_count: int = 100

    # Signal filters
    rr_ratio:          float = 2.0    # Risk:Reward multiplier
    min_fvg_pct:       float = 0.05   # Min FVG size as % of price (filters micro gaps)
    swing_lookback:    int   = 10     # Bars to look back for swing highs/lows
    session_filter:    bool  = False  # Set True to restrict to London/NY hours
    require_mss:       bool  = True   # Require Market Structure Shift confirmation

    @property
    def uri(self) -> str:
        return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"


CFG = Config()


# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET — fetch candles for any granularity
# ══════════════════════════════════════════════════════════════════════════════
async def fetch_candles(ws, symbol: str, granularity: int, count: int) -> pd.DataFrame | None:
    request = {
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "style": "candles",
        "granularity": granularity,
    }
    await ws.send(json.dumps(request))
    response = json.loads(await ws.recv())

    if "error" in response:
        log.error("Candle fetch error (%ds): %s", granularity, response["error"]["message"])
        return None

    candles = response.get("candles", [])
    if not candles:
        return None

    df = pd.DataFrame(candles)
    df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}, inplace=True)
    df[["Open", "High", "Low", "Close"]] = df[["Open", "High", "Low", "Close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    df.drop(columns=["epoch"], inplace=True)
    return df


async def fetch_all_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Open a single WebSocket session and fetch both HTF and LTF candles."""
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            # Authorize
            await ws.send(json.dumps({"authorize": CFG.api_token}))
            auth = json.loads(await ws.recv())
            if "error" in auth:
                log.error("Auth failed: %s", auth["error"]["message"])
                return None, None
            log.info("Authorized — account: %s", auth.get("authorize", {}).get("loginid", "?"))

            # Fetch HTF (H4) for bias
            log.info("Fetching H4 candles for trend bias...")
            htf_df = await fetch_candles(ws, CFG.symbol, CFG.htf_granularity, CFG.htf_count)

            # Fetch LTF (M30) for entries
            log.info("Fetching M30 candles for entry analysis...")
            ltf_df = await fetch_candles(ws, CFG.symbol, CFG.ltf_granularity, CFG.ltf_count)

            return htf_df, ltf_df

    except websockets.exceptions.WebSocketException as e:
        log.error("WebSocket error: %s", e)
        return None, None
    except asyncio.TimeoutError:
        log.error("Connection timed out.")
        return None, None


# ══════════════════════════════════════════════════════════════════════════════
#  INDICATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"]  - df["Close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def swing_highs(df: pd.DataFrame, lookback: int) -> pd.Series:
    """True where a candle is the highest high in ±lookback bars."""
    return df["High"] == df["High"].rolling(lookback * 2 + 1, center=True).max()


def swing_lows(df: pd.DataFrame, lookback: int) -> pd.Series:
    """True where a candle is the lowest low in ±lookback bars."""
    return df["Low"] == df["Low"].rolling(lookback * 2 + 1, center=True).min()


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYSIS LAYERS
# ══════════════════════════════════════════════════════════════════════════════

# ── Layer 1: HTF Bias ──────────────────────────────────────────────────────────
def get_htf_bias(htf_df: pd.DataFrame) -> str:
    """
    Determine higher-timeframe trend direction using EMA 50/200 crossover
    and slope confirmation.
    Returns 'BULLISH', 'BEARISH', or 'NEUTRAL'.
    """
    df = htf_df.copy()
    df["EMA50"]  = ema(df["Close"], 50)
    df["EMA200"] = ema(df["Close"], 200)

    last = df.iloc[-1]
    prev = df.iloc[-3]  # 3-bar slope check

    ema50_sloping_up   = last["EMA50"]  > prev["EMA50"]
    ema200_sloping_up  = last["EMA200"] > prev["EMA200"]
    price_above_ema50  = last["Close"]  > last["EMA50"]
    price_above_ema200 = last["Close"]  > last["EMA200"]
    emas_bullish_cross = last["EMA50"]  > last["EMA200"]

    bull_score = sum([ema50_sloping_up, ema200_sloping_up, price_above_ema50,
                      price_above_ema200, emas_bullish_cross])

    if bull_score >= 4:
        return "BULLISH"
    elif bull_score <= 1:
        return "BEARISH"
    else:
        return "NEUTRAL"


# ── Layer 2: Liquidity Sweep Detection ────────────────────────────────────────
def detect_liquidity_sweeps(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """
    A liquidity sweep occurs when price wicks beyond a recent swing high/low
    but CLOSES back inside — smart money grabbing stops before reversing.

    Bullish sweep: wick below swing low, close above it  → expect BUY
    Bearish sweep: wick above swing high, close below it → expect SELL
    """
    df = df.copy()
    df["SwingHigh"] = swing_highs(df, lookback)
    df["SwingLow"]  = swing_lows(df, lookback)

    # Rolling recent swing levels (last N bars)
    df["RecentSwingHigh"] = df["High"].where(df["SwingHigh"]).ffill()
    df["RecentSwingLow"]  = df["Low"].where(df["SwingLow"]).ffill()

    # Bullish sweep: low dips below prior swing low but close recovers above it
    df["BullishSweep"] = (
        (df["Low"]   < df["RecentSwingLow"].shift(1)) &
        (df["Close"] > df["RecentSwingLow"].shift(1))
    )

    # Bearish sweep: high pokes above prior swing high but close falls back below
    df["BearishSweep"] = (
        (df["High"]  > df["RecentSwingHigh"].shift(1)) &
        (df["Close"] < df["RecentSwingHigh"].shift(1))
    )

    return df


# ── Layer 3: Fair Value Gap (FVG) ─────────────────────────────────────────────
def detect_fvg(df: pd.DataFrame, min_fvg_pct: float) -> pd.DataFrame:
    """
    Valid FVG = gap between candle[i-2] and candle[i] with NO overlap on candle[i-1].
    Size filter removes micro gaps that are just noise.
    """
    df = df.copy()

    # Bullish FVG: candle[i-2] high < candle[i] low
    bull_gap = df["Low"] - df["High"].shift(2)
    df["Bullish_FVG"]      = (bull_gap > 0) & (bull_gap / df["Close"] > min_fvg_pct / 100)
    df["Bullish_FVG_Size"] = bull_gap.clip(lower=0)

    # Bearish FVG: candle[i-2] low > candle[i] high
    bear_gap = df["Low"].shift(2) - df["High"]
    df["Bearish_FVG"]      = (bear_gap > 0) & (bear_gap / df["Close"] > min_fvg_pct / 100)
    df["Bearish_FVG_Size"] = bear_gap.clip(lower=0)

    return df


# ── Layer 4: Market Structure Shift (MSS) ─────────────────────────────────────
def detect_mss(df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """
    Bullish MSS: in a recent downtrend, price breaks above the last significant
                 lower high — structure shifts bullish.
    Bearish MSS: in a recent uptrend, price breaks below the last significant
                 higher low — structure shifts bearish.
    """
    df = df.copy()

    # Rolling max/min over lookback window (excluding current bar)
    roll_high = df["High"].shift(1).rolling(lookback).max()
    roll_low  = df["Low"].shift(1).rolling(lookback).min()

    df["Bullish_MSS"] = df["Close"] > roll_high   # break of structure up
    df["Bearish_MSS"] = df["Close"] < roll_low    # break of structure down

    return df


# ── Layer 5: Session Filter (UTC) ─────────────────────────────────────────────
def is_kill_zone(timestamp) -> bool:
    """
    London Kill Zone:    02:00 – 05:00 UTC
    New York Kill Zone:  12:00 – 15:00 UTC
    """
    hour = timestamp.hour if hasattr(timestamp, "hour") else pd.Timestamp(timestamp).hour
    return (2 <= hour < 5) or (12 <= hour < 15)


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL COMBINER
# ══════════════════════════════════════════════════════════════════════════════
def generate_confluence_signals(
    ltf_df: pd.DataFrame,
    htf_bias: str,
) -> pd.DataFrame:
    """
    Combine all layers into a high-confluence signal.
    A BUY fires only when:
      - HTF bias is BULLISH
      - Bullish liquidity sweep occurred within last 3 bars
      - Bullish FVG is present
      - MSS confirms bullish break (if enabled)
      - Current bar is in a kill zone (if session filter enabled)
    """
    df = ltf_df.copy()

    # Apply analysis layers
    df = detect_liquidity_sweeps(df, CFG.swing_lookback)
    df = detect_fvg(df, CFG.min_fvg_pct)
    if CFG.require_mss:
        df = detect_mss(df, CFG.swing_lookback)
    else:
        df["Bullish_MSS"] = True
        df["Bearish_MSS"] = True

    # Sweep within last 3 bars (sweep happens, then FVG forms on pullback)
    recent_bull_sweep = df["BullishSweep"].rolling(3).max().astype(bool)
    recent_bear_sweep = df["BearishSweep"].rolling(3).max().astype(bool)

    # Session filter
    if CFG.session_filter:
        in_session = df.index.map(is_kill_zone)
    else:
        in_session = pd.Series(True, index=df.index)

    # BUY confluence
    buy_signal = (
        (htf_bias == "BULLISH") &
        recent_bull_sweep &
        df["Bullish_FVG"] &
        df["Bullish_MSS"] &
        in_session
    )

    # SELL confluence
    sell_signal = (
        (htf_bias == "BEARISH") &
        recent_bear_sweep &
        df["Bearish_FVG"] &
        df["Bearish_MSS"] &
        in_session
    )

    df["Signal"] = "HOLD"
    df.loc[buy_signal,  "Signal"] = "BUY"
    df.loc[sell_signal, "Signal"] = "SELL"

    # Confluence score (0–4) for signal quality rating
    df["Confluence"] = 0
    df.loc[buy_signal  | sell_signal, "Confluence"] += 1
    df.loc[(buy_signal  & df["Bullish_MSS"]) | (sell_signal & df["Bearish_MSS"]), "Confluence"] += 1
    df.loc[(buy_signal  & recent_bull_sweep) | (sell_signal & recent_bear_sweep), "Confluence"] += 1
    df.loc[in_session,  "Confluence"] += 1

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  TRADE PLAN BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_plans(signals: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized trade plan with:
    - Entry  = Close of signal candle
    - SL     = Below swing low (BUY) / Above swing high (SELL) + ATR buffer
    - TP     = Entry ± (risk × RR ratio)
    - Rating = signal quality label
    """
    s = signals.copy()
    atr_series = atr(s, 14)

    buy_mask  = s["Signal"] == "BUY"
    sell_mask = s["Signal"] == "SELL"

    s["Entry"] = s["Close"].round(4)
    s["SL"]    = 0.0
    s["TP"]    = 0.0
    s["Risk"]  = 0.0

    # BUY plan: SL = recent swing low minus half ATR buffer
    s.loc[buy_mask, "SL"] = (
        s.loc[buy_mask, "RecentSwingLow"] - atr_series[buy_mask] * 0.5
    ).round(4)
    s.loc[buy_mask, "Risk"] = (s.loc[buy_mask, "Entry"] - s.loc[buy_mask, "SL"]).round(4)
    s.loc[buy_mask, "TP"]   = (
        s.loc[buy_mask, "Entry"] + s.loc[buy_mask, "Risk"] * CFG.rr_ratio
    ).round(4)

    # SELL plan: SL = recent swing high plus half ATR buffer
    s.loc[sell_mask, "SL"] = (
        s.loc[sell_mask, "RecentSwingHigh"] + atr_series[sell_mask] * 0.5
    ).round(4)
    s.loc[sell_mask, "Risk"] = (s.loc[sell_mask, "SL"] - s.loc[sell_mask, "Entry"]).round(4)
    s.loc[sell_mask, "TP"]   = (
        s.loc[sell_mask, "Entry"] - s.loc[sell_mask, "Risk"] * CFG.rr_ratio
    ).round(4)

    # Signal quality label
    def rate(score):
        if score >= 4: return "⭐⭐⭐ STRONG"
        if score >= 3: return "⭐⭐   GOOD"
        if score >= 2: return "⭐     WEAK"
        return              "✗     SKIP"

    s["Rating"] = s["Confluence"].apply(rate)

    return s


# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY
# ══════════════════════════════════════════════════════════════════════════════
def print_report(result: pd.DataFrame, htf_bias: str):
    active = result[result["Signal"] != "HOLD"].copy()

    print("\n" + "═" * 65)
    print("   SMC MULTI-CONFLUENCE SCANNER  ─  " + CFG.symbol)
    print("═" * 65)
    print(f"   HTF Bias (H4)  : {htf_bias}")
    print(f"   Scanned        : {len(result)} M30 candles")
    print(f"   Filters        : FVG + Liquidity Sweep + MSS + HTF Bias")
    print(f"   Session Filter : {'ON (London/NY)' if CFG.session_filter else 'OFF'}")
    print(f"   Min FVG Size   : {CFG.min_fvg_pct}%  |  RR: 1:{int(CFG.rr_ratio)}")
    print("─" * 65)

    if active.empty:
        print("\n  No high-confluence setups detected on current data.")
        print("  All filters must align simultaneously — this is intentional.")
        print("  Check back at London open (02:00 UTC) or NY open (12:00 UTC).\n")
    else:
        cols = ["Signal", "Entry", "SL", "TP", "Risk", "Rating"]
        latest = active[cols].tail(5)
        print(latest.to_string())
        print()

        # Detailed breakdown of latest signal
        last = active.iloc[-1]
        print("─" * 65)
        print(f"  LATEST SIGNAL BREAKDOWN")
        print(f"  Time    : {last.name.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Signal  : {last['Signal']}")
        print(f"  Entry   : {last['Entry']}")
        print(f"  Stop Loss    : {last['SL']}  (swing-based + ATR buffer)")
        print(f"  Take Profit  : {last['TP']}  (1:{int(CFG.rr_ratio)} RR)")
        print(f"  Risk/pt  : {last['Risk']}")
        print(f"  Rating   : {last['Rating']}")
        print(f"  HTF Bias : {htf_bias}")
        print(f"  Confluence confirmed: FVG ✓  Sweep ✓  MSS ✓  HTF ✓")

    buys  = (active["Signal"] == "BUY").sum()
    sells = (active["Signal"] == "SELL").sum()
    print("─" * 65)
    print(f"  Total  →  BUY: {buys}  |  SELL: {sells}  |  Skipped (HOLD): {len(result) - len(active)}")
    print("═" * 65 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    print("\n╔══════════════════════════════════════════════════════════╗")
    print("║   Advanced SMC Scanner  —  Starting up...               ║")
    print("╚══════════════════════════════════════════════════════════╝\n")

    htf_df, ltf_df = await fetch_all_data()

    if htf_df is None or ltf_df is None:
        log.error("Failed to fetch market data. Check credentials and connection.")
        return

    # Layer 1 — HTF bias from H4
    htf_bias = get_htf_bias(htf_df)
    log.info("H4 Trend Bias: %s", htf_bias)

    if htf_bias == "NEUTRAL":
        log.warning("HTF bias is NEUTRAL — no directional edge. Skipping signal generation.")
        print("\n  Market is in consolidation on H4. No trades recommended.\n")
        return

    # Layers 2–5 — full confluence analysis
    analyzed = generate_confluence_signals(ltf_df, htf_bias)

    # Build trade plans for active signals
    result = build_trade_plans(analyzed)

    # Print report
    print_report(result, htf_bias)


if __name__ == "__main__":
    asyncio.run(main())
