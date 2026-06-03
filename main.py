

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import websockets

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Config:
    # ── Credentials ───────────────────────────────────────────────────────────
    api_token: str = "eHDQAIUyPXvtgLL"
    app_id:    str = "1089"
    symbol:    str = "R_75"

    # ── Timeframes (seconds) ──────────────────────────────────────────────────
    h1_tf:  int = 3600  # H1  — Structure + Order Block
    m15_tf: int = 900   # M15 — Bias + Sweep + MSS
    m5_tf:  int = 300   # M5  — FVG entry inside OB

    # ── Candle counts ─────────────────────────────────────────────────────────
    h1_count:  int = 100
    m15_count: int = 120
    m5_count:  int = 150

    # ── Risk / Reward ─────────────────────────────────────────────────────────
    rr_ratio:   float = 2.0  # Full TP multiplier
    partial_tp: float = 0.5  # 50% closed at TP1 (1:1)

    # ── Order Block settings ──────────────────────────────────────────────────
    ob_lookback:       int   = 30   # H1 bars to search for OBs
    ob_min_body_ratio: float = 0.5  # OB candle must have strong body
    ob_proximity_pct:  float = 0.3  # M5 price within 0.3% of OB zone

    # ── Signal filters ────────────────────────────────────────────────────────
    swing_lookback: int   = 5
    min_fvg_pct:    float = 0.03   # M5 FVG minimum size %
    rsi_period:     int   = 7
    rsi_overbought: float = 70.0
    rsi_oversold:   float = 30.0
    body_ratio_min: float = 0.45
    cooldown_bars:  int   = 4      # M5 bars between same-direction signals
    session_filter: bool  = True
    require_mss:    bool  = True

    # ── Live mode ─────────────────────────────────────────────────────────────
    live_mode:       bool = True
    scan_interval_s: int  = 300    # Re-scan every M5 close (5 min)

    @property
    def uri(self) -> str:
        return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"


CFG = Config()


# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET — fetch candles
# ══════════════════════════════════════════════════════════════════════════════
async def fetch_candles(
    ws, granularity: int, count: int
) -> Optional[pd.DataFrame]:
    await ws.send(json.dumps({
        "ticks_history":     CFG.symbol,
        "adjust_start_time": 1,
        "count":             count,
        "end":               "latest",
        "style":             "candles",
        "granularity":       granularity,
    }))
    resp = json.loads(await ws.recv())

    if "error" in resp:
        log.error("Fetch error (%ds): %s", granularity, resp["error"]["message"])
        return None

    candles = resp.get("candles", [])
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


async def fetch_all() -> tuple[
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
]:
    """Single WebSocket session — fetch H1, M15, M5."""
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            await ws.send(json.dumps({"authorize": CFG.api_token}))
            auth = json.loads(await ws.recv())
            if "error" in auth:
                log.error("Auth failed: %s", auth["error"]["message"])
                return None, None, None
            log.info("Authorized — %s", auth.get("authorize", {}).get("loginid", "?"))

            log.info("Fetching H1  (structure + OB)...")
            h1  = await fetch_candles(ws, CFG.h1_tf,  CFG.h1_count)

            log.info("Fetching M15 (bias + sweep + MSS)...")
            m15 = await fetch_candles(ws, CFG.m15_tf, CFG.m15_count)

            log.info("Fetching M5  (FVG entry)...")
            m5  = await fetch_candles(ws, CFG.m5_tf,  CFG.m5_count)

            return h1, m15, m5

    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("Connection error: %s", e)
        return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
#  INDICATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def rsi(series: pd.Series, n: int) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=n, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=n, adjust=False).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def atr(df: pd.DataFrame, n: int = 7) -> pd.Series:
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"]  - df["Close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


def body_ratio(df: pd.DataFrame) -> pd.Series:
    rng = (df["High"] - df["Low"]).replace(0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng


def swing_highs(df: pd.DataFrame, n: int) -> pd.Series:
    return df["High"] == df["High"].rolling(n * 2 + 1, center=True).max()


def swing_lows(df: pd.DataFrame, n: int) -> pd.Series:
    return df["Low"] == df["Low"].rolling(n * 2 + 1, center=True).min()


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — H1: STRUCTURE BIAS + ORDER BLOCK DETECTION
# ══════════════════════════════════════════════════════════════════════════════
def get_h1_structure(h1: pd.DataFrame) -> str:
    """
    H1 trend bias via EMA 21/50.
    Both EMAs must slope in the same direction for a clean directional read.
    """
    df = h1.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)

    last = df.iloc[-1]
    prev = df.iloc[-3]

    bull = (
        last["E21"] > last["E50"]   and
        last["Close"] > last["E21"] and
        last["E21"]  > prev["E21"] and  # EMA21 rising
        last["E50"]  > prev["E50"]      # EMA50 rising
    )
    bear = (
        last["E21"] < last["E50"]   and
        last["Close"] < last["E21"] and
        last["E21"]  < prev["E21"] and
        last["E50"]  < prev["E50"]
    )

    if bull: return "BULLISH"
    if bear: return "BEARISH"
    return "NEUTRAL"


def detect_order_blocks(h1: pd.DataFrame, bias: str) -> dict:
    """
    Order Block identification on H1:

    BULLISH OB (entry zone for BUY):
      Last strong BEARISH candle immediately before a bullish impulse
      that caused a Market Structure Shift upward. The OB zone is that
      candle's body range [Open, Close]. Must be unmitigated — price has
      not closed below the OB low since it formed.

    BEARISH OB (entry zone for SELL):
      Last strong BULLISH candle before a bearish MSS. Same logic inverted.

    SL anchor: OB wick extreme (not body) gives extra buffer room.
    """
    df = h1.copy()
    df["BodyRatio"] = body_ratio(df)
    df["IsBull"]    = df["Close"] > df["Open"]
    df["IsBear"]    = df["Close"] < df["Open"]

    roll_n = 5
    df["RollHigh"] = df["High"].shift(1).rolling(roll_n).max()
    df["RollLow"]  = df["Low"].shift(1).rolling(roll_n).min()
    df["BullMSS"]  = df["Close"] > df["RollHigh"]
    df["BearMSS"]  = df["Close"] < df["RollLow"]

    lookback = df.iloc[-CFG.ob_lookback:]
    result   = {"bullish_ob": None, "bearish_ob": None}

    # ── Bullish OB — last red candle before bullish MSS ───────────────────────
    if bias == "BULLISH":
        for mss_idx in reversed(lookback[lookback["BullMSS"]].index.tolist()):
            before    = lookback.loc[:mss_idx].iloc[:-1]
            candidates= before[
                before["IsBear"] &
                (before["BodyRatio"] >= CFG.ob_min_body_ratio)
            ]
            if candidates.empty:
                continue
            ob_bar  = candidates.iloc[-1]
            ob_high = max(ob_bar["Open"], ob_bar["Close"])
            ob_low  = min(ob_bar["Open"], ob_bar["Close"])
            # Unmitigated: no close below OB low since formation
            if df.loc[ob_bar.name:]["Low"].min() < ob_low:
                continue
            result["bullish_ob"] = {
                "time":     ob_bar.name,
                "ob_high":  round(ob_high, 4),
                "ob_low":   round(ob_low,  4),
                "wick_low": round(ob_bar["Low"], 4),
            }
            break

    # ── Bearish OB — last green candle before bearish MSS ────────────────────
    if bias == "BEARISH":
        for mss_idx in reversed(lookback[lookback["BearMSS"]].index.tolist()):
            before    = lookback.loc[:mss_idx].iloc[:-1]
            candidates= before[
                before["IsBull"] &
                (before["BodyRatio"] >= CFG.ob_min_body_ratio)
            ]
            if candidates.empty:
                continue
            ob_bar  = candidates.iloc[-1]
            ob_high = max(ob_bar["Open"], ob_bar["Close"])
            ob_low  = min(ob_bar["Open"], ob_bar["Close"])
            if df.loc[ob_bar.name:]["High"].max() > ob_high:
                continue
            result["bearish_ob"] = {
                "time":      ob_bar.name,
                "ob_high":   round(ob_high, 4),
                "ob_low":    round(ob_low,  4),
                "wick_high": round(ob_bar["High"], 4),
            }
            break

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — M15: BIAS, SWEEP, MSS
# ══════════════════════════════════════════════════════════════════════════════
def get_m15_bias(m15: pd.DataFrame) -> str:
    """M15 EMA 8/21 — must align with H1 for signal to pass."""
    df   = m15.copy()
    df["E8"]  = ema(df["Close"], 8)
    df["E21"] = ema(df["Close"], 21)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    if last["E8"] > last["E21"] and last["Close"] > last["E8"] and last["E8"] > prev["E8"]:
        return "BULLISH"
    if last["E8"] < last["E21"] and last["Close"] < last["E8"] and last["E8"] < prev["E8"]:
        return "BEARISH"
    return "NEUTRAL"


def analyze_m15(m15: pd.DataFrame) -> dict:
    """
    Scan M15 for:
    - Liquidity sweep (wick beyond swing hi/lo, close back inside)
    - MSS (close beyond rolling structure high/low)
    Returns scalar flags and last swing levels for context.
    """
    df = m15.copy()
    n  = CFG.swing_lookback

    sh = swing_highs(df, n)
    sl = swing_lows(df,  n)
    df["RSwingHigh"] = df["High"].where(sh).ffill()
    df["RSwingLow"]  = df["Low"].where(sl).ffill()

    df["BullSweep"] = (
        (df["Low"]   < df["RSwingLow"].shift(1)) &
        (df["Close"] > df["RSwingLow"].shift(1))
    )
    df["BearSweep"] = (
        (df["High"]  > df["RSwingHigh"].shift(1)) &
        (df["Close"] < df["RSwingHigh"].shift(1))
    )

    roll_high = df["High"].shift(1).rolling(n).max()
    roll_low  = df["Low"].shift(1).rolling(n).min()
    df["BullMSS"] = df["Close"] > roll_high
    df["BearMSS"] = df["Close"] < roll_low

    return {
        "bull_sweep":      bool(df["BullSweep"].tail(5).any()),
        "bear_sweep":      bool(df["BearSweep"].tail(5).any()),
        "bull_mss":        bool(df["BullMSS"].tail(3).any()),
        "bear_mss":        bool(df["BearMSS"].tail(3).any()),
        "last_swing_low":  float(df["RSwingLow"].iloc[-1]),
        "last_swing_high": float(df["RSwingHigh"].iloc[-1]),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 3 — M5: FVG INSIDE H1 OB
# ══════════════════════════════════════════════════════════════════════════════
def price_in_ob_zone(price: float, ob: dict) -> bool:
    """True if price is within or very near the OB body range."""
    tol = price * (CFG.ob_proximity_pct / 100)
    return (ob["ob_low"] - tol) <= price <= (ob["ob_high"] + tol)


def prepare_m5(m5: pd.DataFrame) -> pd.DataFrame:
    """Add FVG detection, RSI, body ratio and ATR to M5 dataframe."""
    df = m5.copy()

    # FVG
    bull_gap = df["Low"] - df["High"].shift(2)
    bear_gap = df["Low"].shift(2) - df["High"]
    min_size = CFG.min_fvg_pct / 100

    df["Bullish_FVG"] = (bull_gap > 0) & (bull_gap / df["Close"] > min_size)
    df["Bearish_FVG"] = (bear_gap > 0) & (bear_gap / df["Close"] > min_size)

    # Indicators
    df["RSI"]       = rsi(df["Close"], CFG.rsi_period).round(2)
    df["BodyRatio"] = body_ratio(df).round(3)
    df["ATR"]       = atr(df, 7)

    return df


def in_kill_zone(ts) -> bool:
    """London 02–05 UTC | NY 12–15 UTC | Asian overlap 00–02 UTC."""
    h = pd.Timestamp(ts).hour
    return (0 <= h < 2) or (2 <= h < 5) or (12 <= h < 15)


# ══════════════════════════════════════════════════════════════════════════════
#  SIGNAL COMBINER — all 3 layers
# ══════════════════════════════════════════════════════════════════════════════
def generate_signals(
    m5:       pd.DataFrame,
    h1_bias:  str,
    ob_zones: dict,
    m15_bias: str,
    m15_data: dict,
) -> pd.DataFrame:
    """
    Entry fires on M5 when ALL conditions align:
      H1  : trend bias + valid unmitigated OB exists
      M15 : bias aligns + liquidity sweep + MSS
      M5  : FVG inside H1 OB + RSI ok + momentum candle + session + cooldown
    """
    df = prepare_m5(m5)

    bias_bull = (h1_bias == "BULLISH") and (m15_bias == "BULLISH")
    bias_bear = (h1_bias == "BEARISH") and (m15_bias == "BEARISH")

    ob_bull = ob_zones.get("bullish_ob")
    ob_bear = ob_zones.get("bearish_ob")

    # OB proximity per M5 bar
    in_bull_ob = (
        df["Close"].apply(lambda p: price_in_ob_zone(p, ob_bull))
        if (bias_bull and ob_bull) else pd.Series(False, index=df.index)
    )
    in_bear_ob = (
        df["Close"].apply(lambda p: price_in_ob_zone(p, ob_bear))
        if (bias_bear and ob_bear) else pd.Series(False, index=df.index)
    )

    # M15 flags broadcast to M5 index
    bull_sweep = pd.Series(m15_data["bull_sweep"], index=df.index)
    bear_sweep = pd.Series(m15_data["bear_sweep"], index=df.index)
    bull_mss   = pd.Series(m15_data["bull_mss"] if CFG.require_mss else True, index=df.index)
    bear_mss   = pd.Series(m15_data["bear_mss"] if CFG.require_mss else True, index=df.index)

    rsi_buy_ok  = df["RSI"] < CFG.rsi_overbought
    rsi_sell_ok = df["RSI"] > CFG.rsi_oversold
    momentum    = df["BodyRatio"] >= CFG.body_ratio_min

    session = (
        pd.Series([in_kill_zone(t) for t in df.index], index=df.index)
        if CFG.session_filter else pd.Series(True, index=df.index)
    )

    buy_cond = (
        bias_bull         &  # H1 + M15 both bullish
        df["Bullish_FVG"] &  # M5 FVG present
        in_bull_ob        &  # FVG inside H1 OB zone  ← KEY gate
        bull_sweep        &  # M15 liquidity swept
        bull_mss          &  # M15 MSS confirmed
        rsi_buy_ok        &  # RSI not exhausted
        momentum          &  # strong body candle
        session              # kill zone
    )
    sell_cond = (
        bias_bear         &
        df["Bearish_FVG"] &
        in_bear_ob        &  # FVG inside H1 OB zone
        bear_sweep        &
        bear_mss          &
        rsi_sell_ok       &
        momentum          &
        session
    )

    df["Signal"] = "HOLD"
    df.loc[buy_cond,  "Signal"] = "BUY"
    df.loc[sell_cond, "Signal"] = "SELL"

    # ── Cooldown ──────────────────────────────────────────────────────────────
    last_bar: dict[str, int] = {"BUY": -999, "SELL": -999}
    suppress = set()
    for idx in df.index:
        sig = df.at[idx, "Signal"]
        if sig in ("BUY", "SELL"):
            pos = df.index.get_loc(idx)
            if pos - last_bar[sig] < CFG.cooldown_bars:
                suppress.add(idx)
            else:
                last_bar[sig] = pos
    df.loc[list(suppress), "Signal"] = "HOLD"

    # ── Confluence score (max 8) ───────────────────────────────────────────────
    active = df["Signal"] != "HOLD"
    df["Score"] = 0
    df.loc[active,                                 "Score"] += 1  # FVG
    df.loc[active & in_bull_ob  & buy_cond,        "Score"] += 2  # OB zone (2pts — core filter)
    df.loc[active & in_bear_ob  & sell_cond,       "Score"] += 2
    df.loc[active & (bull_sweep | bear_sweep),     "Score"] += 1  # Sweep
    df.loc[active & (bull_mss   | bear_mss),       "Score"] += 1  # MSS
    df.loc[active & momentum,                      "Score"] += 1  # Momentum
    df.loc[active & session,                       "Score"] += 1  # Session
    df.loc[active & rsi_buy_ok  & buy_cond,        "Score"] += 1  # RSI clear
    df.loc[active & rsi_sell_ok & sell_cond,       "Score"] += 1

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  TRADE PLAN — OB-anchored SL, partial + full TP
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_plans(
    df:       pd.DataFrame,
    ob_zones: dict,
    h1_atr:   float,
) -> pd.DataFrame:
    """
    SL placement (OB-anchored):
      BUY  → below OB wick low  − 0.5× H1 ATR
      SELL → above OB wick high + 0.5× H1 ATR

    TP1 (partial 50%): Entry ± risk × 1.0  → move SL to breakeven
    TP2 (remaining):   Entry ± risk × RR
    """
    s         = df.copy()
    buy_mask  = s["Signal"] == "BUY"
    sell_mask = s["Signal"] == "SELL"
    atr_buf   = h1_atr * 0.5

    ob_bull = ob_zones.get("bullish_ob")
    ob_bear = ob_zones.get("bearish_ob")

    s["Entry"]   = s["Close"].round(4)
    s["SL"]      = 0.0
    s["TP1"]     = 0.0
    s["TP2"]     = 0.0
    s["Risk"]    = 0.0
    s["OB_Zone"] = ""

    if ob_bull and buy_mask.any():
        sl = round(ob_bull["wick_low"] - atr_buf, 4)
        s.loc[buy_mask, "SL"]      = sl
        s.loc[buy_mask, "Risk"]    = (s.loc[buy_mask, "Entry"] - sl).round(4)
        s.loc[buy_mask, "TP1"]     = (s.loc[buy_mask, "Entry"] + s.loc[buy_mask, "Risk"] * 1.0).round(4)
        s.loc[buy_mask, "TP2"]     = (s.loc[buy_mask, "Entry"] + s.loc[buy_mask, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[buy_mask, "OB_Zone"] = f"{ob_bull['ob_low']} – {ob_bull['ob_high']}"

    if ob_bear and sell_mask.any():
        sl = round(ob_bear["wick_high"] + atr_buf, 4)
        s.loc[sell_mask, "SL"]      = sl
        s.loc[sell_mask, "Risk"]    = (sl - s.loc[sell_mask, "Entry"]).round(4)
        s.loc[sell_mask, "TP1"]     = (s.loc[sell_mask, "Entry"] - s.loc[sell_mask, "Risk"] * 1.0).round(4)
        s.loc[sell_mask, "TP2"]     = (s.loc[sell_mask, "Entry"] - s.loc[sell_mask, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[sell_mask, "OB_Zone"] = f"{ob_bear['ob_low']} – {ob_bear['ob_high']}"

    def rate(n):
        if n >= 8: return "🔥 PRIME SETUP"
        if n >= 6: return "⭐⭐ STRONG"
        if n >= 4: return "⭐  GOOD"
        return             "✗   SKIP"

    s["Rating"] = s["Score"].apply(rate)
    return s


# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY
# ══════════════════════════════════════════════════════════════════════════════
def print_report(
    result:    pd.DataFrame,
    h1_bias:   str,
    m15_bias:  str,
    ob_zones:  dict,
    scan_time: str,
):
    active  = result[result["Signal"] != "HOLD"]
    ob_bull = ob_zones.get("bullish_ob")
    ob_bear = ob_zones.get("bearish_ob")

    print("\n" + "═" * 72)
    print(f"  SMC SCALPER  ─  {CFG.symbol}  ─  {scan_time}")
    print(f"  H1 Structure+OB  |  M15 Bias/Sweep/MSS  |  M5 FVG-in-OB Entry")
    print("═" * 72)
    print(f"  H1  Bias      : {h1_bias}")
    print(f"  M15 Bias      : {m15_bias}")

    if ob_bull:
        t = ob_bull["time"].strftime("%m-%d %H:%M") if hasattr(ob_bull["time"], "strftime") else str(ob_bull["time"])
        print(f"  H1 Bullish OB : {ob_bull['ob_low']} – {ob_bull['ob_high']}  [{t} UTC]  ← BUY ZONE")
    else:
        print(f"  H1 Bullish OB : Not found / fully mitigated")

    if ob_bear:
        t = ob_bear["time"].strftime("%m-%d %H:%M") if hasattr(ob_bear["time"], "strftime") else str(ob_bear["time"])
        print(f"  H1 Bearish OB : {ob_bear['ob_low']} – {ob_bear['ob_high']}  [{t} UTC]  ← SELL ZONE")
    else:
        print(f"  H1 Bearish OB : Not found / fully mitigated")

    print(f"  RR: 1:{CFG.rr_ratio}  |  Partial TP: {int(CFG.partial_tp*100)}% at TP1  |  Session: {'ON (London/NY)' if CFG.session_filter else 'OFF'}")
    print("─" * 72)

    if active.empty:
        print("\n  No M5 FVG detected inside the H1 OB zone yet.")
        print("  All 3 layers must align simultaneously.\n")
        print("  What to watch:")
        if ob_bull:
            print(f"    BUY  → wait for M5 price to enter {ob_bull['ob_low']} – {ob_bull['ob_high']}")
            print(f"           then look for a bullish FVG forming inside that zone.")
        if ob_bear:
            print(f"    SELL → wait for M5 price to enter {ob_bear['ob_low']} – {ob_bear['ob_high']}")
            print(f"           then look for a bearish FVG forming inside that zone.")
        print()
    else:
        cols = ["Signal", "Entry", "SL", "TP1", "TP2", "Risk", "OB_Zone", "RSI", "Rating"]
        print(active[cols].tail(5).to_string())
        print()

        last      = active.iloc[-1]
        direction = "▲ LONG  (BUY)" if last["Signal"] == "BUY" else "▼ SHORT (SELL)"
        print("─" * 72)
        print(f"  LATEST SIGNAL  {direction}")
        print(f"  Time         : {last.name.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Entry        : {last['Entry']}  ← M5 FVG close, inside H1 OB")
        print(f"  H1 OB Zone   : {last['OB_Zone']}")
        print(f"  Stop Loss    : {last['SL']}  ← OB wick + 0.5× H1 ATR buffer")
        print(f"  TP1 (50%)    : {last['TP1']}  ← close half here, move SL to BE")
        print(f"  TP2 (50%)    : {last['TP2']}  ← let rest run  (1:{CFG.rr_ratio} RR)")
        print(f"  Risk/pt      : {last['Risk']}")
        print(f"  RSI          : {last['RSI']}")
        print(f"  Rating       : {last['Rating']}")
        print(f"  Stack check  : H1 OB ✓  H1 Trend ✓  M15 Sweep ✓  M15 MSS ✓  M5 FVG-in-OB ✓  Momentum ✓")

    buys  = (active["Signal"] == "BUY").sum()
    sells = (active["Signal"] == "SELL").sum()
    print("─" * 72)
    print(f"  Signals → BUY: {buys}  SELL: {sells}  Filtered out: {len(result) - len(active)}")
    print("═" * 72 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
#  SCAN LOOP
# ══════════════════════════════════════════════════════════════════════════════
async def run_scan():
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    log.info("Scanning %s — %s", CFG.symbol, now)

    h1, m15, m5 = await fetch_all()
    if h1 is None or m15 is None or m5 is None:
        log.error("Data fetch incomplete. Retrying next cycle.")
        return

    # Layer 1 — H1
    h1_bias  = get_h1_structure(h1)
    ob_zones = detect_order_blocks(h1, h1_bias)
    h1_atr_v = float(atr(h1, 7).iloc[-1])

    log.info("H1 bias: %s | Bull OB: %s | Bear OB: %s",
             h1_bias,
             "found" if ob_zones.get("bullish_ob") else "none",
             "found" if ob_zones.get("bearish_ob") else "none")

    if h1_bias == "NEUTRAL":
        print(f"\n  [{now}] H1 NEUTRAL — no directional bias. Waiting.\n")
        return

    # Layer 2 — M15
    m15_bias = get_m15_bias(m15)
    m15_data = analyze_m15(m15)
    log.info("M15 bias: %s | Sweep bull=%s bear=%s | MSS bull=%s bear=%s",
             m15_bias,
             m15_data["bull_sweep"], m15_data["bear_sweep"],
             m15_data["bull_mss"],   m15_data["bear_mss"])

    # Layer 3 — M5
    signals = generate_signals(m5, h1_bias, ob_zones, m15_bias, m15_data)
    result  = build_trade_plans(signals, ob_zones, h1_atr_v)

    print_report(result, h1_bias, m15_bias, ob_zones, now)


async def main():
    print("\n╔══════════════════════════════════════════════════════════════════╗")
    print("║   SMC SCALPER  —  H1 OB  |  M15 Sweep/MSS  |  M5 FVG Entry     ║")
    print("║   Deriv Synthetic Indices                                        ║")
    print("╚══════════════════════════════════════════════════════════════════╝\n")

    if CFG.live_mode:
        print(f"  Live mode ON — scanning every {CFG.scan_interval_s // 60} min (M5 close)")
        print("  Press Ctrl+C to stop.\n")
        try:
            while True:
                await run_scan()
                await asyncio.sleep(CFG.scan_interval_s)
        except KeyboardInterrupt:
            print("\n  Scanner stopped.")
            sys.exit(0)
    else:
        await run_scan()


if __name__ == "__main__":
    asyncio.run(main())
