
import asyncio, json, logging, sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import websockets

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG  — edit only this block
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Config:
    api_token: str  = "eHDQAIUyPXvtgLL"
    app_id:    str  = "1089"
    symbol:    str  = "R_75"

    # Timeframes (seconds)
    h1_tf:  int = 3600   # H1  — structure + OB
    m15_tf: int = 900    # M15 — bias + sweep + MSS
    m5_tf:  int = 300    # M5  — FVG entry trigger

    # Candle counts
    h1_count:  int = 100
    m15_count: int = 120
    m5_count:  int = 150

    # Risk / Reward
    rr_ratio:   float = 2.0   # TP2 multiplier
    partial_tp: float = 0.5   # 50% closed at TP1

    # Order Block settings
    ob_lookback:       int   = 30    # H1 bars to scan
    ob_min_body_ratio: float = 0.5   # minimum body strength for a valid OB candle
    ob_proximity_pct:  float = 0.3   # M5 price tolerance to OB zone (%)

    # Signal filters
    swing_lookback: int   = 5
    min_fvg_pct:    float = 0.03    # minimum M5 FVG size (%)
    rsi_period:     int   = 7
    rsi_overbought: float = 70.0
    rsi_oversold:   float = 30.0
    body_ratio_min: float = 0.45
    cooldown_bars:  int   = 4       # M5 bars between same-direction signals
    session_filter: bool  = True    # restrict to London / NY kill zones
    require_mss:    bool  = True

    # Live scanning
    live_mode:       bool = True
    scan_interval_s: int  = 300     # 5 minutes

    @property
    def uri(self) -> str:
        return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"


CFG = Config()


# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET
# ══════════════════════════════════════════════════════════════════════════════
async def fetch_candles(ws, granularity: int, count: int) -> Optional[pd.DataFrame]:
    await ws.send(json.dumps({
        "ticks_history": CFG.symbol, "adjust_start_time": 1,
        "count": count, "end": "latest", "style": "candles",
        "granularity": granularity,
    }))
    resp = json.loads(await ws.recv())
    if "error" in resp:
        log.error("Fetch error (%ds): %s", granularity, resp["error"]["message"])
        return None
    candles = resp.get("candles", [])
    if not candles:
        return None
    df = pd.DataFrame(candles)
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(pd.to_numeric, errors="coerce")
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    df.drop(columns=["epoch"], inplace=True)
    return df


async def fetch_all():
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            await ws.send(json.dumps({"authorize": CFG.api_token}))
            auth = json.loads(await ws.recv())
            if "error" in auth:
                log.error("Auth failed: %s", auth["error"]["message"])
                return None, None, None
            log.info("Authorized  %s", auth.get("authorize", {}).get("loginid", "?"))
            log.info("Fetching H1...")
            h1  = await fetch_candles(ws, CFG.h1_tf,  CFG.h1_count)
            log.info("Fetching M15...")
            m15 = await fetch_candles(ws, CFG.m15_tf, CFG.m15_count)
            log.info("Fetching M5...")
            m5  = await fetch_candles(ws, CFG.m5_tf,  CFG.m5_count)
            return h1, m15, m5
    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("Connection error: %s", e)
        return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
#  INDICATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def rsi(s: pd.Series, n: int) -> pd.Series:
    d = s.diff()
    g = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=n, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, float("nan"))))

def atr(df: pd.DataFrame, n: int = 7) -> pd.Series:
    tr = pd.concat([df["High"]-df["Low"],
                    (df["High"]-df["Close"].shift()).abs(),
                    (df["Low"] -df["Close"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

def body_ratio(df: pd.DataFrame) -> pd.Series:
    return (df["Close"]-df["Open"]).abs() / (df["High"]-df["Low"]).replace(0, float("nan"))

def swing_highs(df: pd.DataFrame, n: int) -> pd.Series:
    return df["High"] == df["High"].rolling(n*2+1, center=True).max()

def swing_lows(df: pd.DataFrame, n: int) -> pd.Series:
    return df["Low"] == df["Low"].rolling(n*2+1, center=True).min()


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — H1: STRUCTURE BIAS + ORDER BLOCK
# ══════════════════════════════════════════════════════════════════════════════
def get_h1_structure(h1: pd.DataFrame) -> str:
    """EMA 21/50 trend. Both must slope in the same direction."""
    df = h1.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    last, prev = df.iloc[-1], df.iloc[-3]
    bull = (last["E21"] > last["E50"] and last["Close"] > last["E21"]
            and last["E21"] > prev["E21"] and last["E50"] > prev["E50"])
    bear = (last["E21"] < last["E50"] and last["Close"] < last["E21"]
            and last["E21"] < prev["E21"] and last["E50"] < prev["E50"])
    return "BULLISH" if bull else "BEARISH" if bear else "NEUTRAL"


def detect_order_blocks(h1: pd.DataFrame, bias: str) -> dict:
    """
    BULLISH OB: last strong red candle before a bullish MSS. Zone = body range.
                Unmitigated = no close below OB low since formation.
                SL anchor   = wick low.

    BEARISH OB: last strong green candle before a bearish MSS. Zone = body range.
                Unmitigated = no close above OB high since formation.
                SL anchor   = wick high.
    """
    df = h1.copy()
    df["BR"]       = body_ratio(df)
    df["IsBull"]   = df["Close"] > df["Open"]
    df["IsBear"]   = df["Close"] < df["Open"]
    rn             = 5
    df["RollHigh"] = df["High"].shift(1).rolling(rn).max()
    df["RollLow"]  = df["Low"].shift(1).rolling(rn).min()
    df["BullMSS"]  = df["Close"] > df["RollHigh"]
    df["BearMSS"]  = df["Close"] < df["RollLow"]

    lkb    = df.iloc[-CFG.ob_lookback:]
    result = {"bullish_ob": None, "bearish_ob": None}

    # Bullish OB
    if bias == "BULLISH":
        for mss_idx in reversed(lkb[lkb["BullMSS"]].index.tolist()):
            cands = lkb.loc[:mss_idx].iloc[:-1]
            cands = cands[cands["IsBear"] & (cands["BR"] >= CFG.ob_min_body_ratio)]
            if cands.empty: continue
            ob  = cands.iloc[-1]
            hi  = max(ob["Open"], ob["Close"])
            lo  = min(ob["Open"], ob["Close"])
            if df.loc[ob.name:]["Low"].min() < lo: continue   # mitigated
            result["bullish_ob"] = {"time": ob.name,
                                    "ob_high": round(hi, 4), "ob_low": round(lo, 4),
                                    "wick_low": round(ob["Low"], 4)}
            break

    # Bearish OB
    if bias == "BEARISH":
        for mss_idx in reversed(lkb[lkb["BearMSS"]].index.tolist()):
            cands = lkb.loc[:mss_idx].iloc[:-1]
            cands = cands[cands["IsBull"] & (cands["BR"] >= CFG.ob_min_body_ratio)]
            if cands.empty: continue
            ob  = cands.iloc[-1]
            hi  = max(ob["Open"], ob["Close"])
            lo  = min(ob["Open"], ob["Close"])
            if df.loc[ob.name:]["High"].max() > hi: continue  # mitigated
            result["bearish_ob"] = {"time": ob.name,
                                    "ob_high": round(hi, 4), "ob_low": round(lo, 4),
                                    "wick_high": round(ob["High"], 4)}
            break

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — M15: BIAS + SWEEP + MSS
# ══════════════════════════════════════════════════════════════════════════════
def get_m15_bias(m15: pd.DataFrame) -> str:
    """EMA 8/21 momentum — must match H1 direction."""
    df = m15.copy()
    df["E8"]  = ema(df["Close"], 8)
    df["E21"] = ema(df["Close"], 21)
    last, prev = df.iloc[-1], df.iloc[-2]
    if last["E8"] > last["E21"] and last["Close"] > last["E8"] and last["E8"] > prev["E8"]: return "BULLISH"
    if last["E8"] < last["E21"] and last["Close"] < last["E8"] and last["E8"] < prev["E8"]: return "BEARISH"
    return "NEUTRAL"


def analyze_m15(m15: pd.DataFrame) -> dict:
    """
    Liquidity sweep: wick beyond swing level, close back inside.
    MSS: close beyond rolling structure high/low.
    Returns scalar flags consumed by generate_signals.
    """
    df = m15.copy()
    n  = CFG.swing_lookback
    df["RSH"] = df["High"].where(swing_highs(df, n)).ffill()
    df["RSL"] = df["Low"].where(swing_lows(df,  n)).ffill()
    df["BullSweep"] = (df["Low"] < df["RSL"].shift(1)) & (df["Close"] > df["RSL"].shift(1))
    df["BearSweep"] = (df["High"] > df["RSH"].shift(1)) & (df["Close"] < df["RSH"].shift(1))
    df["BullMSS"]   = df["Close"] > df["High"].shift(1).rolling(n).max()
    df["BearMSS"]   = df["Close"] < df["Low"].shift(1).rolling(n).min()
    return {
        "bull_sweep": bool(df["BullSweep"].tail(5).any()),
        "bear_sweep": bool(df["BearSweep"].tail(5).any()),
        "bull_mss":   bool(df["BullMSS"].tail(3).any()),
        "bear_mss":   bool(df["BearMSS"].tail(3).any()),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 3 — M5: FVG INSIDE H1 OB ZONE
# ══════════════════════════════════════════════════════════════════════════════
def _in_ob(price: float, ob: dict) -> bool:
    tol = price * (CFG.ob_proximity_pct / 100)
    return (ob["ob_low"] - tol) <= price <= (ob["ob_high"] + tol)


def generate_signals(m5: pd.DataFrame, h1_bias: str,
                     ob_zones: dict, m15_bias: str, m15_data: dict) -> pd.DataFrame:
    """
    Entry on M5 fires only when ALL three layers align:
      H1  : trend confirmed + unmitigated OB exists
      M15 : bias matches + sweep + MSS
      M5  : FVG inside H1 OB + RSI ok + momentum candle + session + cooldown
    """
    df = m5.copy()

    # M5 indicators
    bg = df["Low"] - df["High"].shift(2)
    sg = df["Low"].shift(2) - df["High"]
    ms = CFG.min_fvg_pct / 100
    df["Bullish_FVG"] = (bg > 0) & (bg / df["Close"] > ms)
    df["Bearish_FVG"] = (sg > 0) & (sg / df["Close"] > ms)
    df["RSI"]         = rsi(df["Close"], CFG.rsi_period).round(2)
    df["BodyRatio"]   = body_ratio(df).round(3)
    df["ATR"]         = atr(df, 7)

    bias_bull = (h1_bias == "BULLISH") and (m15_bias == "BULLISH")
    bias_bear = (h1_bias == "BEARISH") and (m15_bias == "BEARISH")

    ob_bull = ob_zones.get("bullish_ob")
    ob_bear = ob_zones.get("bearish_ob")

    in_bull_ob = (df["Close"].apply(lambda p: _in_ob(p, ob_bull))
                  if bias_bull and ob_bull else pd.Series(False, index=df.index))
    in_bear_ob = (df["Close"].apply(lambda p: _in_ob(p, ob_bear))
                  if bias_bear and ob_bear else pd.Series(False, index=df.index))

    bull_sweep = pd.Series(m15_data["bull_sweep"], index=df.index)
    bear_sweep = pd.Series(m15_data["bear_sweep"], index=df.index)
    bull_mss   = pd.Series(m15_data["bull_mss"] if CFG.require_mss else True, index=df.index)
    bear_mss   = pd.Series(m15_data["bear_mss"] if CFG.require_mss else True, index=df.index)

    rsi_ok_buy  = df["RSI"] < CFG.rsi_overbought
    rsi_ok_sell = df["RSI"] > CFG.rsi_oversold
    momentum    = df["BodyRatio"] >= CFG.body_ratio_min

    def kz(ts):
        h = pd.Timestamp(ts).hour
        return (0 <= h < 2) or (2 <= h < 5) or (12 <= h < 15)

    session = (pd.Series([kz(t) for t in df.index], index=df.index)
               if CFG.session_filter else pd.Series(True, index=df.index))

    buy_cond  = bias_bull & df["Bullish_FVG"] & in_bull_ob & bull_sweep & bull_mss & rsi_ok_buy  & momentum & session
    sell_cond = bias_bear & df["Bearish_FVG"] & in_bear_ob & bear_sweep & bear_mss & rsi_ok_sell & momentum & session

    df["Signal"] = "HOLD"
    df.loc[buy_cond,  "Signal"] = "BUY"
    df.loc[sell_cond, "Signal"] = "SELL"

    # Cooldown — suppress signals too close together
    last_bar: dict = {"BUY": -999, "SELL": -999}
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

    # Confluence score (max 9)
    act = df["Signal"] != "HOLD"
    df["Score"] = 0
    df.loc[act,                              "Score"] += 1   # FVG
    df.loc[act & in_bull_ob & buy_cond,      "Score"] += 2   # OB zone (double weight)
    df.loc[act & in_bear_ob & sell_cond,     "Score"] += 2
    df.loc[act & (bull_sweep | bear_sweep),  "Score"] += 1   # Sweep
    df.loc[act & (bull_mss   | bear_mss),    "Score"] += 1   # MSS
    df.loc[act & momentum,                   "Score"] += 1   # Momentum
    df.loc[act & session,                    "Score"] += 1   # Session
    df.loc[act & rsi_ok_buy  & buy_cond,     "Score"] += 1   # RSI clear
    df.loc[act & rsi_ok_sell & sell_cond,    "Score"] += 1

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  TRADE PLAN
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_plans(df: pd.DataFrame, ob_zones: dict, h1_atr: float) -> pd.DataFrame:
    """
    SL  = OB wick extreme +/- 0.5x H1 ATR
    TP1 = 1:1 RR  (close 50%, move SL to breakeven)
    TP2 = 1:RR    (let rest run)
    """
    s         = df.copy()
    buy_mask  = s["Signal"] == "BUY"
    sell_mask = s["Signal"] == "SELL"
    buf       = h1_atr * 0.5
    ob_bull   = ob_zones.get("bullish_ob")
    ob_bear   = ob_zones.get("bearish_ob")

    for col in ["Entry", "SL", "TP1", "TP2", "Risk"]:
        s[col] = 0.0
    s["OB_Zone"] = ""
    s["Entry"]   = s["Close"].round(4)

    if ob_bull and buy_mask.any():
        sl = round(ob_bull["wick_low"] - buf, 4)
        s.loc[buy_mask, "SL"]      = sl
        s.loc[buy_mask, "Risk"]    = (s.loc[buy_mask, "Entry"] - sl).round(4)
        s.loc[buy_mask, "TP1"]     = (s.loc[buy_mask, "Entry"] + s.loc[buy_mask, "Risk"]).round(4)
        s.loc[buy_mask, "TP2"]     = (s.loc[buy_mask, "Entry"] + s.loc[buy_mask, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[buy_mask, "OB_Zone"] = f"{ob_bull['ob_low']} - {ob_bull['ob_high']}"

    if ob_bear and sell_mask.any():
        sl = round(ob_bear["wick_high"] + buf, 4)
        s.loc[sell_mask, "SL"]      = sl
        s.loc[sell_mask, "Risk"]    = (sl - s.loc[sell_mask, "Entry"]).round(4)
        s.loc[sell_mask, "TP1"]     = (s.loc[sell_mask, "Entry"] - s.loc[sell_mask, "Risk"]).round(4)
        s.loc[sell_mask, "TP2"]     = (s.loc[sell_mask, "Entry"] - s.loc[sell_mask, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[sell_mask, "OB_Zone"] = f"{ob_bear['ob_low']} - {ob_bear['ob_high']}"

    def rate(n):
        if n >= 8: return "PRIME"
        if n >= 6: return "STRONG"
        if n >= 4: return "GOOD"
        return             "SKIP"

    s["Rating"] = s["Score"].apply(rate)
    return s


# ══════════════════════════════════════════════════════════════════════════════
#  REPORT
# ══════════════════════════════════════════════════════════════════════════════
def print_report(result, h1_bias, m15_bias, ob_zones, scan_time):
    active  = result[result["Signal"] != "HOLD"]
    ob_bull = ob_zones.get("bullish_ob")
    ob_bear = ob_zones.get("bearish_ob")
    W       = 72

    def fmt_ob(ob, label):
        if not ob: return f"  {label}: Not found / all mitigated"
        t = ob["time"].strftime("%m-%d %H:%M") if hasattr(ob["time"], "strftime") else str(ob["time"])
        tag = "<< BUY ZONE" if "bullish" in label.lower() else "<< SELL ZONE"
        return f"  {label}: {ob['ob_low']} - {ob['ob_high']}  [{t} UTC]  {tag}"

    print("\n" + "=" * W)
    print(f"  SMC SCALPER  |  {CFG.symbol}  |  {scan_time}")
    print(f"  H1 Structure+OB  >>  M15 Bias/Sweep/MSS  >>  M5 FVG-in-OB Entry")
    print("=" * W)
    print(f"  H1  Bias : {h1_bias}    M15 Bias : {m15_bias}")
    print(fmt_ob(ob_bull, "H1 Bullish OB"))
    print(fmt_ob(ob_bear, "H1 Bearish OB"))
    print(f"  RR 1:{CFG.rr_ratio}  |  Partial TP: {int(CFG.partial_tp*100)}% at TP1  |  Session: {'ON (London/NY)' if CFG.session_filter else 'OFF'}")
    print("-" * W)

    if active.empty:
        print("\n  Waiting for M5 FVG to form inside the H1 OB zone...")
        if ob_bull: print(f"  BUY  -> watch M5 price approach {ob_bull['ob_low']} - {ob_bull['ob_high']}, then FVG forms.")
        if ob_bear: print(f"  SELL -> watch M5 price approach {ob_bear['ob_low']} - {ob_bear['ob_high']}, then FVG forms.")
        print()
    else:
        print(active[["Signal","Entry","SL","TP1","TP2","Risk","OB_Zone","RSI","Rating"]].tail(5).to_string())
        print()
        last = active.iloc[-1]
        d    = "LONG (BUY)" if last["Signal"] == "BUY" else "SHORT (SELL)"
        print("-" * W)
        print(f"  LATEST SIGNAL  >>  {d}")
        print(f"  Time    : {last.name.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Entry   : {last['Entry']}  (M5 FVG close inside H1 OB)")
        print(f"  OB Zone : {last['OB_Zone']}")
        print(f"  SL      : {last['SL']}  (OB wick + 0.5x H1 ATR buffer)")
        print(f"  TP1 50% : {last['TP1']}  (close half, move SL to breakeven)")
        print(f"  TP2 50% : {last['TP2']}  (let rest run  1:{CFG.rr_ratio})")
        print(f"  Risk/pt : {last['Risk']}    RSI: {last['RSI']}    Rating: {last['Rating']}")
        print(f"  Stack   : H1 OB + Trend  M15 Sweep + MSS  M5 FVG-in-OB  Momentum  all confirmed")

    buys, sells = (active["Signal"] == "BUY").sum(), (active["Signal"] == "SELL").sum()
    print("-" * W)
    print(f"  Signals  BUY: {buys}  SELL: {sells}  Filtered: {len(result) - len(active)}")
    print("=" * W + "\n")


# ══════════════════════════════════════════════════════════════════════════════
#  SCAN LOOP
# ══════════════════════════════════════════════════════════════════════════════
async def run_scan():
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    log.info("Scanning %s  %s", CFG.symbol, now)

    h1, m15, m5 = await fetch_all()
    if any(x is None for x in (h1, m15, m5)):
        log.error("Data fetch incomplete. Will retry.")
        return

    h1_bias  = get_h1_structure(h1)
    ob_zones = detect_order_blocks(h1, h1_bias)
    h1_atr_v = float(atr(h1, 7).iloc[-1])
    log.info("H1: %s | Bull OB: %s | Bear OB: %s", h1_bias,
             "found" if ob_zones.get("bullish_ob") else "none",
             "found" if ob_zones.get("bearish_ob") else "none")

    if h1_bias == "NEUTRAL":
        print(f"\n  [{now}] H1 NEUTRAL — waiting for directional bias.\n")
        return

    m15_bias = get_m15_bias(m15)
    m15_data = analyze_m15(m15)
    log.info("M15: %s | Sweep B/S=%s/%s | MSS B/S=%s/%s", m15_bias,
             m15_data["bull_sweep"], m15_data["bear_sweep"],
             m15_data["bull_mss"],   m15_data["bear_mss"])

    signals = generate_signals(m5, h1_bias, ob_zones, m15_bias, m15_data)
    result  = build_trade_plans(signals, ob_zones, h1_atr_v)
    print_report(result, h1_bias, m15_bias, ob_zones, now)


async def main():
    print("\n+------------------------------------------------------------------+")
    print("|  SMC SCALPER  |  H1 OB  >>  M15 Sweep/MSS  >>  M5 FVG Entry    |")
    print("|  Deriv Synthetic Indices                                         |")
    print("+------------------------------------------------------------------+\n")

    if CFG.live_mode:
        print(f"  Live mode ON — rescanning every {CFG.scan_interval_s // 60} min (M5 close)")
        print("  Ctrl+C to stop.\n")
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
