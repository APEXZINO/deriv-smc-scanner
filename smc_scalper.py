"""
SMC Scalper — Deriv Synthetic Indices
Stack: H1 (Structure + OB) >> M15 (Bias + Sweep + MSS) >> M5 (FVG inside OB)
Notifications: Telegram alert on every valid signal
"""

import asyncio, json, logging, sys, urllib.request, urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# =============================================================================
#  CONFIG  — only edit this section
# =============================================================================
@dataclass
class Config:
    # Deriv credentials
    api_token: str = "eHDQAIUyPXvtgLL"
    app_id:    str = "1089"
    symbol:    str = "R_75"

    # Telegram — set via GitHub Secrets (leave blank here)
    tg_token:  str = ""
    tg_chat_id:str = ""

    # Timeframes
    h1_tf:  int = 3600;  h1_count:  int = 100
    m15_tf: int = 900;   m15_count: int = 120
    m5_tf:  int = 300;   m5_count:  int = 150

    # Risk settings
    rr_ratio:   float = 2.0   # Take profit = risk x 2

    # Order Block settings
    ob_lookback:       int   = 30
    ob_min_body_ratio: float = 0.5
    ob_proximity_pct:  float = 0.3

    # Signal filters
    swing_lookback: int   = 5
    min_fvg_pct:    float = 0.03
    rsi_period:     int   = 7
    rsi_overbought: float = 70.0
    rsi_oversold:   float = 30.0
    body_ratio_min: float = 0.45
    cooldown_bars:  int   = 4
    session_filter: bool  = False  # False = scan 24/7
    require_mss:    bool  = True

    # Run mode — always False for GitHub Actions
    live_mode: bool = False

    @property
    def uri(self):
        return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"

CFG = Config()

# Load Telegram credentials from environment if available (GitHub Secrets)
import os
if os.environ.get("TG_TOKEN"):
    CFG.tg_token   = os.environ["TG_TOKEN"]
if os.environ.get("TG_CHAT_ID"):
    CFG.tg_chat_id = os.environ["TG_CHAT_ID"]


# =============================================================================
#  TELEGRAM ALERT
# =============================================================================
def send_telegram(message: str):
    if not CFG.tg_token or not CFG.tg_chat_id:
        log.info("Telegram not configured — skipping alert.")
        return
    try:
        url  = f"https://api.telegram.org/bot{CFG.tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    CFG.tg_chat_id,
            "text":       message,
            "parse_mode": "Markdown"
        }).encode()
        req  = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        log.info("Telegram alert sent.")
    except Exception as e:
        log.error("Telegram error: %s", e)


def build_alert(signal_row, obs, h1b, m15b) -> str:
    sig  = signal_row["Signal"]
    icon = "🟢 *BUY (LONG)*" if sig == "BUY" else "🔴 *SELL (SHORT)*"
    ob   = obs.get("bullish_ob") if sig == "BUY" else obs.get("bearish_ob")
    zone = f"{ob['ob_low']} - {ob['ob_high']}" if ob else "N/A"

    return (
        f"{icon}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Symbol:*  {CFG.symbol}\n"
        f"*Time:*    {signal_row.name.strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*Entry:*   {signal_row['Entry']}\n"
        f"*SL:*      {signal_row['SL']}\n"
        f"*TP1:*     {signal_row['TP1']}  _(close 50%, move SL to BE)_\n"
        f"*TP2:*     {signal_row['TP2']}  _(let rest run)_\n"
        f"*Risk/pt:* {signal_row['Risk']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"*H1 OB Zone:* {zone}\n"
        f"*H1 Trend:*   {h1b}\n"
        f"*M15 Bias:*   {m15b}\n"
        f"*RSI:*        {signal_row['RSI']}\n"
        f"*Rating:*     {signal_row['Rating']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_Stack: H1 OB confirmed | M15 Sweep + MSS | M5 FVG in OB_"
    )


# =============================================================================
#  WEBSOCKET — fetch candles
# =============================================================================
async def fetch_candles(ws, granularity, count) -> Optional[pd.DataFrame]:
    await ws.send(json.dumps({
        "ticks_history":     CFG.symbol,
        "adjust_start_time": 1,
        "count":             count,
        "end":               "latest",
        "style":             "candles",
        "granularity":       granularity,
    }))
    resp = json.loads(await ws.recv())
    if "error" in resp or not resp.get("candles"):
        log.error("Fetch error %ds: %s", granularity,
                  resp.get("error", {}).get("message", "no candles"))
        return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(
        pd.to_numeric, errors="coerce"
    )
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
            log.info("Authorized: %s", auth.get("authorize", {}).get("loginid", "?"))
            h1  = await fetch_candles(ws, CFG.h1_tf,  CFG.h1_count)
            m15 = await fetch_candles(ws, CFG.m15_tf, CFG.m15_count)
            m5  = await fetch_candles(ws, CFG.m5_tf,  CFG.m5_count)
            return h1, m15, m5
    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("Connection error: %s", e)
        return None, None, None


# =============================================================================
#  INDICATORS
# =============================================================================
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def body_ratio(df):
    rng = (df["High"] - df["Low"]).replace(0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng

def swing_hi(df, n):
    return df["High"] == df["High"].rolling(n*2+1, center=True).max()

def swing_lo(df, n):
    return df["Low"] == df["Low"].rolling(n*2+1, center=True).min()

def rsi(s, n):
    d = s.diff()
    g = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=n, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, float("nan"))))

def atr(df, n=7):
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"]  - df["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


# =============================================================================
#  LAYER 1 — H1: TREND + ORDER BLOCK
# =============================================================================
def h1_trend(h1) -> str:
    df = h1.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    a, b = df.iloc[-1], df.iloc[-3]
    bull = (a["E21"] > a["E50"] and a["Close"] > a["E21"]
            and a["E21"] > b["E21"] and a["E50"] > b["E50"])
    bear = (a["E21"] < a["E50"] and a["Close"] < a["E21"]
            and a["E21"] < b["E21"] and a["E50"] < b["E50"])
    if bull: return "BULLISH"
    if bear: return "BEARISH"
    return "NEUTRAL"


def find_ob(h1, bias) -> dict:
    """
    Bullish OB: last strong red candle before a bullish MSS on H1.
    Bearish OB: last strong green candle before a bearish MSS on H1.
    Rejects any OB that price has already returned to (mitigated).
    SL is anchored to the OB wick, not the body.
    """
    df = h1.copy()
    df["BR"]      = body_ratio(df)
    df["IsBull"]  = df["Close"] > df["Open"]
    df["IsBear"]  = df["Close"] < df["Open"]
    df["BullMSS"] = df["Close"] > df["High"].shift(1).rolling(5).max()
    df["BearMSS"] = df["Close"] < df["Low"].shift(1).rolling(5).min()
    lkb = df.iloc[-CFG.ob_lookback:]
    obs = {"bullish_ob": None, "bearish_ob": None}

    if bias == "BULLISH":
        for idx in reversed(lkb[lkb["BullMSS"]].index.tolist()):
            pool = lkb.loc[:idx].iloc[:-1]
            pool = pool[pool["IsBear"] & (pool["BR"] >= CFG.ob_min_body_ratio)]
            if pool.empty:
                continue
            ob = pool.iloc[-1]
            hi = max(ob["Open"], ob["Close"])
            lo = min(ob["Open"], ob["Close"])
            if df.loc[ob.name:]["Low"].min() < lo:
                continue  # mitigated
            obs["bullish_ob"] = {
                "time":     ob.name,
                "ob_high":  round(hi, 4),
                "ob_low":   round(lo, 4),
                "wick_low": round(ob["Low"], 4),
            }
            break

    if bias == "BEARISH":
        for idx in reversed(lkb[lkb["BearMSS"]].index.tolist()):
            pool = lkb.loc[:idx].iloc[:-1]
            pool = pool[pool["IsBull"] & (pool["BR"] >= CFG.ob_min_body_ratio)]
            if pool.empty:
                continue
            ob = pool.iloc[-1]
            hi = max(ob["Open"], ob["Close"])
            lo = min(ob["Open"], ob["Close"])
            if df.loc[ob.name:]["High"].max() > hi:
                continue  # mitigated
            obs["bearish_ob"] = {
                "time":      ob.name,
                "ob_high":   round(hi, 4),
                "ob_low":    round(lo, 4),
                "wick_high": round(ob["High"], 4),
            }
            break

    return obs


# =============================================================================
#  LAYER 2 — M15: BIAS + SWEEP + MSS
# =============================================================================
def m15_bias(m15) -> str:
    df = m15.copy()
    df["E8"]  = ema(df["Close"], 8)
    df["E21"] = ema(df["Close"], 21)
    a, b = df.iloc[-1], df.iloc[-2]
    if a["E8"] > a["E21"] and a["Close"] > a["E8"] and a["E8"] > b["E8"]: return "BULLISH"
    if a["E8"] < a["E21"] and a["Close"] < a["E8"] and a["E8"] < b["E8"]: return "BEARISH"
    return "NEUTRAL"


def m15_context(m15) -> dict:
    df = m15.copy()
    n  = CFG.swing_lookback
    df["RSH"] = df["High"].where(swing_hi(df, n)).ffill()
    df["RSL"] = df["Low"].where(swing_lo(df,  n)).ffill()
    df["BSw"] = (df["Low"]  < df["RSL"].shift(1)) & (df["Close"] > df["RSL"].shift(1))
    df["SSw"] = (df["High"] > df["RSH"].shift(1)) & (df["Close"] < df["RSH"].shift(1))
    df["BMSS"]= df["Close"] > df["High"].shift(1).rolling(n).max()
    df["SMSS"]= df["Close"] < df["Low"].shift(1).rolling(n).min()
    return {
        "bull_sweep": bool(df["BSw"].tail(5).any()),
        "bear_sweep": bool(df["SSw"].tail(5).any()),
        "bull_mss":   bool(df["BMSS"].tail(3).any()),
        "bear_mss":   bool(df["SMSS"].tail(3).any()),
    }


# =============================================================================
#  LAYER 3 — M5: FVG INSIDE H1 OB
# =============================================================================
def _in_ob(price, ob):
    tol = price * (CFG.ob_proximity_pct / 100)
    return (ob["ob_low"] - tol) <= price <= (ob["ob_high"] + tol)

def _in_session(ts):
    h = pd.Timestamp(ts).hour
    return (0 <= h < 2) or (2 <= h < 5) or (12 <= h < 15)


def build_signals(m5, h1b, obs, m15b, ctx) -> pd.DataFrame:
    df = m5.copy()

    # FVG detection
    bg = df["Low"] - df["High"].shift(2)
    sg = df["Low"].shift(2) - df["High"]
    ms = CFG.min_fvg_pct / 100
    df["BFVG"] = (bg > 0) & (bg / df["Close"] > ms)
    df["SFVG"] = (sg > 0) & (sg / df["Close"] > ms)

    df["RSI"] = rsi(df["Close"], CFG.rsi_period).round(2)
    df["BR"]  = body_ratio(df).round(3)

    bull = (h1b == "BULLISH") and (m15b == "BULLISH")
    bear = (h1b == "BEARISH") and (m15b == "BEARISH")
    ob_b = obs.get("bullish_ob")
    ob_s = obs.get("bearish_ob")

    in_b = (df["Close"].apply(lambda p: _in_ob(p, ob_b))
            if bull and ob_b else pd.Series(False, index=df.index))
    in_s = (df["Close"].apply(lambda p: _in_ob(p, ob_s))
            if bear and ob_s else pd.Series(False, index=df.index))

    bsw  = pd.Series(ctx["bull_sweep"], index=df.index)
    ssw  = pd.Series(ctx["bear_sweep"], index=df.index)
    bmss = pd.Series(ctx["bull_mss"] if CFG.require_mss else True, index=df.index)
    smss = pd.Series(ctx["bear_mss"] if CFG.require_mss else True, index=df.index)

    rsi_b = df["RSI"] < CFG.rsi_overbought
    rsi_s = df["RSI"] > CFG.rsi_oversold
    mom   = df["BR"] >= CFG.body_ratio_min
    sess  = (pd.Series([_in_session(t) for t in df.index], index=df.index)
             if CFG.session_filter else pd.Series(True, index=df.index))

    buy  = bull & df["BFVG"] & in_b & bsw & bmss & rsi_b & mom & sess
    sell = bear & df["SFVG"] & in_s & ssw & smss & rsi_s & mom & sess

    df["Signal"] = "HOLD"
    df.loc[buy,  "Signal"] = "BUY"
    df.loc[sell, "Signal"] = "SELL"

    # Cooldown — prevent repeated signals in same zone
    last: dict = {"BUY": -999, "SELL": -999}
    drop = set()
    for idx in df.index:
        sig = df.at[idx, "Signal"]
        if sig in ("BUY", "SELL"):
            pos = df.index.get_loc(idx)
            if pos - last[sig] < CFG.cooldown_bars:
                drop.add(idx)
            else:
                last[sig] = pos
    df.loc[list(drop), "Signal"] = "HOLD"

    # Confluence score
    act = df["Signal"] != "HOLD"
    df["Score"] = 0
    df.loc[act,                  "Score"] += 1   # FVG
    df.loc[act & in_b & buy,     "Score"] += 2   # OB zone (double weight)
    df.loc[act & in_s & sell,    "Score"] += 2
    df.loc[act & (bsw | ssw),    "Score"] += 1   # Sweep
    df.loc[act & (bmss | smss),  "Score"] += 1   # MSS
    df.loc[act & mom,            "Score"] += 1   # Momentum
    df.loc[act & sess,           "Score"] += 1   # Session
    df.loc[act & rsi_b & buy,    "Score"] += 1   # RSI
    df.loc[act & rsi_s & sell,   "Score"] += 1

    return df


# =============================================================================
#  TRADE PLAN — OB-anchored SL, partial + full TP
# =============================================================================
def trade_plan(df, obs, h1_atr) -> pd.DataFrame:
    s   = df.copy()
    bm  = s["Signal"] == "BUY"
    sm  = s["Signal"] == "SELL"
    buf = h1_atr * 0.5
    ob_b = obs.get("bullish_ob")
    ob_s = obs.get("bearish_ob")

    for c in ["Entry", "SL", "TP1", "TP2", "Risk"]:
        s[c] = 0.0
    s["OB_Zone"] = ""
    s["Entry"]   = s["Close"].round(4)

    if ob_b and bm.any():
        sl = round(ob_b["wick_low"] - buf, 4)
        s.loc[bm, "SL"]      = sl
        s.loc[bm, "Risk"]    = (s.loc[bm, "Entry"] - sl).round(4)
        s.loc[bm, "TP1"]     = (s.loc[bm, "Entry"] + s.loc[bm, "Risk"] * 1.0).round(4)
        s.loc[bm, "TP2"]     = (s.loc[bm, "Entry"] + s.loc[bm, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[bm, "OB_Zone"] = f"{ob_b['ob_low']} - {ob_b['ob_high']}"

    if ob_s and sm.any():
        sl = round(ob_s["wick_high"] + buf, 4)
        s.loc[sm, "SL"]      = sl
        s.loc[sm, "Risk"]    = (sl - s.loc[sm, "Entry"]).round(4)
        s.loc[sm, "TP1"]     = (s.loc[sm, "Entry"] - s.loc[sm, "Risk"] * 1.0).round(4)
        s.loc[sm, "TP2"]     = (s.loc[sm, "Entry"] - s.loc[sm, "Risk"] * CFG.rr_ratio).round(4)
        s.loc[sm, "OB_Zone"] = f"{ob_s['ob_low']} - {ob_s['ob_high']}"

    s["Rating"] = s["Score"].apply(
        lambda n: "PRIME" if n >= 8 else "STRONG" if n >= 6 else "GOOD" if n >= 4 else "SKIP"
    )
    return s


# =============================================================================
#  REPORT — printed to GitHub Actions log
# =============================================================================
def report(result, h1b, m15b, obs, now):
    active = result[result["Signal"] != "HOLD"]
    ob_b   = obs.get("bullish_ob")
    ob_s   = obs.get("bearish_ob")

    print(f"\n{'='*65}", flush=True)
    print(f"  SMC SCALPER  |  {CFG.symbol}  |  {now}", flush=True)
    print(f"  H1: {h1b}  |  M15: {m15b}  |  RR 1:{CFG.rr_ratio}", flush=True)

    if ob_b:
        t = ob_b["time"].strftime("%m-%d %H:%M") if hasattr(ob_b["time"], "strftime") else str(ob_b["time"])
        print(f"  Bull OB: {ob_b['ob_low']} - {ob_b['ob_high']}  [{t} UTC]", flush=True)
    else:
        print("  Bull OB: none / mitigated", flush=True)

    if ob_s:
        t = ob_s["time"].strftime("%m-%d %H:%M") if hasattr(ob_s["time"], "strftime") else str(ob_s["time"])
        print(f"  Bear OB: {ob_s['ob_low']} - {ob_s['ob_high']}  [{t} UTC]", flush=True)
    else:
        print("  Bear OB: none / mitigated", flush=True)

    print(f"{'-'*65}", flush=True)

    if active.empty:
        print("  No setup. Waiting for M5 FVG inside H1 OB...", flush=True)
        if ob_b: print(f"  Watch BUY zone:  {ob_b['ob_low']} - {ob_b['ob_high']}", flush=True)
        if ob_s: print(f"  Watch SELL zone: {ob_s['ob_low']} - {ob_s['ob_high']}", flush=True)
    else:
        cols = ["Signal","Entry","SL","TP1","TP2","Risk","OB_Zone","RSI","Rating"]
        print(active[cols].tail(5).to_string(), flush=True)
        last = active.iloc[-1]
        sig  = "LONG (BUY)" if last["Signal"] == "BUY" else "SHORT (SELL)"
        print(f"\n  >> {sig}  {last.name.strftime('%Y-%m-%d %H:%M UTC')}", flush=True)
        print(f"     Entry:   {last['Entry']}", flush=True)
        print(f"     SL:      {last['SL']}", flush=True)
        print(f"     TP1:     {last['TP1']}  (close 50%, move SL to BE)", flush=True)
        print(f"     TP2:     {last['TP2']}  (let rest run)", flush=True)
        print(f"     Risk:    {last['Risk']}  |  RSI: {last['RSI']}  |  {last['Rating']}", flush=True)

        # Send Telegram alert for latest signal only
        send_telegram(build_alert(last, obs, h1b, m15b))

    buys  = (active["Signal"] == "BUY").sum()
    sells = (active["Signal"] == "SELL").sum()
    print(f"{'-'*65}", flush=True)
    print(f"  BUY: {buys}  SELL: {sells}  Filtered: {len(result)-len(active)}", flush=True)
    print(f"{'='*65}\n", flush=True)


# =============================================================================
#  MAIN
# =============================================================================
async def scan():
    now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    h1, m15, m5 = await fetch_all()
    if any(x is None for x in (h1, m15, m5)):
        log.error("Data fetch failed.")
        return

    h1b  = h1_trend(h1)
    obs  = find_ob(h1, h1b)
    h1av = float(atr(h1, 7).iloc[-1])
    log.info("H1: %s | Bull OB: %s | Bear OB: %s", h1b,
             "found" if obs.get("bullish_ob") else "none",
             "found" if obs.get("bearish_ob") else "none")

    if h1b == "NEUTRAL":
        print(f"\n  [{now}] H1 NEUTRAL — no bias. No trades.\n", flush=True)
        return

    m15b = m15_bias(m15)
    ctx  = m15_context(m15)
    log.info("M15: %s | Sweep B=%s S=%s | MSS B=%s S=%s",
             m15b, ctx["bull_sweep"], ctx["bear_sweep"],
             ctx["bull_mss"], ctx["bear_mss"])

    sigs   = build_signals(m5, h1b, obs, m15b, ctx)
    result = trade_plan(sigs, obs, h1av)
    report(result, h1b, m15b, obs, now)


async def main():
    print("SMC Scalper | H1 OB >> M15 Sweep/MSS >> M5 FVG | Deriv", flush=True)
    if CFG.live_mode:
        try:
            while True:
                await scan()
                await asyncio.sleep(300)
        except KeyboardInterrupt:
            print("Stopped.")
            sys.exit(0)
    else:
        await scan()

if __name__ == "__main__":
    asyncio.run(main())
