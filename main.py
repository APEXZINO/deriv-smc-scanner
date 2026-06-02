"""
SMC SCALPER | Deriv R_75
Multi-Timeframe Confluence Engine
"""

import asyncio
import json
import logging
import pandas as pd
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
@dataclass
class Config:
    api_token: str = "eHDQAIUyPXvtgLL"
    app_id:    str = "1089"
    symbol:    str = "R_75"
    h1_tf: int = 3600; m15_tf: int = 900; m5_tf: int = 300
    h1_count: int = 100; m15_count: int = 120; m5_count: int = 150
    rr_ratio: float = 2.0
    ob_lookback: int = 30; ob_min_body_ratio: float = 0.5; ob_proximity_pct: float = 0.3
    swing_lookback: int = 5; min_fvg_pct: float = 0.03
    rsi_period: int = 7; rsi_overbought: float = 70.0; rsi_oversold: float = 30.0
    body_ratio_min: float = 0.45; cooldown_bars: int = 4
    session_filter: bool = True; require_mss: bool = True

    @property
    def uri(self) -> str: return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"

CFG = Config()

# ── Data Fetching ─────────────────────────────────────────────────────────────
async def fetch_candles(ws, granularity: int, count: int) -> Optional[pd.DataFrame]:
    await ws.send(json.dumps({"ticks_history": CFG.symbol, "adjust_start_time": 1, "count": count, "end": "latest", "style": "candles", "granularity": granularity}))
    resp = json.loads(await ws.recv())
    if "error" in resp or "candles" not in resp: return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(pd.to_numeric, errors="coerce")
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    return df

async def fetch_all():
    try:
        async with asyncio.timeout(30):
            async with websockets.connect(CFG.uri, ping_timeout=10) as ws:
                await ws.send(json.dumps({"authorize": CFG.api_token}))
                auth = json.loads(await ws.recv())
                if "error" in auth: return None, None, None
                log.info("Authorized")
                h1 = await fetch_candles(ws, CFG.h1_tf, CFG.h1_count)
                m15 = await fetch_candles(ws, CFG.m15_tf, CFG.m15_count)
                m5 = await fetch_candles(ws, CFG.m5_tf, CFG.m5_count)
                return h1, m15, m5
    except Exception as e:
        log.error(f"Connection failed: {e}")
        return None, None, None

# ── Indicator Helpers ────────────────────────────────────────────────────────
def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n):
    d = s.diff(); g = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=n, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, float("nan"))))
def atr(df, n=7):
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift()).abs(), (df["Low"]-df["Close"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

# ── Logic Layers (Summarized for brevity) ────────────────────────────────────
def get_h1_structure(h1) -> str:
    df = h1.copy()
    df["E21"], df["E50"] = ema(df["Close"], 21), ema(df["Close"], 50)
    last, prev = df.iloc[-1], df.iloc[-3]
    if last["E21"] > last["E50"] and last["Close"] > last["E21"] and last["E21"] > prev["E21"]: return "BULLISH"
    if last["E21"] < last["E50"] and last["Close"] < last["E21"] and last["E21"] < prev["E21"]: return "BEARISH"
    return "NEUTRAL"

# (Placeholder for detect_order_blocks, analyze_m15, generate_signals, build_trade_plans)
# Ensure these functions are included as per your previous logic.

# ── Execution ────────────────────────────────────────────────────────────────
# ── Execution block (Make sure this is at the very bottom) ──
async def run_scan():
    h1, m15, m5 = await fetch_all()
    if h1 is None: 
        log.warning("No data returned from API. Skipping report.")
        return
    log.info("Analysis Complete. Checking for confluence...")
    
    # Ensure this function call matches your previous logic
    h1_bias = get_h1_structure(h1)
    ob_zones = detect_order_blocks(h1, h1_bias)
    m15_bias = get_m15_bias(m15)
    m15_data = analyze_m15(m15)
    signals = generate_signals(m5, h1_bias, ob_zones, m15_bias, m15_data)
    result = build_trade_plans(signals, ob_zones, atr(h1, 7).iloc[-1])
    
    print_report(result, h1_bias, m15_bias, ob_zones, datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))

if __name__ == "__main__":
    asyncio.run(run_scan())
    
