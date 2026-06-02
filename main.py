import asyncio, json, logging
import pandas as pd
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

@dataclass
class Config:
    api_token: str = "eHDQAIUyPXvtgLL"
    app_id:    str = "1089"
    symbol:    str = "R_75"
    # ... (all your other settings) ...

    @property
    def uri(self) -> str:
        return f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"

CFG = Config()


# --- UTILS ---
def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n):
    d = s.diff(); g = d.clip(lower=0).ewm(span=n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(span=n, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, float("nan"))))
def atr(df, n=7):
    tr = pd.concat([df["High"]-df["Low"], (df["High"]-df["Close"].shift()).abs(), (df["Low"]-df["Close"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

# --- LOGIC LAYERS ---
def get_h1_structure(h1):
    df = h1.copy()
    df["E21"], df["E50"] = ema(df["Close"], 21), ema(df["Close"], 50)
    last, prev = df.iloc[-1], df.iloc[-3]
    if last["E21"] > last["E50"] and last["Close"] > last["E21"] and last["E21"] > prev["E21"]: return "BULLISH"
    if last["E21"] < last["E50"] and last["Close"] < last["E21"] and last["E21"] < prev["E21"]: return "BEARISH"
    return "NEUTRAL"

def detect_order_blocks(h1, bias):
    # Paste your detect_order_blocks logic here
    return {"bullish_ob": None, "bearish_ob": None}

def analyze_m15(m15):
    # Paste your analyze_m15 logic here
    return {"bull_sweep": False, "bear_sweep": False, "bull_mss": False, "bear_mss": False}

def generate_signals(m5, h1_bias, ob_zones, m15_bias, m15_data):
    # Paste your generate_signals logic here
    m5["Signal"] = "HOLD"
    return m5

def build_trade_plans(df, ob_zones, h1_atr):
    # Paste your build_trade_plans logic here
    df["Signal"] = "HOLD"
    return df

def print_report(result, h1_bias, m15_bias, ob_zones, scan_time):
    print(f"Analysis Complete at {scan_time}")

# --- EXECUTION ---
async def fetch_candles(ws, g, c):
    await ws.send(json.dumps({"ticks_history": CFG.symbol, "adjust_start_time": 1, "count": c, "end": "latest", "style": "candles", "granularity": g}))
    resp = json.loads(await ws.recv())
    if "candles" not in resp: return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(pd.to_numeric, errors="coerce")
    return df

async def fetch_all():
    try:
        async with websockets.connect(CFG.uri, ping_timeout=10) as ws:
            await asyncio.wait_for(ws.send(json.dumps({"authorize": CFG.api_token})), timeout=10)
            auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if "error" in auth: return None, None, None
            h1 = await asyncio.wait_for(fetch_candles(ws, CFG.h1_tf, CFG.h1_count), timeout=10)
            m15 = await asyncio.wait_for(fetch_candles(ws, CFG.m15_tf, CFG.m15_count), timeout=10)
            m5 = await asyncio.wait_for(fetch_candles(ws, CFG.m5_tf, CFG.m5_count), timeout=10)
            return h1, m15, m5
    except Exception as e:
        log.error(f"Connection failed: {e}")
        return None, None, None

async def run_scan():
    h1, m15, m5 = await fetch_all()
    if h1 is None: return
    h1_bias = get_h1_structure(h1)
    ob_zones = detect_order_blocks(h1, h1_bias)
    m15_bias = get_m15_bias(m15)
    m15_data = analyze_m15(m15)
    signals = generate_signals(m5, h1_bias, ob_zones, m15_bias, m15_data)
    result = build_trade_plans(signals, ob_zones, atr(h1, 7).iloc[-1])
    print_report(result, h1_bias, m15_bias, ob_zones, datetime.now(timezone.utc).strftime("%H:%M:%S"))

if __name__ == "__main__":
    asyncio.run(run_scan())
    
