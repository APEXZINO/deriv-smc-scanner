import pandas as pd
import asyncio
import json
import logging
import websockets

# ── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Config (keep your original credentials as-is) ─────────────────────────────
API_TOKEN   = "eHDQAIUyPXvtgLL"
APP_ID      = "1089"
SYMBOL      = "R_75"
GRANULARITY = 1800   # M30 in seconds
CANDLE_COUNT = 100
URI = f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"


# ── Data Fetching ──────────────────────────────────────────────────────────────
async def fetch_deriv_data() -> pd.DataFrame | None:
    """Connect to Deriv WebSocket, authorize, and pull M30 candle history."""
    try:
        async with websockets.connect(URI, ping_timeout=15) as ws:

            # 1. Authorize
            log.info("Authorizing with Deriv API...")
            await ws.send(json.dumps({"authorize": API_TOKEN}))
            auth_resp = json.loads(await ws.recv())

            if "error" in auth_resp:
                log.error("Authorization failed: %s", auth_resp["error"]["message"])
                return None
            log.info("Authorized as: %s", auth_resp.get("authorize", {}).get("loginid", "unknown"))

            # 2. Request candles (M30)
            request = {
                "ticks_history": SYMBOL,
                "adjust_start_time": 1,
                "count": CANDLE_COUNT,
                "end": "latest",
                "style": "candles",
                "granularity": GRANULARITY,
            }
            await ws.send(json.dumps(request))
            response = json.loads(await ws.recv())

            if "error" in response:
                log.error("Market data error: %s", response["error"]["message"])
                return None

            candles = response.get("candles", [])
            if not candles:
                log.warning("No candle data returned.")
                return None

            # 3. Build DataFrame
            df = pd.DataFrame(candles)
            df.rename(
                columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"},
                inplace=True,
            )
            df[["Open", "High", "Low", "Close"]] = df[["Open", "High", "Low", "Close"]].apply(
                pd.to_numeric, errors="coerce"
            )

            # Convert epoch → readable datetime index
            df["Time"] = pd.to_datetime(df["epoch"], unit="s")
            df.set_index("Time", inplace=True)
            df.drop(columns=["epoch"], inplace=True)

            log.info("Fetched %d candles for %s (M30)", len(df), SYMBOL)
            return df

    except websockets.exceptions.WebSocketException as e:
        log.error("WebSocket error: %s", e)
        return None
    except asyncio.TimeoutError:
        log.error("Connection timed out.")
        return None


# ── SMC Signal Generation ──────────────────────────────────────────────────────
def generate_smc_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect Fair Value Gaps (FVG) using proper 3-candle structure:

    Bullish FVG:  Candle[i-2].High < Candle[i].Low  (gap above candle 1, below candle 3)
    Bearish FVG:  Candle[i-2].Low  > Candle[i].High (gap below candle 1, above candle 3)

    Candle 2 (middle) is implicitly the body bridging the gap — the shift(2)
    comparison already validates the gap exists across the 3-candle window.
    """
    df = df.copy()

    # Bullish FVG: prior high (2 bars ago) is below current low → gap up
    df["Bullish_FVG"] = df["High"].shift(2) < df["Low"]

    # Bearish FVG: prior low (2 bars ago) is above current high → gap down
    df["Bearish_FVG"] = df["Low"].shift(2) > df["High"]

    # Assign signal — bearish takes priority on a conflict (rare edge case)
    df["Signal"] = "HOLD"
    df.loc[df["Bullish_FVG"], "Signal"] = "BUY"
    df.loc[df["Bearish_FVG"], "Signal"] = "SELL"

    return df


# ── Risk/Reward Calculation ────────────────────────────────────────────────────
def calculate_targets(signals: pd.DataFrame, rr_ratio: float = 2.0) -> pd.DataFrame:
    """
    Vectorized 1:RR take-profit calculation.

    BUY:  TP = Close + (Close - Low)  * rr_ratio
    SELL: TP = Close - (High - Close) * rr_ratio
    """
    signals = signals.copy()

    buy_mask  = signals["Signal"] == "BUY"
    sell_mask = signals["Signal"] == "SELL"

    signals["SL"] = 0.0
    signals["TP (1:%s RR)" % int(rr_ratio)] = 0.0

    # BUY targets
    signals.loc[buy_mask, "SL"] = signals.loc[buy_mask, "Low"].round(2)
    signals.loc[buy_mask, "TP (1:%s RR)" % int(rr_ratio)] = (
        signals.loc[buy_mask, "Close"]
        + (signals.loc[buy_mask, "Close"] - signals.loc[buy_mask, "Low"]) * rr_ratio
    ).round(2)

    # SELL targets
    signals.loc[sell_mask, "SL"] = signals.loc[sell_mask, "High"].round(2)
    signals.loc[sell_mask, "TP (1:%s RR)" % int(rr_ratio)] = (
        signals.loc[sell_mask, "Close"]
        - (signals.loc[sell_mask, "High"] - signals.loc[sell_mask, "Close"]) * rr_ratio
    ).round(2)

    return signals


# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    print("\n" + "=" * 55)
    print("   SMC / ICT FVG Scanner — Deriv Synthetic Indices")
    print("=" * 55)

    log.info("Connecting to Deriv WebSocket...")
    df = await fetch_deriv_data()

    if df is None or df.empty:
        log.error("Failed to pull market history. Verify your API token credentials.")
        return

    log.info("Running SMC signal analysis...")
    analyzed_df = generate_smc_signals(df)

    active_signals = analyzed_df[analyzed_df["Signal"] != "HOLD"]

    if active_signals.empty:
        print("\n  Markets scanned. No valid FVG mitigation zones forming right now.\n")
        return

    result = calculate_targets(active_signals)
    tp_col = [c for c in result.columns if c.startswith("TP")][0]

    display_cols = ["Open", "High", "Low", "Close", "SL", tp_col, "Signal"]
    latest = result[display_cols].tail(5)

    print("\n🚨 MATCHING SMC/ICT SETUP DETECTED 🚨\n")
    print(latest.to_string())

    # Summary
    buys  = (result["Signal"] == "BUY").sum()
    sells = (result["Signal"] == "SELL").sum()
    print(f"\n  Total signals found → BUY: {buys}  |  SELL: {sells}")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
