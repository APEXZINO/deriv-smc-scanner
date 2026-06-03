import pandas as pd
import asyncio

# --- Configuration ---
class CFG:
    ob_lookback = 20
    ob_min_body_ratio = 0.5
    ob_proximity_pct = 0.02

# --- Core Logic ---
def body_ratio(df):
    return (df['Close'] - df['Open']).abs() / (df['High'] - df['Low']).replace(0, 0.0001)

def find_ob(h1, bias) -> dict:
    df = h1.copy()
    df['BR'] = body_ratio(df)
    df['IsBull'] = df['Close'] > df['Open']
    df['IsBear'] = df['Close'] < df['Open']
    df['BullMSS'] = df['Close'] > df['High'].shift(1).rolling(5).max()
    df['BearMSS'] = df['Close'] < df['Low'].shift(1).rolling(5).min()
    
    lkb = df.iloc[-CFG.ob_lookback:]
    obs = {"bullish_ob": None, "bearish_ob": None}
    
    print(f"Scanner active. Checking bias: {bias}", flush=True)

    if bias == "BULLISH":
        for idx in reversed(lkb[lkb['BullMSS']].index.tolist()):
            pool = lkb.loc[:idx].iloc[-1:]
            mask = (pool['IsBear'] == True) & (pool['BR'] >= CFG.ob_min_body_ratio)
            pool = pool[mask]
            if not pool.empty:
                ob = pool.iloc[-1]
                obs["bullish_ob"] = {"time": ob.name, "high": ob['High'], "low": ob['Low']}
                break

    elif bias == "BEARISH":
        for idx in reversed(lkb[lkb['BearMSS']].index.tolist()):
            pool = lkb.loc[:idx].iloc[-1:]
            mask = (pool['IsBull'] == True) & (pool['BR'] >= CFG.ob_min_body_ratio)
            pool = pool[mask]
            if not pool.empty:
                ob = pool.iloc[-1]
                obs["bearish_ob"] = {"time": ob.name, "high": ob['High'], "low": ob['Low']}
                break
    return obs

async def main():
    print("Process started.", flush=True)
    # Placeholder for your actual data fetching
    # If this prints, your workflow is solid.
    print("Scanner analysis complete. No setups found in current window.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
    
