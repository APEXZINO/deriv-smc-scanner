# ── H1: Trend + Order Block ───────────────────────────────────────────────────
def h1_trend(h1) -> str:
    df = h1.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    a, b = df.iloc[-1], df.iloc[-3]
    if a["E21"] > a["E50"] and a["Close"] > a["E21"] and a["E21"] > b["E21"] and a["E50"] > b["E50"]: return "BULLISH"
    if a["E21"] < a["E50"] and a["Close"] < a["E21"] and a["E21"] < b["E21"] and a["E50"] < b["E50"]: return "BEARISH"
    return "NEUTRAL"


def find_ob(h1, bias) -> dict:
    """
    Bullish OB = last strong red candle before a bullish structure break.
    Bearish OB = last strong green candle before a bearish structure break.
    Skips any OB that price has already mitigated (returned to).
    """
    df = h1.copy()
    df['BR'] = body_ratio(df)
    df['IsBull'] = df['Close'] > df['Open']
    df['IsBear'] = df['Close'] < df['Open']
    df['BullMSS'] = df['Close'] > df['High'].shift(1).rolling(5).max()
    df['BearMSS'] = df['Close'] < df['Low'].shift(1).rolling(5).min()
    lkb = df.iloc[-CFG.ob_lookback:]
    obs = {"bullish_ob": None, "bearish_ob": None}
    
    print(f"DEBUG: Scanning for OBs... Current Bias: {bias}")
    
    if bias == "BULLISH":
        for idx in reversed(lkb[lkb['BullMSS']].index.tolist()):
            pool = lkb.loc[:idx].iloc[-1]
            mask = (pool['IsBear'] == True) & (pool['BR'] >= CFG.ob_min_body_ratio)
            pool = lkb.loc[:idx].iloc[-1:] 
            pool = pool[mask]
            
            if pool.empty: 
                continue
            ob = pool.iloc[-1]
            hi, lo = max(ob['Open'], ob['Close']), min(ob['Open'], ob['Close'])
            if df.loc[ob.name:]["Low"].min() < lo: continue # mitigated
            obs["bullish_ob"] = {"time": ob.name, "ob_high": round(hi, 4),
                                "ob_low": round(lo, 4), "wick_low": round(ob['Low'], 4)}
            break

    elif bias == "BEARISH":
        for idx in reversed(lkb[lkb['BearMSS']].index.tolist()):
            pool = lkb.loc[:idx].iloc[-1]
            mask = (pool['IsBull'] == True) & (pool['BR'] >= CFG.ob_min_body_ratio)
            pool = lkb.loc[:idx].iloc[-1:]
            pool = pool[mask]
            
            if pool.empty: 
                continue
            ob = pool.iloc[-1]
            hi, lo = max(ob['Open'], ob['Close']), min(ob['Open'], ob['Close'])
            if df.loc[ob.name:]["High"].max() > hi: continue # mitigated
            obs["bearish_ob"] = {"time": ob.name, "ob_high": round(hi, 4),
                                "ob_low": round(lo, 4), "wick_high": round(ob['High'], 4)}
            break

    return obs
    
