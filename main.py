import os
import asyncio
import json
import websockets
import pandas as pd
import numpy as np

# --- Core SMC/ICT Logic Engine ---
def detect_fvgs(df):
    df['Bullish_FVG'] = False
    df['Bearish_FVG'] = False
    df['FVG_Top'] = np.nan
    df['FVG_Bottom'] = np.nan
    for i in range(2, len(df)):
        if df['Low'].iloc[i] > df['High'].iloc[i-2] and df['Close'].iloc[i-1] > df['Open'].iloc[i-1]:
            df.loc[df.index[i], 'Bullish_FVG'] = True
            df.loc[df.index[i], 'FVG_Top'] = df['Low'].iloc[i]
            df.loc[df.index[i], 'FVG_Bottom'] = df['High'].iloc[i-2]
        elif df['High'].iloc[i] < df['Low'].iloc[i-2] and df['Close'].iloc[i-1] < df['Open'].iloc[i-1]:
            df.loc[df.index[i], 'Bearish_FVG'] = True
            df.loc[df.index[i], 'FVG_Top'] = df['Low'].iloc[i-2]
            df.loc[df.index[i], 'FVG_Bottom'] = df['High'].iloc[i]
    return df

def detect_market_structure(df, window=5):
    df['Swing_High'] = df['High'].rolling(window=window, center=True).max() == df['High']
    df['Swing_Low'] = df['Low'].rolling(window=window, center=True).min() == df['Low']
    df['MSS_Bullish'] = False
    df['MSS_Bearish'] = False
    
    last_high, last_low = None, None
    for i in range(len(df)):
        if df['Swing_High'].iloc[i]: last_high = df['High'].iloc[i]
        if df['Swing_Low'].iloc[i]: last_low = df['Low'].iloc[i]
        if last_high and df['Close'].iloc[i] > last_high:
            df.loc[df.index[i], 'MSS_Bullish'] = True
            last_high = None
        if last_low and df['Close'].iloc[i] < last_low:
            df.loc[df.index[i], 'MSS_Bearish'] = True
            last_low = None
    return df

def generate_smc_signals(df):
    df = detect_market_structure(df)
    df = detect_fvgs(df)
    df['Signal'] = 'HOLD'
    
    bullish_mss_active, bearish_mss_active = False, False
    bullish_counter, bearish_counter = 0, 0
    fvg_zones = []

    for i in range(len(df)):
        if df['MSS_Bullish'].iloc[i]:
            bullish_mss_active, bearish_mss_active, bullish_counter = True, False, 0
        if df['MSS_Bearish'].iloc[i]:
            bearish_mss_active, bullish_mss_active, bearish_counter = True, False, 0
            
        if bullish_mss_active:
            bullish_counter += 1
            if bullish_counter > 15: bullish_mss_active = False
        if bearish_mss_active:
            bearish_counter += 1
            if bearish_counter > 15: bearish_mss_active = False

        if df['Bullish_FVG'].iloc[i]:
            fvg_zones.append({'type': 'bullish', 'top': df['FVG_Top'].iloc[i], 'bottom': df['FVG_Bottom'].iloc[i], 'active': True})
        if df['Bearish_FVG'].iloc[i]:
            fvg_zones.append({'type': 'bearish', 'top': df['FVG_Top'].iloc[i], 'bottom': df['FVG_Bottom'].iloc[i], 'active': True})

        current_close = df['Close'].iloc[i]
        for zone in fvg_zones:
            if zone['active']:
                if zone['type'] == 'bullish' and bullish_mss_active and df['Low'].iloc[i] <= zone['top'] and current_close >= zone['bottom']:
                    df.loc[df.index[i], 'Signal'] = 'BUY'
                    zone['active'] = False
                    bullish_mss_active = False
                elif zone['type'] == 'bearish' and bearish_mss_active and df['High'].iloc[i] >= zone['bottom'] and current_close <= zone['top']:
                    df.loc[df.index[i], 'Signal'] = 'SELL'
                    zone['active'] = False
                    bearish_mss_active = False
    return df

# --- Updated Deriv Connection Engine ---
async def fetch_deriv_data():
    # Use standard App ID 1089 for connection endpoint
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    token = os.getenv('DERIV_APP_ID') # This pulls the token you saved in Secrets
    
    async with websockets.connect(url) as websocket:
        # First send the security token to authorize the session
        if token:
            auth_request = {"authorize": token}
            await websocket.send(json.dumps(auth_request))
            auth_response = await websocket.recv()
            auth_data = json.loads(auth_response)
            if 'error' in auth_data:
                print(f"Authorization Failed: {auth_data['error']['message']}")
                return None

        # Requesting Volatility 75 (1s) Index candles on a 15-minute interval ('M15')
        request = {
            "ticks_history": "1HZ75V", 
            "adjust_start_time": 1,
            "count": 100,
            "end": "latest",
            "style": "candles",
            "granularity": 1800 
        }
        
        await websocket.send(json.dumps(request))
        response = await websocket.recv()
        data = json.loads(response)
        
        if 'error' in data:
            print(f"Deriv API Error: {data['error']['message']}")
            return None
            
        candles = data.get('candles', [])
        if not candles:
            return None
            
        df = pd.DataFrame(candles)
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'epoch': 'Time'}, inplace=True)
        df['Time'] = pd.to_datetime(df['Time'], unit='s')
        df.set_index('Time', inplace=True)
        return df

async def main():
    print("Connecting and Authorizing with Deriv...")
    df = await fetch_deriv_data()
    
    if df is not None and not df.empty:
        print("Analyzing Synthetic Market Patterns...")
        analyzed_df = generate_smc_signals(df)
        signals = analyzed_df[analyzed_df['Signal'] != 'HOLD']
        
                if not signals.empty:
            print("\n🚨 MATCHING SMC/ICT SETUP DETECTED 🚨")
            
            # Create the 1:2 RR calculation columns
            signals = signals.copy()
            signals['TP (1:2 RR)'] = 0.0
            
            for idx, row in signals.iterrows():
                if row['Signal'] == 'BUY':
                    risk = row['Close'] - row['Low']
                    signals.at[idx, 'TP (1:2 RR)'] = round(row['Close'] + (risk * 2), 2)
                elif row['Signal'] == 'SELL':
                    risk = row['High'] - row['Close']
                    signals.at[idx, 'TP (1:2 RR)'] = round(row['Close'] - (risk * 2), 2)
            
            # Print updated table with new automated target
            print(signals[['Open', 'High', 'Low', 'Close', 'TP (1:2 RR)', 'Signal']].tail(5).to_string())
        else:
            print("\nMarkets Scanned. No valid FVG + MSS mitigation zones forming right now.")

    else:
        print("Failed to pull market history. Verify your API token credentials.")

if __name__ == "__main__":
    asyncio.run(main())
