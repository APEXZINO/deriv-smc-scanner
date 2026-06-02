import pandas as pd
import json
import os
import asyncio
from websockets import connect

# --- Updated Deriv Connection Engine ---
async def fetch_deriv_data():
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    token = os.getenv('DERIV_APP_ID')
    
    async with connect(url) as websocket:
        if token:
            auth_request = {"authorize": token}
            await websocket.send(json.dumps(auth_request))
            auth_response = await websocket.recv()
            auth_data = json.loads(auth_response)
            if 'error' in auth_data:
                print(f"Authorization Failed: {auth_data['error']['message']}")
                return None
        
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
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
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
            signals = signals.copy()
            signals['TP (1:2 RR)'] = 0.0
            for idx, row in signals.iterrows():
                if row['Signal'] == 'BUY':
                    risk = row['Close'] - row['Low']
                    signals.at[idx, 'TP (1:2 RR)'] = round(row['Close'] + (risk * 2), 2)
                elif row['Signal'] == 'SELL':
                    risk = row['High'] - row['Close']
                    signals.at[idx, 'TP (1:2 RR)'] = round(row['Close'] - (risk * 2), 2)
            
            print(signals[['Open', 'High', 'Low', 'Close', 'TP (1:2 RR)', 'Signal']].tail(5).to_string())
        else:
            print("\nMarkets Scanned. No valid FVG + MSS mitigation zones forming right now.")
          
    else:
        print("Failed to pull market history. Verify your API token credentials.")

if __name__ == "__main__":
    asyncio.run(main())
