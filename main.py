from alpaca_keys import ALPACA_API_KEY, ALPACA_SECRET_KEY

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime

import os
import time
import gc

client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)


# Get all tickers
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
assets = client.get_all_assets(search_params)
all_tickers = [asset.symbol for asset in assets if asset.tradable]

stock_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

def batch_tickers(all_tickers, batch_size):
    print("\n===== STARTED BATCHING TICKERS =====")
    
    all_data_frames = []
    
    batch_num = (len(all_tickers) + batch_size - 1) // batch_size

    for i in range(batch_num):
        print(f"\nBatch {i+1} / {batch_num}")
        current_tickers = all_tickers[i * batch_size : (i + 1) * batch_size]
        
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=current_tickers,
                timeframe=TimeFrame.Day, 
                start=datetime(2020, 1, 1)
            )

            bars = stock_client.get_stock_bars(request_params)
            
            if bars.df is not None:
                new_data = bars.df.reset_index()
                all_data_frames.append(new_data)
                print(f" -> Success: {len(new_data)} rows.")
            
            # Rate Limit 200 RPM
            time.sleep(0.5) 

        except Exception as e:
            print(f" -> Error in batch {i+1}: {e}")
            continue

        if (i + 1) % 10 == 0:
            temp_df = pd.concat(all_data_frames)
            temp_df.to_parquet(f'data/stocks_checkpoint_{i+1}.parquet')
            print(f"!!! Checkpoint saved at batch {i+1}")
        
            del temp_df
            all_data_frames = []
            gc.collect()
            
    if all_data_frames:
        final_df = pd.concat(all_data_frames).drop_duplicates()
        final_df.to_parquet('data/stocks_checkpoint_rest.parquet', engine='pyarrow', compression='snappy')
        print("\n===== ALL DONE! Final file saved. =====")

batch_tickers(all_tickers, 50) 