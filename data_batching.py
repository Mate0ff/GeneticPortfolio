from alpaca_keys import ALPACA_API_KEY, ALPACA_SECRET_KEY

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime

import time
import gc
import os

time_frame_mapping = {
    'minute' : TimeFrame.Minute,
    'hour' : TimeFrame.Hour,
    'day' : TimeFrame.Day,
    'week' : TimeFrame.Week,
    'month' : TimeFrame.Month,
}

class DataBatcher():

    def __init__(self, 
                 tickers: list, 
                 batch_size: int, 
                 output_dir: str):
        
        self._tickers = tickers
        self._batch_size = batch_size
        self._output_dir = output_dir
        self._stock_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    def batch_tickers(self, 
                      time_frame: str, 
                      start_date: datetime, 
                      end_date = None) -> None:
        
        if not os.path.isdir(f'{self._output_dir}/'):
            os.mkdir(f'{self._output_dir}/')

        print("\n===== STARTED BATCHING TICKERS =====")
        
        checkpoint_dfs = []
        batch_num = (len(self._tickers) + self._batch_size - 1) // self._batch_size

        for i in range(batch_num):
            print(f"\nBatch {i+1} / {batch_num}")
            current_tickers = self._tickers[i * self._batch_size : (i + 1) * self._batch_size]
            
            try:
                request_params = StockBarsRequest(
                    symbol_or_symbols=current_tickers,
                    timeframe=time_frame_mapping[time_frame], 
                    start=start_date,
                    end=end_date
                )

                bars = self._stock_client.get_stock_bars(request_params)
                
                if bars.df is not None:
                    new_data = bars.df.reset_index()
                    checkpoint_dfs.append(new_data)
                    print(f" Success: {len(new_data)} rows.")
                
                time.sleep(0.5) 

            except Exception as e:
                print(f" -> Error in batch {i+1}: {e}")
                continue

            if (i + 1) % 10 == 0:
                temp_df = pd.concat(checkpoint_dfs)
                temp_df.to_parquet(f'{self._output_dir}/stocks_checkpoint_{i+1}.parquet', engine='pyarrow', compression='snappy')
                print(f"!!! Checkpoint saved at batch {i+1}")
            
                del temp_df
                checkpoint_dfs = []
                gc.collect()
                
        if checkpoint_dfs:
            final_df = pd.concat(checkpoint_dfs).drop_duplicates()
            final_df.to_parquet(f'{self._output_dir}/stocks_checkpoint_rest.parquet', engine='pyarrow', compression='snappy')
            print("\n===== ALL DONE! Final file saved. =====")


    def merge_batched_data(self) -> None:
        
        print("\n===== STARTED MERGING DATA =====")
        try:
            output_name = f'{self._output_dir}/merged_data.parquet'
            file_list = [ f for f in os.listdir(self._output_dir) if f.endswith('.parquet')]
            print(f" {len(file_list)} files to merge!")
            df_final = pd.concat([pd.read_parquet(f'{self._output_dir}/{f}') for f in file_list])
            df_final.drop_duplicates(inplace=True)
            df_final.reset_index(inplace=True)
            df_final.drop('index', axis=1, inplace=True)
            df_final.to_parquet(output_name, compression='snappy')
            print(f" Merging finished, you can see the data here: {output_name}")

        except Exception as e:
            print(e)
        


