from data_batching import DataBatcher
from alpaca_keys import ALPACA_API_KEY, ALPACA_SECRET_KEY

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass

from datetime import datetime



client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

# Get all tickers
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
assets = client.get_all_assets(search_params)
all_tickers = [asset.symbol for asset in assets if asset.tradable]

# Get batched data
db = DataBatcher(all_tickers, batch_size=50, output_dir='data')
db.batch_tickers(time_frame='day', start_date=datetime(2023,1,1))
db.merge_batched_data() 