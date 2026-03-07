import pandas as pd
import numpy as np
import os


from alpaca_keys import ALPACA_API_KEY, ALPACA_SECRET_KEY

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus, AssetClass

import random


client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

# Get all tickers
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
assets = client.get_all_assets(search_params)
all_tickers = [asset.symbol for asset in assets if asset.tradable]



def _get_portfolio(portfolio_size) -> dict: 

    current_tickers = random.sample(all_tickers, portfolio_size)
    weights = np.random.rand(portfolio_size)

    normalized_weights = weights / weights.sum()
    return dict(zip(current_tickers,normalized_weights))


def get_population(pop_size = 400) -> list:

    population = [
                _get_portfolio(random.randint(3,20)) 
                for _ in range(pop_size)
                ]

    return population


def evaluate_portfolios(generation):
    pass

def get_portfolio_score(portfolio):
    pass

def get_parents():
    pass
    

