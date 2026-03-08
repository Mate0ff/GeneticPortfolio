import pandas as pd
import numpy as np

import random




class GeneticPortfolio():

    def __init__(self, data):
        self._tickers = self.__extract_tickers(data) 
        self._data = self.__calculate_returns(data)

    @staticmethod
    def __extract_tickers(data):
        return list(data['symbol'].unique())
    
    @staticmethod
    def __calculate_returns(data):
        data['returns'] = data.groupby('symbol')['close'].pct_change().fillna(0)
        return data

    def get_data(self):
        return self._data
    
    def get_tickers(self):
        return self._tickers

    def _get_portfolio(self, portfolio_size) -> dict: 

        current_tickers = random.sample(self._tickers, portfolio_size)
        weights = np.random.rand(portfolio_size)

        normalized_weights = weights / weights.sum()
        return dict(zip(current_tickers,normalized_weights))

    def get_population(self, pop_size = 400) -> list:

        population = [
                    self._get_portfolio(random.randint(3,20)) 
                    for _ in range(pop_size)
                    ]

        return population

    def get_returns(self):

        for i in self._tickers:
            ticker_data = self._data[self._data['symbol'] == i]
            ticker_data = ticker_data['close']
            ticker_data.pct_change()
            
    def evaluate_portfolios(generation):
        # sharpe ratio for now 
        pass

    def get_portfolio_score(portfolio):
        pass

    def get_parents():
        pass
    

