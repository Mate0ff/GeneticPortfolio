import pandas as pd
import numpy as np
import os
import random


class GeneticPortfolio():

    def __init__(self, data):
        self._tickers = self.__extract_tickers(data) 
        self._data = self.__calculate_returns(data)
        self._cov_matrix = self.__get_cov_cache(self._data)
        self._symbol_means = self.__calculate_mean_returns(self._data)
        self._population_size = None

    @staticmethod
    def __extract_tickers(data):
        return list(data['symbol'].unique())
    
    @staticmethod
    def __calculate_returns(data):
        df = data.copy()
        df['returns'] = df.groupby('symbol')['close'].pct_change().fillna(0)
        return df
    
    @staticmethod
    def __calculate_cov_matrix(data):
        returns_matrix = data.pivot(index='timestamp', columns='symbol', values='returns')
        return returns_matrix.cov()

    def __get_cov_cache(self, data):
        cache_path = 'cov_matrix.parquet'

        if os.path.exists(cache_path):
            return pd.read_parquet(cache_path)
        else: 
            cov_matrix = self.__calculate_cov_matrix(data)
            cov_matrix.to_parquet(cache_path, engine='pyarrow', compression='snappy')
            return cov_matrix

    @staticmethod
    def __calculate_mean_returns(data):
        return data.groupby('symbol')['returns'].mean()
    
    def get_data(self):
        return self._data
    
    def get_tickers(self):
        return self._tickers

    def __get_portfolio(self, portfolio_size) -> dict: 

        current_tickers = random.sample(self._tickers, portfolio_size)
        weights = np.random.rand(portfolio_size)

        normalized_weights = weights / weights.sum()
        return dict(zip(current_tickers,normalized_weights))

    def get_population(self, pop_size = 400) -> list:
        self._population_size = pop_size
        population = [
                    self.__get_portfolio(random.randint(3,20)) 
                    for _ in range(self._population_size)
                    ]

        return population
            
    def evaluate_portfolios(self,generation):
        fitnes = []
        
        for p in generation:
            
            symbols = list(p.keys())
            weights = np.array(list(p.values()))

            portfolio_return = np.sum(self._symbol_means[symbols] * weights) 
  
            portfolio_cov = self._cov_matrix.loc[symbols, symbols].values
            portfolio_variance = np.dot(weights.T, np.dot(portfolio_cov, weights))
            volatility = np.sqrt(portfolio_variance) if portfolio_variance > 0 else 1e-9

            fitnes.append(portfolio_return/volatility)

        return np.array(fitnes)

    def get_next_gen(self,generation,fitnes):
        
        # top 10 go to next gen
        best_portfolios_idx = np.argpartition(fitnes,-10)[-10:]
        portfolios = [generation[i] for i in best_portfolios_idx] 

        # torunament
        for _ in range(self._population_size):
            p1, p2 = self.__get_parents(generation, fitnes)
            c1, c2 = self.__cros_breed(p1, p2)

            c1 = self.__mutate(c1, chance = 0.02)
            c2 = self.__mutate(c2, chance = 0.02)
            portfolios.append(c1)
            portfolios.append(c2)

        return portfolios 

    def __cros_breed(self, p1, p2):
        choices = list(set(p1.keys()) | set(p2.keys()))
        avg_size = (len(p1) + len(p2)) / 2
        child_size = int(np.clip(round(avg_size) + + random.randint(-1, 1), 3, 20))
        child_size = min(child_size, len(choices))

        def create_one_child():
            tickers = np.random.choice(choices, size=child_size, replace=False)
            w_values = np.zeros(len(tickers))
            
            for i, t in enumerate(tickers):
                if t in p1 and t in p2:
                    w_values[i] = (p1[t] + p2[t]) / 2.0
                elif t in p1:
                    w_values[i] = p1[t]
                else:
                    w_values[i] = p2[t]

            sum_w = np.sum(w_values)
            if sum_w > 0:
                w_values /= sum_w
            else:
                w_values = np.ones(len(tickers)) / len(tickers)
                
            return dict(zip(tickers, w_values))

        c1 = create_one_child()
        c2 = create_one_child()

        return c1, c2
    
    def __mutate(self,child,chance = 0.02):

        mutate = ( random.random() ) <= chance
        if mutate:
            mutation = random.randint(0,1)
                # random ticker is replaced
            if mutation == 0:
                replace = np.random.choice(list(child.keys()))
                rand_ticker = np.random.choice(self._tickers)

                while rand_ticker in child:
                    rand_ticker = np.random.choice(self._tickers)
                
                child[rand_ticker] = child.pop(replace)
            elif mutation == 1:
                # random weight is altered
                t = list(child.keys())
                w = np.array(list(child.values()))

                index = random.randrange(len(w))
                w[index] = np.clip(w[index] + random.uniform(-0.1, 0.1), 0.01, 1.0)

                total_w = np.sum(w)
                if total_w > 0:
                    w /= total_w
                else:
                    w = np.ones(len(w)) / len(w)

                child.update(zip(t, w))

        return child
    
    def __get_parents(self, generation, fitnes, n_turnament=10):
        parents = []
        
        while len(parents) < 2:
            possible_portfolios = np.random.choice(self._population_size, n_turnament, replace=False)
            
            fitnes_scores = fitnes[possible_portfolios]
            best_one_idx = possible_portfolios[np.argmax(fitnes_scores)]

            if best_one_idx not in parents:
                parents.append(best_one_idx)
                
        return generation[parents[0]], generation[parents[1]]


    def run_ga_optimization(self, n):
        pass