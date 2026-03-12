from dataclasses import dataclass, field
import numpy as np
@dataclass
class Portfolio:
    tickers:list = field(default_factory=list)
    weights:np.array = field(default_factory=np.array)
    score:float=0


    def __repr__(self):
        lines = []
        for i, (ticker, weight) in enumerate(zip(self.tickers, self.weights)):
            entry = f"{ticker} ({weight:.2%})" 
            
            if i > 0 and i % 3 == 0:
                lines.append(f"\n{entry}")
            else:
                lines.append(entry)
                
        return " | ".join(lines)