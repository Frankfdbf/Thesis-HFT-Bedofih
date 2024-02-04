# Import Built-Ins
import os

# Import Third-Party
import pandas as pd

# Import Homebrew


class Stocks:
    """
    Class object to store the stocks that will be used for analysis and their
    different classification.
    """
    
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                                 'indices_constituents_list.xlsx')
        # Read Excel file
        self.data = pd.read_excel(self.path, sheet_name='All Tickers SBF 2017')

        self.all = list(self.data[self.data.included == True].isin_id)
        self.enough_trades = list(self.data[(self.data.included == True) & (self.data.trades_min_num_rows > 400)].isin_id)
        self.category_A = None      # Stocks in category A
        self.category_B = None      # Stocks in category B
        self.category_C = None      # Stocks in category C
        self.llp = None             # Stocks in llp program
        self.not_llp = None         # Stocks not in llp program
