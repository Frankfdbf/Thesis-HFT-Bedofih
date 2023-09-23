# Import Built-Ins
import os

# Import Third-Party
import pandas as pd

# Import Homebrew


class Stocks:
    """
    Class object to store the stocks that will be used for analysis and their
    different classification
    """
    
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                                 'indices_constituents_list.xlsx')
        #self.path = os.path.join(os.getcwd(), 'indices_constituents_list.xlsx')
        self.data = pd.read_excel(self.path, sheet_name='All Tickers SBF 2017')

        self.all = list(self.data[self.data.included == True].isin_id)
        self.category_A = None
        self.category_B = None
        self.category_C = None
        self.llp = None
        self.not_llp = None