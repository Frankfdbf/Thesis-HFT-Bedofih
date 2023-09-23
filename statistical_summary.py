import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
from multiprocessing import Pool, Process

# Import Homebrew
from utilities.functions import timeit, preprocess_trade_file, get_sbf_120_list

class StatisticalSummary:
    """
    Class that will provide methods to generate statistical summaries of the data
    """
    def __init__(self, origin):
        self.origin = origin
        self.sbf = get_sbf_120_list()

    def create_daily_time_series(self):
        """
        Create a folder to store the files.
        Creates a file for each stock with
        - closing price
        - the returns
        Saves the file as csv.
        """
        # Create main folder
        if 'Daily Time Series' not in os.listdir(self.origin):
            os.mkdir(os.path.join(self.origin, 'Daily Time Series'))

        # For each stock
        for isin in tqdm(self.sbf):
            # Create dataframe
            time_series = pd.DataFrame(columns=['Date', 'Price Close'])
            
            # For each file
            trade_files = os.listdir(os.path.join(self.origin, 'BEDOFIH 2017 Structured', 'Trades', isin))
            for file in trade_files:
                path = os.path.join(self.origin, 'BEDOFIH 2017 Structured', 'Trades', isin, file)
                data = preprocess_trade_file(path)

                # Get the date and the price
                date = pd.to_datetime(file[-12:-4], format="%Y%m%d").date()
                if data.empty == True:
                    # If there were no trade, close price is close the day before
                    price = [time_series.iloc[-1, -1]]
                else:
                    price = [data.iloc[-1, data.columns.get_loc('t_price')]]
                # Format the row to append
                row = pd.DataFrame({
                    #'Date': [data.iloc[-1, -1].date()],
                    'Date': date,
                    'Price Close': price
                })
                # Append the rows
                time_series = time_series.append(row)
            
            # Calculate returns
            time_series['Returns'] = time_series['Price Close'].pct_change()
            # Save file
            time_series.set_index('Date', inplace=True)
            output_path = os.path.join(self.origin, 'Daily Time Series', isin + '_time_series_2017.csv')
            time_series.to_csv(output_path)
            del time_series
            time.sleep(3)
            


            
            
        


    def statistics_agressiveness(self):
        """
        Return a dataframe object
        columns:
        trade value average, trade volume average
        
        """
        pass

if __name__ == '__main__':
    ss = StatisticalSummary(origin='/Volumes/Extreme ssd')
    #print(ss.sbf)
    ss.create_daily_time_series()