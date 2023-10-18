# Import Built-Ins
import os

# Import Third-Party
import pandas as pd
from tqdm import tqdm

# Import Homebrew
from constants.constants import STOCKS, PATHS, CLOSING_AUCTION_CUTOFF
from utils.time_utils import timeit


@timeit
def get_auctions():
    """
    Get auction time and price for each day and isin.
    Save it all to one file. (Can be saved to csv for easier readability).

    Returns
    -------
    None.
    Save file with auction data.
    """

    columns = [
        'isin', 
        'date', 
        'auct_open_datetime', 
        'auct_open_price', 
        'auct_close_datetime', 
        'auct_close_price'
    ]

    df_all_auctions = pd.DataFrame(columns=columns)

    for isin in tqdm(STOCKS.all):
        for file in os.listdir(os.path.join(PATHS['trades'], isin)):
            
            # Read trade file
            origin = os.path.join(PATHS['trades'], isin, file)
            trades = pd.read_parquet(origin)
            
            # First trades occurs just after auction 
            auction_open_datetime = trades.iloc[0].t_dtm_neg
            auction_open_price   = trades.iloc[0].t_price
            
            # Second auction is the first time after 17:35:00
            auction_trades  = trades[trades.t_dtm_neg.dt.time > CLOSING_AUCTION_CUTOFF]
            
            if len(auction_trades) != 0:
                auction_close_datetime = auction_trades.iloc[0].t_dtm_neg
                auction_close_price   = auction_trades.iloc[0].t_price
            else:
                auction_close_datetime = None
                auction_close_price   = None 

            # Create row to append
            df_row = pd.DataFrame(
                {
                    'isin': [isin],
                    'date': [trades.iloc[0].t_dtm_neg.date()],
                    'auct_open_datetime': [auction_open_datetime],
                    'auct_open_price': [auction_open_price],
                    'auct_close_datetime': [auction_close_datetime],
                    'auct_close_price': [auction_close_price]
                }
            )
            
            # Append row
            df_all_auctions = pd.concat([df_all_auctions, df_row], ignore_index=True)

    # Save file
    df_all_auctions.to_parquet(os.path.join(PATHS['root'], 'auctions.parquet'), index=False)

if __name__ == '__main__':
    # Get auctions 
    print('Getting auctions ...')
    get_auctions()