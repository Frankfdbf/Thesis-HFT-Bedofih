# Import Built-Ins
import os

# Import Third-Party
import pandas as pd
from tqdm import tqdm

# Import Homebrew
from preprocessing import read_processed_trades
from constants.constants import STOCKS, PATHS, CLOSING_AUCTION_CUTOFF


def get_auctions():
    """
    Get auction time for a specific day and isin.

    Returns
    -------
    None.
    Save file with auction data.
    """
    df_all_auctions = pd.DataFrame(columns=[
        'isin', 'date', 
        'auct_open_time', 'auct_open_price', 
        'auct_close_time', 'auct_close_price'
        ])

    for isin in tqdm(STOCKS.all):
        for file in os.listdir(os.path.join(PATHS['trades'], isin)):
            origin = os.path.join(PATHS['trades'], isin, file)
            trades = read_processed_trades(origin)
            
            # first trades occurs just after auction 
            auct_open_time = trades.iloc[0].t_dtm_neg.time()
            auct_open_price   = trades.iloc[0].t_price
            
            # second auction is the first time after 17:35:00
            auction_trades  = trades[trades.t_dtm_neg.dt.time > CLOSING_AUCTION_CUTOFF]
            
            if not auction_trades.empty:
                auct_close_time = trades.iloc[0].t_dtm_neg.time()
                auct_close_price   = auction_trades.iloc[0].t_price
            else:
                auct_close_time = None
                auct_close_price   = None 

            # Create row to append
            df_row = pd.DataFrame(
                {
                    'isin': isin,
                    'date': trades.iloc[0].t_dtm_neg.date(),
                    'auct_open_time': auct_open_time,
                    'auct_open_price': auct_open_price,
                    'auct_close_time': auct_close_time,
                    'auct_close_price': auct_close_price
                }
            )
            # Append row
            df_all_auctions = pd.concat([df_all_auctions, df_row], ignore_index=True)

    # Save file
    df_all_auctions.to_csv(os.path.join(PATHS['root'], 'auctions.csv'))


if __name__ == '__main__':
    # Get auctions 
    print('Getting auctions ...')
    get_auctions()