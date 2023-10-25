# Import Built-Ins
import os

# Import Third-Party
from tqdm import tqdm
import pandas as pd
from pathlib import Path

# Import Homebrew
from constants.constants import STOCKS, PATHS


def get_removed_orders():
    """
    Create files with removed orders obtained from the order files.
    Messages that indicate removal, actually carry other pieces of information. 
    The removal is only the next action however there are no messages created for 
    order removals. 
    (Think about the order state info being one step early each message).
    Thus, we need to keep track of all orders removed from the 
    book, for what reason they were removed (trade, cancelation) and the time at
    which it happened. We need this information to accuratly remove them from 
    the order book.
    Note that we need to go through the history and the order file for each day.
    This is because if an order is not updated during the day (not in order file)
    and is executed during that day, the information is only obtainable in the 
    history file when that order is re-introduced.

    Returns
    -------
    None.

    Creates one file per ISIN and per day with 4 columns:
    o_dtm_mo, o_id_fd, o_cha_id, o_state.
    """

    for isin in tqdm(STOCKS.all):
        for file in tqdm(os.listdir(os.path.join(PATHS['orders'], isin))):
            # Date of the file
            date = file[-16:-8]

            # Paths
            origin_orders = os.path.join(PATHS['orders'], isin, file)
            origin_history = os.path.join(PATHS['histories'], isin, f'VHOXhistory_{isin}_{date}.parquet')
            output_filename = f'removedOrders_{isin}_{date}.parquet'
            destination = os.path.join(PATHS['removed_orders'], isin, output_filename)

            # Read files and merge history and orders
            df_orders = pd.read_parquet(origin_orders)
            try:
                df_history = pd.read_parquet(origin_history)
                df = pd.concat([df_history, df_orders])
            except FileNotFoundError:
                df = df_orders
                p = Path(origin_history)
                print(f'File: \'{p.stem}\' is empty (path: \'{p.parent}\').')
            
            # Specify the columns needed and the unvalid states (everything except order removal)
            unvalid_states = ['0', '1', '5']
            columns_to_select = ['o_dtm_br', 'o_id_fd', 'o_bs', 'o_state', 'o_account', 'o_member', 'o_nb_tr']

            # Create df with only removed orders
            removed_orders = df.drop_duplicates(subset=['o_id_fd'], keep='last')[columns_to_select]
            removed_orders = removed_orders[~removed_orders.o_state.isin(unvalid_states)]

            # Save file
            removed_orders.to_parquet(destination, index=False)
        #break # Limit to one isin (for testing only)


if __name__ == '__main__':
    print('Getting removed orders ...')
    get_removed_orders()