# Import Built-Ins
import os

# Import Third-Party
from tqdm import tqdm
import pandas as pd

# Import Homebrew
from preprocessing import read_processed_orders
from constants.constants import STOCKS, PATHS


def get_cancelled_orders():
    """
    Create files with cancelled orders obtained from the order files.
    Add why we need to have them ...

    Parameters
    ----------
    x: 

    Returns
    -------
    None.

    Creates one file per ISIN and per day with 4 columns:
    - o_id_fd
    - o_cha_id
    - o_dtm_mo
    - o_state
    """
    for isin in tqdm(STOCKS.all):
        for file in os.listdir(os.path.join(PATHS['orders'], isin)):
            # Paths
            date = file[-12:-4]
            output_filename = f'cancelledOrders_{isin}_{date}.csv'
            origin = os.path.join(PATHS['orders'], isin, file)
            destination = os.path.join(PATHS['cancelled_orders'], isin, output_filename)

            # Create df with only cancelled orders
            df = read_processed_orders(origin)
            cancelled_orders = df[
                (df.o_state == 'S') | 
                (df.o_state == '4') | 
                (df.o_state == 'C') | 
                (df.o_state == 'P')
                ].copy()
            cancelled_orders = cancelled_orders[['o_dtm_mo', 'o_id_fd', 'o_cha_id', 'o_state']]

            # Save file
            cancelled_orders.to_csv(destination, index=False)


def concatenate_by_isin():
    """
    Create files with cancelled orders obtained from the order files, 
    concatenated by isin.
    Add why we need to have them ...

    Parameters
    ----------
    x: 

    Returns
    -------
    None.

    Creates one file per ISIN  with 4 columns:
    - o_id_fd
    - o_cha_id
    - o_dtm_mo
    - o_state
    """
    for isin in tqdm(STOCKS.all):
        # Create main dataframe per isin
        total_df = pd.DataFrame(columns=['o_dtm_mo', 'o_id_fd', 'o_cha_id', 'o_state'])

        # Concatenate dataframe
        for file in os.listdir(os.path.join(PATHS['cancelled_orders'], isin)):
            origin = os.path.join(PATHS['cancelled_orders'], isin, file)
            daily_df = pd.read_csv(origin)
            total_df = pd.concat([total_df, daily_df], ignore_index=True)

        total_df.sort_values(by=['o_dtm_mo', 'o_id_fd'], inplace=True)

        """
        total_df_grouped = total_df.groupby('o_id_fd')
        week_grouped = df.groupby('week')
        week_grouped.sum().reset_index().to_csv('week_grouped.csv')
        """

        # Save file
        output_filename = f'cancelledOrders_{isin}.csv'
        destination = os.path.join(PATHS['cancelled_orders'], isin, output_filename)
        total_df.to_csv(destination, index=False)


if __name__ == '__main__':
    # Extract cancelled orders
    print('Getting cancelled orders ...')
    get_cancelled_orders()

    # Concatenate by isin
    print('Concatenating by isin ...')
    concatenate_by_isin()
