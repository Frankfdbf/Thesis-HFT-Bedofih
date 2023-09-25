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

    Returns
    -------
    None.

    Creates one file per ISIN and per day with 4 columns:
    o_dtm_mo, o_id_fd, o_cha_id, o_state.
    """
    for isin in tqdm(STOCKS.all):
        for file in tqdm(os.listdir(os.path.join(PATHS['orders'], isin))):
            # Paths
            #date = file[-12:-4]
            date = file[-16:-8]

            output_filename = f'cancelledOrders_{isin}_{date}.parquet'
            origin = os.path.join(PATHS['orders'], isin, file)
            destination = os.path.join(PATHS['cancelled_orders'], isin, output_filename)

            # Read file
            #df = read_processed_orders(origin)
            df = pd.read_parquet(origin)

            # Define the valid states
            valid_states = ['S', '4', 'C', 'P']

            # Specify the columns you need
            columns_to_select = ['o_dtm_mo', 'o_id_fd', 'o_cha_id', 'o_state']

            # Create df with only cancelled orders
            cancelled_orders = df[df.o_state.isin(valid_states)].copy()[columns_to_select]

            # Save file
            cancelled_orders.to_parquet(destination, index=False)
        break

def concatenate_by_isin():
    """
    Create files with cancelled orders obtained from the order files, 
    concatenated by isin.
    Add why we need to have them ...

    Returns
    -------
    None.

    Creates one file per ISIN  with 4 columns:
    o_dtm_mo, o_id_fd, o_cha_id, o_state.
    """
    for isin in tqdm(STOCKS.all):
        # Create main dataframe per isin
        total_df = pd.DataFrame(columns=['o_dtm_mo', 'o_id_fd', 'o_cha_id', 'o_state'])

        # Concatenate dataframe
        for file in os.listdir(os.path.join(PATHS['cancelled_orders'], isin)):
            origin = os.path.join(PATHS['cancelled_orders'], isin, file)
            #daily_df = pd.read_csv(origin)
            daily_df = pd.read_parquet(origin)
            total_df = pd.concat([total_df, daily_df], ignore_index=True)

        total_df.sort_values(by=['o_dtm_mo', 'o_id_fd'], inplace=True)

        """
        total_df_grouped = total_df.groupby('o_id_fd')
        week_grouped = df.groupby('week')
        week_grouped.sum().reset_index().to_csv('week_grouped.csv')
        """

        # Save file
        output_filename = f'cancelledOrders_{isin}.parquet'
        destination = os.path.join(PATHS['cancelled_orders'], isin, output_filename)
        total_df.to_parquet(destination, index=False)

        break


if __name__ == '__main__':
    # Extract cancelled orders
    print('Getting cancelled orders ...')
    get_cancelled_orders()

    # Concatenate by isin
    print('Concatenating by isin ...')
    concatenate_by_isin()
