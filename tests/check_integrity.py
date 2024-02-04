# Import Built-Ins
import os

# Import Third-Party
from tqdm import tqdm
import pandas as pd
from pathlib import Path

# Import Homebrew
from src.constants.constants import STOCKS, PATHS, DATES


def check_integrity() -> None:
    """
    Checks if files are missing or are empty.
    """

    missing_files = []
    empty_files = []

    for isin in tqdm(STOCKS.all):
        dates = DATES.all
        dates_str = [item.strftime('%Y%m%d') for item in dates]

        for file in tqdm(os.listdir(os.path.join(PATHS['orders'], isin))):
            # Date of the file (remove it from list)
            date = file[-16:-8]
            dates_str.remove(date)

            # Paths
            origin_history = os.path.join(PATHS['histories'], isin, f'VHOXhistory_{isin}_{date}.parquet')
            origin_trades = os.path.join(PATHS['trades'], isin, f'VHD_{isin}_{date}.parquet')

            _check_single_file(origin_history, missing_files, empty_files)
            _check_single_file(origin_trades, missing_files, empty_files)

        # Dates left in list are missing files
        for date_str in dates_str:
            origin_orders = os.path.join(PATHS['orders'], isin, f'VHOX_{isin}_{date_str}.parquet')
            missing_files.append(origin_orders)


    df_missing = pd.DataFrame(missing_files, columns=['missing_files'])
    df_empty = pd.DataFrame(empty_files, columns=['empty_files'])
    
    df_missing.to_csv(os.path.join(PATHS['root'], 'files_missing.csv'))
    df_empty.to_csv(os.path.join(PATHS['root'], 'files_empty.csv'))


def _check_single_file(path: str, missing_files: list, empty_files: list) -> None:
    """
    Checks one file.
    """
    p = Path(path)

    try:
        df = pd.read_parquet(path)
        if len(df) == 0:
            empty_files.append(path)

    except FileNotFoundError:
        missing_files.append(path)


if __name__ == '__main__':
    print('Checking integrity ...')
    check_integrity()