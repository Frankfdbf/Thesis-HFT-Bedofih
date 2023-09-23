# Import Built-Ins
import os
import re
import time

# Import Third-Party
from tqdm import tqdm

# Import Homebrew
from preprocessing import preprocess_trades, preprocess_orders, preprocess_events
from constants.constants import STOCKS, PATHS, MONTHS_STR


def create_isin_folder_structure(name, path):
    """
    Create folder structure better than original. Same files as raw, only
    for chosen isins.

    params:
    name: str, name of the new folder. eg: 'trades'
    path: str, path to the new folder. eg: 'root/trades/'
    """

    # Main folder
    if name not in os.listdir(PATHS['root']):
        os.mkdir(path)
    
    # Sub folders (isins)
    for isin in STOCKS.all:
        if isin not in os.listdir(path):
            os.mkdir(os.path.join(path, isin))


def create_single_folder(name, path):
    """
    Create a new folder.

    params:
    name: str, name of the new folder. eg: 'trades'
    path: str, path to the new folder. eg: 'root/trades/'
    """

    # Main folder
    if name not in os.listdir(PATHS['root']):
        os.mkdir(path)


def reorganize_data():
        """
        Copy and formats necessary files from raw structure to the organised one.

        params
        name: str, name of the new folder. eg: 'trades'
        path: str, path to the new folder. eg: 'root/trades/'
        """
        
        # For each month
        for month in tqdm(MONTHS_STR):
            dates = [date for date in os.listdir(os.path.join(PATHS['raw'], month)) if os.path.isdir(os.path.join(PATHS['raw'], month, date))]
            # For each trading day during the month
            for date in dates:
                isin_groups = [dir for dir in os.listdir(os.path.join(PATHS['raw'], month, date)) if os.path.isdir(os.path.join(PATHS['raw'], month, date, dir))]        
                
                # Event file
                event_file = [file for file in os.listdir(os.path.join(PATHS['raw'], month, date)) if file[-4:] == '.csv'][0]
                origin_path = os.path.join(PATHS['raw'], month, date, event_file)
                destination_path = os.path.join(PATHS['root'], 'events', event_file)
                df = preprocess_events(origin_path)
                df.to_csv(destination_path, index=False)
                del df
                
                # For each isin group
                for isin_group in isin_groups:
                    isins = os.listdir(os.path.join(PATHS['raw'], month, date, isin_group))
                    # For each isin
                    for isin in isins:
                        # Check if isin is in the list
                        if isin in STOCKS.all:
                            files = os.listdir(os.path.join(PATHS['raw'], month, date, isin_group, isin))
                            # For each file
                            for file in files:
                                
                                # Order files
                                if re.match(pattern='^VHOX_.*', string=file):
                                    origin_path = os.path.join(PATHS['raw'], month, date, isin_group, isin, file)
                                    destination_path = os.path.join(PATHS['root'], 'orders', isin, file)
                                    df = preprocess_orders(origin_path)
                                    df.to_csv(destination_path, index=False)
                                    del df
                                
                                # Trade files
                                elif re.match(pattern='^VHD_.*', string=file):
                                    origin_path = os.path.join(PATHS['raw'], month, date, isin_group, isin, file)
                                    destination_path = os.path.join(PATHS['root'], 'trades', isin, file)
                                    df = preprocess_trades(origin_path)
                                    df.to_csv(destination_path, index=False)
                                    del df

                                # History files
                                elif re.match(pattern='^VHOXhistory.*', string=file):
                                    origin_path = os.path.join(PATHS['raw'], month, date, isin_group, isin, file)
                                    destination_path = os.path.join(PATHS['root'], 'histories', isin, file)
                                    df = preprocess_orders(origin_path)
                                    df.to_csv(destination_path, index=False)
                                    del df
                print('Over.')
                time.sleep(20)
        

if __name__ == '__main__':
    # Create structure
    print('Creating folder structure ...')
    for key, value in PATHS.items():
        if key in ('root', 'raw'):
            continue
        elif key in ('orders', 'trades', 'histories', 'cancelled_orders'): 
            create_isin_folder_structure(name=key, path=value)
        else:
            create_single_folder(name=key, path=value)

    # Format and reorganise data
    print('Reorganizing data files ...')
    reorganize_data()