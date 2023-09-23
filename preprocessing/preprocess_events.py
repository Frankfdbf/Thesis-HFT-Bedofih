# Import Built-Ins

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew
from utils.other_utils import check_empty_csv


def preprocess_events(path):
    """ Preprocessing of the event file.
    The function will transform the data into a usable database.
    The data is returned as a pandas database.

    Parameters
    ----------
    :param path : string
        Path of the csv order update file

    
    Returns
    -------
    value : pd.DataFrame
        New formatted order update file
    """

    # Handle data if file is empty
    if check_empty_csv(path):
        return pd.read_csv(path)

    columns = [
        'e_seq',
        'e_act_m_state',
        'e_d_upd',
        'e_d_me',
        'e_t_me',
        'e_d_suspension',
        'e_t_suspension',
        'e_ct_state',
        'e_value_state',
        'e_cd_gc',
        'e_t_op',
        'e_reservation',
        'e_isin',
        'e_cd_pc'
    ]

    data = pd.read_csv(path, names=columns)

    #Column drops
    data.drop(columns=[
        'e_d_suspension', 
        'e_t_suspension', 
        'e_ct_state', 
        'e_cd_pc'],
        inplace=True)

    return data


def read_processed_events(path):
    """ Function to read processed file.
    Add specific actions here.

    Parameters
    ----------
    :param path : string
        Path of the csv trade file
    
    Returns
    -------
    value : pd.DataFrame
        New formatted trade file
    """
    
    df = pd.read_csv(path)
    if df.empty:
        print(f"File: \"{Path(path).stem}\" is empty")
    return df