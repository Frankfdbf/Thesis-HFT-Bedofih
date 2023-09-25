# Import Built-Ins

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew
from utils.other_utils import check_empty_csv
from utils.time_utils import timeit



def preprocess_trades(path):
    """ Preprocessing of the trade file.
    The function will transform the data into a usable database.
    The data can either be exported as csv or return as a pandas database.

    Parameters
    ----------
    :param path : string
        Path of the csv trade file
    
    Returns
    -------
    value : pd.DataFrame
        New formatted trade file
    """

    # Specify headers that will be used in the clean database.
    columns = [
        't_seq',
        't_capital',
        't_price',
        't_price_max',
        't_price_min',
        't_d_b_en',
        't_t_b_en',
        't_d_s_en',
        't_t_s_en',
        't_d_neg',
        't_t_neg',
        't_m_neg',
        't_currency',
        't_cd_gc',
        't_id_b_fd',
        't_id_s_fd',
        't_id_u_fd',
        't_undo',
        't_app',
        't_isin',
        't_origin',
        't_b_sq_nb',
        't_s_sq_nb',
        't_b_account',
        't_s_account',
        't_cd_pc',
        't_q_exchanged',
        't_tr_nb',
        't_id_tr',
        't_agg',
        't_yield',
        't_spread',
        't_b_type',
        't_s_type',
    ]

    dtypes = {
        't_seq': 'int32',
        't_capital': 'float64',
        't_price': 'float64',
        't_price_max': 'float',
        't_price_min': 'float',
        't_d_b_en': 'int32',
        't_t_b_en': 'string',
        't_d_s_en': 'int32',
        't_t_s_en': 'string',
        't_d_neg': 'string',
        't_t_neg': 'string',
        't_m_neg': 'int32',
        't_currency': 'category',
        't_cd_gc': 'category',
        't_id_b_fd': 'int64',
        't_id_s_fd': 'int64',
        't_id_u_fd': 'float',
        't_undo': 'category',
        't_app': 'category',
        't_isin': 'category',
        't_origin': 'category',
        't_b_sq_nb': 'int32',
        't_s_sq_nb': 'int32',
        't_b_account': 'category',
        't_s_account': 'category',
        't_cd_pc': 'category',
        't_q_exchanged': 'int32',
        't_tr_nb': 'int32',
        't_id_tr': 'int64',
        't_agg': 'category',
        't_yield': 'float',
        't_spread': 'float',
        't_b_type': 'category',
        't_s_type': 'category',
    }

    # Read data with headers this time.
    data = pd.read_csv(path, names=columns, dtype=dtypes)
    

    # Handle data if file is empty
    if check_empty_csv(data, path):
        return data


    # Create time columns
    data['t_d_b_en'] = pd.to_datetime(data['t_d_b_en'], format='%Y%m%d')
    data['t_d_s_en'] = pd.to_datetime(data['t_d_s_en'], format='%Y%m%d')
    data['t_dtm_neg'] = pd.to_datetime(data[f't_d_neg'] + ' ' + data[f't_t_neg'], format='%Y%m%d %H:%M:%S') + pd.to_timedelta(data[f't_m_neg'], unit='us')

    #Column drops
    data.drop(columns=[
        't_seq',                        # AMF internal sequencial number
        't_price_max',                  # Always empty
        't_price_min',                  # Always empty
        't_t_b_en',                     # Always 00:00:00
        't_t_s_en',                     # Always 00:00:00
        't_currency',                   # Always EUR
        't_cd_gc',                      # Unsure
        't_undo',                       # Unsure
        't_id_u_fd',                    # Unsure
        't_app',                        # Unsure: besoin de filtrer les trades avec cet indicateur ? Enlever les trades où une "application" survient ?
        't_isin',                       # Already have it in file name
        't_origin',                     # Unsure Origin of message (opening trade or rest of session)
        't_cd_pc',                      # Always 025 (Paris)
        't_yield', 't_spread',          # For bonds
        't_d_neg', 't_t_neg', 't_m_neg',# Ex time columns
    ], inplace=True)

    return data


def read_processed_trades(path):
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