# Import Built-Ins

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew
from utils.other_utils import check_empty_csv


def preprocess_orders(path):
    """ Preprocessing of the order update file.
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
        'o_seq',
        'o_isin',
        'o_d_i',
        'o_t_i',
        'o_cha_id',
        'o_id_fd',
        'o_d_be',
        'o_t_be',
        'o_m_be',
        'o_d_br',
        'o_t_br',
        'o_m_br',
        'o_d_va',
        'o_t_va',
        'o_m_va',
        'o_d_mo',
        'o_t_mo',
        'o_m_mo',
        'o_d_en',
        'o_t_en',
        'o_sq_nb',
        'o_sq_nbm',
        'o_d_p',
        'o_t_p',
        'o_m_p',
        'o_state',
        'o_currency',
        'o_bs',
        'o_type',
        'o_execution',
        'o_validity',
        'o_d_expiration',
        'o_t_expiration',
        'o_price',
        'o_price_sto p',
        'o_price_dfp g',
        'o_disoff',
        'o_q_ini',
        'o_q_min',
        'o_q_dis',
        'o_q_neg',
        'o_app',
        'o_origin',
        'o_account',
        'o_nb_tr',
        'o_q_rem',
        'o_d_upd',
        'o_t_upd',
        'o_member',
    ]

    data = pd.read_csv(path, 
                       names=columns, 
                       dtype={'o_d_br': str,
                              'o_t_br': str,
                              'o_state': str, 
                              'o_type': str,
                              'o_execution': str})

    # Converting time columns
    #data['o_d_en'] = pd.to_datetime(data['o_d_i'], format='%Y%m%d')
    
    # Creating time columns
    time_suffix = ['be', 'br', 'va']
    time_suffix_with_nan = ['mo', 'p']
    time_suffix_no_us = ['expiration', 'upd']

    for suffix in time_suffix:
        data[f'o_d_{suffix}'] = data[f'o_d_{suffix}'].apply(lambda x: str(x))
        data[f'o_t_{suffix}'] = data[f'o_t_{suffix}'].apply(lambda x: str(x))
        data[f'o_dtm_{suffix}'] = pd.to_datetime(data[f'o_d_{suffix}'] + ' ' + data[f'o_t_{suffix}'], format='%Y%m%d %H:%M:%S') + pd.to_timedelta(data[f'o_m_{suffix}'], unit='us')

    for suffix in time_suffix_with_nan:
        na_mask = data[f'o_d_{suffix}'].isnull()
        data.loc[~na_mask, f'o_d_{suffix}'] = data.loc[~na_mask, f'o_d_{suffix}'].astype(int).astype(str)
        data.loc[~na_mask, f'o_t_{suffix}'] = data.loc[~na_mask, f'o_t_{suffix}'].astype(str)
        data.loc[~na_mask, f'o_dtm_{suffix}'] = pd.to_datetime(data[f'o_d_{suffix}'] + ' ' + data[f'o_t_{suffix}'], format='%Y%m%d %H:%M:%S') + pd.to_timedelta(data[f'o_m_{suffix}'], unit='us')

    for suffix in time_suffix_no_us:
        data[f'o_d_{suffix}'] = data[f'o_d_{suffix}'].apply(lambda x: str(x))
        data[f'o_t_{suffix}'] = data[f'o_t_{suffix}'].apply(lambda x: str(x))
        data[f'o_dt_{suffix}'] = pd.to_datetime(data[f'o_d_{suffix}'] + ' ' + data[f'o_t_{suffix}'], format='%Y%m%d %H:%M:%S')
        data[f'o_dt_{suffix}'].dt.ceil(freq='s')

    #Column drops
    data.drop(columns=[
        'o_seq',
        'o_d_i', 'o_t_i',
        'o_d_en', 'o_t_en',
        'o_d_be', 'o_t_be', 'o_m_be',       #d,t,m columns
        'o_d_br', 'o_t_br', 'o_m_br',       #d,t,m columns
        'o_d_va', 'o_t_va', 'o_m_va',       #d,t,m columns
        'o_d_mo', 'o_t_mo', 'o_m_mo',       #d,t,m columns
        'o_d_p', 'o_t_p', 'o_m_p',          #d,t,m columns
        'o_d_expiration', 'o_t_expiration', #d,t columns
        'o_d_upd', 'o_t_upd',               #d,t columns
    ], inplace=True)

    return data


def read_processed_orders(path):
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
    # Handle data if file is empty
    df = pd.read_csv(path)
    if df.empty:
        print(f"File: \"{Path(path).stem}\" is empty")
    return df