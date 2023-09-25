# Import Built-Ins

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew
from utils.other_utils import check_empty_csv
from utils.time_utils import timeit



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
        'o_price_stop',
        'o_price_dfpg',
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

    dtypes = {
        'o_seq': 'int32',
        'o_isin': 'string',
        'o_d_i': 'string',
        'o_t_i': 'string',
        'o_cha_id': 'int16',
        'o_id_fd': 'int64',
        'o_d_be': 'string',
        'o_t_be': 'string',
        'o_m_be': 'int32',
        'o_d_br': 'string',
        'o_t_br': 'string',
        'o_m_br': 'int32',
        'o_d_va': 'string',
        'o_t_va': 'string',
        'o_m_va': 'int32',
        'o_d_mo': 'string',
        'o_t_mo': 'string',
        'o_m_mo': 'float64',
        'o_d_en': 'string',
        'o_t_en': 'string',
        'o_sq_nb': 'int32',
        'o_sq_nbm': 'int32',
        'o_d_p': 'string',
        'o_t_p': 'string',
        'o_m_p': 'float64',
        'o_state': 'category',
        'o_currency': 'category',
        'o_bs': 'category',
        'o_type': 'category',
        'o_execution': 'category',
        'o_validity': 'category',
        'o_d_expiration': 'string',
        'o_t_expiration': 'string',
        'o_price': 'float64',
        'o_price_stop': 'float64',
        'o_price_dfpg': 'int8',
        'o_disoff': 'int8',
        'o_q_ini': 'int32',
        'o_q_min': 'int32',
        'o_q_dis': 'int32',
        'o_q_neg': 'int32',
        'o_app': 'category',
        'o_origin': 'category',
        'o_account': 'category',
        'o_nb_tr': 'int16',
        'o_q_rem': 'int32',
        'o_d_upd': 'string',
        'o_t_upd': 'string',
        'o_member': 'category',
    }


    df = pd.read_csv(path, 
                       names=columns, dtype=dtypes)
    

    # Handle data if file is empty
    if check_empty_csv(df, path):
        return df

    new_columns = ['o_dtm_be', 'o_dtm_br', 'o_dtm_va', 'o_dtm_mo', 'o_dtm_p', 'o_dt_expiration', 'o_dt_upd']
    date_columns = ['o_d_be', 'o_d_br', 'o_d_va', 'o_d_mo', 'o_d_p', 'o_d_expiration', 'o_d_upd']
    time_columns = ['o_t_be', 'o_t_br', 'o_t_va', 'o_t_mo', 'o_t_p', 'o_t_expiration', 'o_t_upd']
    microseconds_columns = ['o_m_be', 'o_m_br', 'o_m_va', 'o_m_mo', 'o_m_p']

    for i, col in enumerate(date_columns):
        # Mask for columns with NaNs
        na_mask = df[date_columns[i]].isnull()

        # Get microseconds
        try:
            microseconds = df[microseconds_columns[i]]
        except IndexError:
            microseconds = 0

        # Creating new time columns
        df.loc[~na_mask, new_columns[i]] = pd.to_datetime(df[date_columns[i]] + ' ' + df[time_columns[i]], format='%Y%m%d %H:%M:%S') + pd.to_timedelta(microseconds, unit='us')

    #Column drops
    df.drop(columns=[
        'o_seq',
        'o_isin',
        'o_d_i', 'o_t_i',
        'o_d_en', 'o_t_en',
        'o_d_be', 'o_t_be', 'o_m_be',       #d,t,m columns
        'o_d_br', 'o_t_br', 'o_m_br',       #d,t,m columns
        'o_d_va', 'o_t_va', 'o_m_va',       #d,t,m columns
        'o_d_mo', 'o_t_mo', 'o_m_mo',       #d,t,m columns
        'o_d_p', 'o_t_p', 'o_m_p',          #d,t,m columns
        'o_currency',
        'o_price_dfpg',
        'o_disoff',
        'o_d_expiration', 'o_t_expiration', #d,t columns
        'o_d_upd', 'o_t_upd',               #d,t columns
        'o_price_dfpg', 'o_disoff',
    ], inplace=True)

    return df


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