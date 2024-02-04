# Import Built-Ins
import traceback

# Import Third-Party
import pandas as pd
import numpy as np

# Import Homebrew
from ..other_utils import check_empty_csv


def preprocess_events(path: str) -> pd.DataFrame:
    """ Preprocessing of the event file. The function will transform the data 
    into a usable database. The data is returned as a pandas dataframe (to then 
    be saved as .parquet).

    Args:
        path (str): path of the event file.

    Returns:
        pd.DataFrame: processed table of the events.
    """

    columns = [
        'e_seq', 'e_act_m_state', 'e_d_upd', 'e_d_me', 'e_t_me', 
        'e_d_suspension', 'e_t_suspension', 'e_ct_state', 'e_value_state',
        'e_cd_gc', 'e_t_op', 'e_reservation', 'e_isin', 'e_cd_pc'
    ]

    dtypes = {
        'e_seq': 'int32',
        'e_act_m_state': 'category',
        'e_d_upd': 'object',
        'e_d_me': 'string',
        'e_t_me': 'string',
        'e_d_suspension': 'float',
        'e_t_suspension': 'float',
        'e_ct_state': 'float',
        'e_value_state': 'category',
        'e_cd_gc': 'category',
        'e_t_op': 'object',
        'e_reservation': 'category',
        'e_isin': 'category',
        'e_cd_pc': 'category'
    }

    try:
        df = pd.read_csv(path, names=columns, dtype=dtypes)
    except Exception as e:
        print(traceback.format_exc())
        print(f'Error path: {path}')
        return pd.DataFrame()
    
    if check_empty_csv(df, path):
        return df
    
    # Time columns
    new_columns = ['e_dt_me']
    date_columns = ['e_d_me']
    time_columns = ['e_t_me']

    for i, col in enumerate(date_columns):

        # Mask for columns with NaNs
        na_mask = df[col].isnull()

        # Creating new time columns
        df.loc[~na_mask, new_columns[i]] = pd.to_datetime(df[date_columns[i]] + ' ' + df[time_columns[i]], format='%Y%m%d %H:%M:%S')
    
    # Update time
    df['e_d_upd'] = pd.to_datetime(df['e_d_upd'], format='%Y%m%d')

    # Programmed opening time
    na_mask = ((df['e_t_op'] == '0') | (df['e_t_op'] == 0))
    df.loc[na_mask, 'e_t_op'] = np.nan
    df.loc[~na_mask, 'e_t_op'] = pd.to_datetime(df['e_t_op'], format='%H:%M:%S').dt.time

    #Column drops
    df.drop(columns=[
        'e_d_suspension', 'e_t_suspension',
        'e_d_me', 'e_t_me',
        'e_ct_state', 
        'e_cd_pc'
        ], inplace=True)

    return df