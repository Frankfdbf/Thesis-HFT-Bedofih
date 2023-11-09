# Import Built-Ins

# Import Third-Party
import pandas as pd

# Import Homebrew
from ..other_utils import check_empty_csv


def preprocess_trades(path: str) -> pd.DataFrame:
    """ Preprocessing of the trade file.
    The function will transform the data into a usable database.
    The data is returned as a pandas dataframe (to then be saved as .parquet).
    """

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

    try:
        df = pd.read_csv(path, names=columns, dtype=dtypes)
    except Exception as e:
        print(e)
        print(f'Error path: {path}')

    # Handle data if file is empty
    if check_empty_csv(df, path):
        return df

    # Create time columns
    df['t_d_b_en'] = pd.to_datetime(df['t_d_b_en'], format='%Y%m%d')
    df['t_d_s_en'] = pd.to_datetime(df['t_d_s_en'], format='%Y%m%d')
    df['t_dtm_neg'] = pd.to_datetime(df[f't_d_neg'] + ' ' + df[f't_t_neg'], format='%Y%m%d %H:%M:%S') + pd.to_timedelta(df[f't_m_neg'], unit='us')

    #Column drops
    df.drop(columns=[
        't_seq',                        # AMF internal sequencial number
        't_price_max',                  # Always empty
        't_price_min',                  # Always empty
        't_t_b_en',                     # Always 00:00:00
        't_t_s_en',                     # Always 00:00:00
        't_currency',                   # Always EUR
        't_cd_gc',                      # Unsure
        't_undo',                       # Unsure
        't_id_u_fd',                    # Unsure
        't_isin',                       # Already have it in file name
        't_origin',                     # Unsure Origin of message (opening trade or rest of session)
        't_cd_pc',                      # Always 025 (Paris)
        't_yield', 't_spread',          # For bonds
        't_d_neg', 't_t_neg', 't_m_neg',# Ex time columns
    ], inplace=True)

    return df