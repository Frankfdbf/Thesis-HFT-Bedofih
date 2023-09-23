# Import Built-Ins
from functools import wraps
import time
import re
import os

# Import Third-Party
import numpy as np
import pandas as pd
from pathlib import Path

# Import Homebrew
from utilities.limit_order_book import LimitOrderBook, Order


def timeit(func):
    """ Decorator to measure execution time of a function."""
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} took {total_time:.4f} seconds.')
        return result
    return timeit_wrapper


#@timeit
def create_limit_order_book(orders, limit_timestamp):
    """
    Normalizes the build of limit order book.
    
    Parameters
    ----------
    :param orders : pandas.DataFrame object
        Dataframe containing the orders
    :param limit_timestamp : datetime object
        Timestamp of the order book 
    
    Returns
    -------
    LimitOrderBook object
    """
    lob = LimitOrderBook()
    for row in orders.itertuples():
        time_validity = row.o_dtm_va
        if time_validity < limit_timestamp:
            if row.o_type == '2':
                id = row.o_id_fd
                char_id = row.o_cha_id
                is_bid = True if row.o_bs == 'B' else False
                ini_size = row.o_q_ini
                neg_size = row.o_q_neg
                rem_size = row.o_q_rem
                price = row.o_price
                state = row.o_state
                o_type = row.o_type
                account = row.o_account
                hft_flag = row.o_member
                time_submission = row.o_dtm_be
                order = Order(uid=id, char_id=char_id, is_bid=is_bid, ini_size=ini_size, neg_size=neg_size, 
                               rem_size=rem_size, price=price, state=state, type=o_type, account=account, 
                               hft_flag=hft_flag, sub_timestamp=time_submission, val_timestamp=time_validity)
                print(order)
                lob.process(order)
                
    return lob


@timeit
def preprocess_order_file(path, save=False, save_path=None):
    """ Preprocessing of the order update file.
    The function will transform the data into a usable database.
    The data can either be exported as csv or return as a pandas database.

    Parameters
    ----------
    :param path : string
        Path of the csv order update file
    :param save : boolean
        If True, saves to specified or default location
        If False, output the data as pandas DataFrame
    :param save_path : string
        Path where the new csv will be saved. Include the file name and 
        extension as well
    
    Returns
    -------
    value : pd.DataFrame
        New formatted order update file
    """
    
    # First read the csv, which does not have headers.
        # We use a regular expression so that pandas does not take into account
        # the commas in between brackets.
    # Create an empty list that will be filled with the clean data.
    # Specify headers that will be used in the clean database.
    # Loop through all the rows of the dirty data.

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

    data = pd.read_csv(path, names=columns)

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

    if save == False:
        # Return the dataframe.
        return data
    else:
        # Save the data to a new csv file.
        if save_path == None:
            raise Exception('Please specify a path to save the file.')
        data.to_csv(save_path, index=False)


def preprocess_trade_file(path, save=False, save_path=None):
    """ Preprocessing of the trade file.
    The function will transform the data into a usable database.
    The data can either be exported as csv or return as a pandas database.

    Parameters
    ----------
    :param path : string
        Path of the csv trade file
    :param save : boolean
        If True, saves to specified or default location
        If False, output the data as pandas DataFrame
    :param save_path : string
        Path where the new csv will be saved. Include the file name and 
        extension as well
    
    Returns
    -------
    value : pd.DataFrame
        New formatted trade file
    """


    # Handle data if file is empty
    data = pd.read_csv(path)
    if data.empty == True:
        print(f"File: \"{Path(path).stem}\" is empty")
        if save == False:
            # Return the dataframe.
            return data
        else:
            # Save the data to a new csv file.
            if save_path == None:
                raise Exception('Please specify a path to save the file.')
            data.to_csv(save_path, index=False)
    del data

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

    # Read data with headers this time.
    data = pd.read_csv(path, names=columns)
    
    # Create time columns
    data['t_d_b_en'] = pd.to_datetime(data['t_d_b_en'], format='%Y%m%d')
    data['t_d_s_en'] = pd.to_datetime(data['t_d_s_en'], format='%Y%m%d')

    data['t_d_neg'] = data[f't_d_neg'].apply(lambda x: str(x))
    data['t_t_neg'] = data[f't_t_neg'].apply(lambda x: str(x))
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

    if save == False:
        # Return the dataframe.
        return data
    else:
        # Save the data to a new csv file.
        if save_path == None:
            raise Exception('Please specify a path to save the file.')
        data.to_csv(save_path, index=False)
    