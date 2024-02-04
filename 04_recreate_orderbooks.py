# Import Built-Ins
import os
import datetime as dt
import logging

# Import Third-Party
import pandas as pd
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
sns.set()

# Import Homebrew
from logger import logger
from src.orderbook.orderbook import Orderbook
from src.utils.time_utils import timeit
from src.constants.constants import STOCKS, PATHS, DATES


@timeit
def reconstruct_orderbook(isin: str, date: dt.date) -> None:
    # READ FILES
    #---------------------------------------------------------------------------
    date_str = format(date, '%Y%m%d')
    date_datetime = dt.datetime.strptime(date_str, '%Y%m%d').date()

    # Read history file (VHOXhistory)
    history_name = f'VHOXhistory_{isin}_{date_str}.parquet'
    history_path = os.path.join(PATHS['histories'], isin, history_name)
    df_history = pd.read_parquet(history_path)

    # Read order file (VHOX)
    orders_name = f'VHOX_{isin}_{date_str}.parquet'
    orders_path = os.path.join(PATHS['orders'], isin, orders_name)
    df_orders = pd.read_parquet(orders_path)

    # Read removed order file
    removed_orders_name = f'removedOrders_{isin}_{date_str}.parquet'
    removed_orders_path = os.path.join(PATHS['removed_orders'], isin, removed_orders_name)
    df_removed_orders = pd.read_parquet(removed_orders_path)

    # Read trades file (VHD)
    trades_name = f'VHD_{isin}_{date_str}.parquet'
    trades_path = os.path.join(PATHS['trades'], isin, trades_name)
    df_trades = pd.read_parquet(trades_path)

    # DATAFRAME TO STORE SNAPSHOTS
    #---------------------------------------------------------------------------
    rows = []
 
    # WE SET UP THE ORDERBOOK CLASS
    #---------------------------------------------------------------------------

    # Get (and set) auction times
    df_auctions = pd.read_parquet(os.path.join(PATHS['root'], 'auctions.parquet'))
    mask = (df_auctions['isin'] == isin) & (df_auctions['date'] == date_datetime)
    auct_open_datetime = df_auctions.loc[mask].auct_open_datetime.item()
    auct_close_datetime = df_auctions.loc[mask].auct_close_datetime.item()

    orderbook = Orderbook(date, isin, auct_open_datetime, auct_close_datetime)#####
    orderbook.set_removed_orders(df_removed_orders)
    orderbook.set_trades(df_trades)

    # WE FIRST ADD TO THE BOOK ALL ORDERS PRESENT BEFORE THE START OF THE DAY
    #---------------------------------------------------------------------------
    for message in df_history.to_dict('records'):
        orderbook.process(message)
        
    # WE NOW ADD TO THE BOOK ALL ORDERS SUBMITTED FOR AUCTION 1
    #---------------------------------------------------------------------------

    # Create timestamps for orderbook snapshot
    #timestamps = _create_datetime_range(date_datetime, minutes=1)
    
    timestamps = _create_datetime_range(date_datetime, seconds=1)
    timestamps_for_df = []
    spreads = []

    for message in df_orders.to_dict('records'): 
        
        message_dtm = message['o_dtm_va']
        #next_timestamp = timestamps[-1]
        #### stop here (to have orderbook before auction)

        #print("\n", message_dtm)
        #print(timestamps[-1])

        # Get snapshot of the orderbook
        while len(timestamps) > 0 and message_dtm > timestamps[-1] :
            timestamp = timestamps.pop()

            # remove cancelled orders between last maessage and snapshot
            orderbook._check_for_order_cancelations(timestamp)
            
            timestamps_for_df.append(timestamp)
            logger.info(f'Snapshot: {timestamp}')
            logger.info(f'Orderbook: {orderbook.get_levels(5)}')
            logger.info(f'Spread: {orderbook.spread}')
            logger.debug(f'Next trade: {orderbook.trades[-1]}')
            logger.debug(f'Orders best bid: {orderbook.best_bid.orders}')
            logger.debug(f'Orders best ask: {orderbook.best_ask.orders}')
            logger.debug(f'Last price: {orderbook.last_trading_price}')
            logger.debug(f'Last message: {orderbook.current_message_datetime}')

            spreads.append(orderbook.spread)

            if orderbook.spread == 0:
                #raise NotImplementedError
                logger.error(f'{timestamp} - Spread null: {orderbook.spread}')
                pass
            elif orderbook.spread < 0:
                logger.error(f'{timestamp} - Spread negative: {orderbook.spread}')
                pass


            row = {
                'timestamp': timestamp,
                'spread': orderbook.spread,
                'best_bid': orderbook.best_bid.price,
                'best_ask': orderbook.best_ask.price,
                'depth_3': None,
                'depth_5': None,
            }

            levels = orderbook.get_levels(depth=5, detailed=True)

            for side in levels.keys():
                for n, limit_level in enumerate(levels[side]):
                    row[f'{side}_{n}_price'] = limit_level.price
                    row[f'{side}_{n}_qty'] = limit_level.size
                    row[f'{side}_{n}_hft_dis'] = limit_level.disclosed_size_hft
                    row[f'{side}_{n}_mix_dis'] = limit_level.disclosed_size_mixed
                    row[f'{side}_{n}_non_dis'] = limit_level.disclosed_size_non
                    row[f'{side}_{n}_hft_hid'] = limit_level.hidden_size_hft
                    row[f'{side}_{n}_mix_hid'] = limit_level.hidden_size_mixed
                    row[f'{side}_{n}_non_hid'] = limit_level.hidden_size_non
                    row[f'EMPTY FOR SUM'] = limit_level.hidden_size_non

            
            rows.append(row)
            
            
        logger.debug(f'{message["o_dtm_va"]} - Handling message: {message["o_id_fd"]} | {message["o_cha_id"]}')
        orderbook.process(message)
        #### or here (to have auction)
       
        if message_dtm.time() > dt.time(hour=17, minute=40, second=0) or len(timestamps) == 0: #### Testing 
            break
    
    df = pd.DataFrame.from_records(rows)
    export_path = os.path.join(PATHS['limit_order_books'], isin, f'LOBs_{isin}_{date_str}.parquet')
    #df.to_parquet(export_path, index=False)
    df.to_excel(os.path.join(PATHS['limit_order_books'], isin, f'LOBs_{isin}_{date_str}.xlsx'), index=False)

    #print(df.tail())

    #### debugging
    #orderbook.df_trades.to_csv('/Users/australien/Desktop/estimated_trades.csv')


def _create_datetime_range(curr_date: dt.date, **kwargs):
    """
    Returns a list of datetime object spaced by the specified frequency. The 
    datetimes will start at 9am + delta and end at 5:30 pm.
    **kwargs take the form: unit=frequency.
    """
    time_delta = dt.timedelta(**kwargs)

    start = pd.to_datetime(curr_date) + dt.timedelta(hours=9, minutes=1)
    end = pd.to_datetime(curr_date) + dt.timedelta(hours=17, minutes=30)
    
    datetime_range = pd.date_range(start, end, freq=time_delta).tolist()
    datetime_range_sorted = sorted(datetime_range, reverse=True)

    return datetime_range_sorted


if __name__ == '__main__':
    isin = STOCKS.all[0]
    
    for date in tqdm(DATES.all):
        if date == dt.date(2017, 1, 3):
            print(f'Reconstructing order books - {isin} - {date}')
            reconstruct_orderbook(isin, date)
            break