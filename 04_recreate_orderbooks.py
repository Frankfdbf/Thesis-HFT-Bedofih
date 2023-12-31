# Import Built-Ins
import os
import datetime as dt 

# Import Third-Party
import pandas as pd
from tqdm import tqdm

# Import Homebrew
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
 
    # WE FIRST ADD TO THE BOOK ALL ORDERS PRESENT BEFORE THE START OF THE DAY
    #---------------------------------------------------------------------------

    # Get (and set) auction times
    df_auctions = pd.read_parquet(os.path.join(PATHS['root'], 'auctions.parquet'))
    mask = (df_auctions['isin'] == isin) & (df_auctions['date'] == date_datetime)
    auct_open_datetime = df_auctions.loc[mask].auct_open_datetime.item()
    auct_close_datetime = df_auctions.loc[mask].auct_close_datetime.item()

    orderbook = Orderbook(date, isin, auct_open_datetime, auct_close_datetime)#####
    #orderbook.set_auction_datetime1(auct_open_datetime)
    #orderbook.set_auction_datetime2(auct_close_datetime)
    orderbook.set_removed_orders(df_removed_orders)

    # Add order history (i.e., all orders present before starting the day)
    for message in df_history.to_dict('records'):
        orderbook.process(message)
        
    # WE NOW ADD TO THE BOOK ALL ORDERS SUBMITTED FOR AUCTION 1
    #---------------------------------------------------------------------------
    for message in df_orders.to_dict('records'): 
        
        message_dtm = message['o_dtm_va']
        #### stop here (to have orderbook before auction)
        
        orderbook.process(message)
        #### or here (to have auction)
       
        if message_dtm.time() > dt.time(hour=9, minute=10): #### Testing trades
            break
    
    #### debugging
    #orderbook.df_trades.to_csv('/Users/australien/Desktop/estimated_trades.csv')
    


if __name__ == '__main__':
    isin = STOCKS.all[0]

    for date in tqdm(DATES.january):
        print(f'Reconstructing order books - {date}')
        if date == dt.date(2017, 1, 2):
            reconstruct_orderbook(isin, date)
            break