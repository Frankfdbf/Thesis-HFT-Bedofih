# Import Built-Ins
import os
import datetime as dt 

# Import Third-Party
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Import Homebrew
from orderbook.limit_order_book import Orderbook
from utils.other_utils import clean_message
from utils.time_utils import timeit
from constants.constants import STOCKS, PATHS, DATES


@timeit
def reconstruct_orderbook(isin, date):
    # READ FILES
    #---------------------------------------------------------------------------
    
    date_str = format(date, '%Y%m%d')
    date_datetime = dt.datetime.strptime(date_str, '%Y%m%d').date()
    #### Add this in constants
    auction1_limit = dt.datetime.strptime(date_str, '%Y%m%d') + dt.timedelta(hours=9, minutes=0, seconds=30)

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

    orderbook = Orderbook(date, isin)
    orderbook.set_auction_datetime1(auct_open_datetime)
    orderbook.set_auction_datetime2(auct_close_datetime)
    orderbook.set_removed_orders(df_removed_orders)

    # Add order history (i.e., all orders present before starting the day)
    for message in df_history.to_dict('records'):
        message = clean_message(message)
        message['action'] = 'ADD'
        message['timing'] = 'HISTORY'
        orderbook.process_message(message)
    
    # WE NOW ADD TO THE BOOK ALL ORDERS SUBMITTED FOR AUCTION 1
    #---------------------------------------------------------------------------
    
    
    """
    orderbook.update_orderbook()
    print("\nBID")
    counter = 0
    for key, value in orderbook.bid.items():
        if counter > 10: break
        print(f'P:{key}    \t- Q:{value}')
        counter += 1
    
    print("\nASK")
    counter = 0
    for key, value in orderbook.ask.items():
        if counter > 10: break
        print(f'P:{key}    \t- Q:{value}')
        counter += 1
        
    """

    auction_in_progress = True

    #idx = 0
    #idx_cancel = 0         #### I find no use for these counters

    while auction_in_progress:

        # Handle each message
        for message in df_orders.to_dict('records'): 
            #### IDEA: we can delete each row after seeing it. 
            # What will be left will be continuous and auction2
            # maybe use linked list (if faster).
            message = clean_message(message)
            message['timing'] = 'AUCTION'

            #### Temp debugging
            #if message['o_type'] == 'K':
            #    continue

            # Check if message is valid before
            #### Need to have a discussion on the meaning of each datetime, different understandings.
            message_dtm = message['o_dtm_va']

            if (message_dtm > orderbook.auction_datetime1) or (message_dtm > auction1_limit):
                auction_in_progress = False
                break

            # New order
            if message['o_id_cha'] == 1:
                message['action'] = 'ADD'

            # Existing order
            else:
                # order is not new and o_id_fd already in orderbook 
                old_price = orderbook.get_order_price(message['o_bs'], message['o_id_fd'])
                new_price = message['o_price']
                old_qty   = orderbook.get_order_qty(message['o_bs'], message['o_id_fd'])
                new_qty   = message['o_q_ini'] # or o_q_rem ???? 
                
                #### ISSUE FOR MARKET ORDERS (in initial code) !
                # issue because price bid market is 100_000 and new price 0
                #### TEMPORARY CORRECTION (works at least for auction)
                if message['o_type'] in ('1', 'K'): # market & market to limit order
                    if message['o_bs'] == 'B':
                        new_price = 100_000
                    elif message['o_type'] == 'S':
                        new_price = 0.0


                if False: # cancelled by broker (or all else)
                    pass
                    #### this part was initialy used for canceled orders.
                    # however messages with o_state canceled are not actually canceled when message is sent
                    # they are conceled later. Hence the need for removed orders
                    # Thus, in my code this part is removed

                # modified: new price 
                elif (old_price != new_price) & (old_qty == new_qty):
                    message['action'] = 'MODIFY_PRICE'
                    # o_state => 5
                    
                # modified: new quantity 
                elif (old_price == new_price) & (old_qty != new_qty):
                    message['action'] = 'MODIFY_QUANTITY'
                    # o_state => 5
                
                # modified: new price and new quantity
                elif (old_price != new_price) & (old_qty != new_qty):
                    message['action'] = 'MODIFY_QUANTITY_&_PRICE'
                
                else:
                    print('to be checked if it happens')
                    
                
                """
                # CHANGE THIS IS WRONG
                # THINK ABOUT TREATING IT AFTER
                # cancelled by broker
                if message['o_dtm_br'] == message['o_dtm_mo']:
                    message['action'] = 'CANCEL'
                    idx_cancel += 1
                    # o_state => 4

                # CHANGE VALIDITY IS NOT THE FIELD YOU THINK
                
                # cancelled by system because of validity reached
                elif orderbook.date > str(msg['o_d_va']):
                    message['action'] = 'CANCEL'
                    # o_state => 'C' 
                """
            orderbook.process_message(message)
    #print(orderbook)
    """
    print("\nBID")
    counter = 0
    for key, value in orderbook.bid.items():
        if counter > 10: break
        print(f'P:{key}    \t- Q:{value}')
        counter += 1
    
    print("\nASK")
    counter = 0
    for key, value in orderbook.ask.items():
        if counter > 10: break
        print(f'P:{key}    \t- Q:{value}')
        counter += 1
        
    
    orderbook_dict_bid = {
        'price_bid': [_ for _ in orderbook.bid.keys()],
        'quantity_bid': [_ for _ in orderbook.bid.values()],
    }
    orderbook_dict_ask = {
        'price_ask': [_ for _ in orderbook.ask.keys()],
        'quantity_ask': [_ for _ in orderbook.ask.values()],
    }
    df_bid = pd.DataFrame(orderbook_dict_bid)
    df_ask = pd.DataFrame(orderbook_dict_ask)
    df_bid.to_excel('/Users/australien/Desktop/orderbook_bid.xlsx')
    df_ask.to_excel('/Users/australien/Desktop/orderbook_ask.xlsx')
    """
    
    # 3. auction process 
    auction_price = orderbook.compute_price_from_auction()
    #print(auction_price)
    #print(orderbook.buy_stop_orders)
    #print(orderbook.sell_stop_orders)
    #orderbook.generate_auction_trades() 

    # on ne devrait pas avoir a le faire 
    # test pour verifier que c'est bien vide ? 
    # orderbook.empty_market_orders_queue() 
        
    # - make trades
    # 4. continuous trading 
    
    
    
    #tmp = messages[(messages['o_t_be'] > '08:59:50')&(messages['o_t_be'] < '09:00:30')]


    #### creating dataframe to check prices
    """
    row = {
        'dates': date,
        'opening_auct_time': auct_open_datetime,
        'opening_auct_price': auction_price
    }
    list_computed_fixing.append(row)
    """



if __name__ == '__main__':
    # Get auctions 
    print('Reconstructing order books ...')
    isin = STOCKS.all[0]
    
    #list_computed_fixing = []


    for date in tqdm(DATES.january):
        reconstruct_orderbook(isin, date)
        

    #df_computed_fixing = pd.DataFrame(list_computed_fixing, columns=['dates', 'opening_auct_time', 'opening_auct_price'])
    #df_computed_fixing.to_csv('/Volumes/Extreme ssd/auctions_computed.csv')





