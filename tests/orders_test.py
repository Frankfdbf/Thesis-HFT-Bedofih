# Import Built-Ins
import logging
from unittest import TestCase
import datetime as dt
import os

# Import Third-Party
import pandas as pd
from tqdm import tqdm

# Import Homebrew
from src.constants.constants import DATES, PATHS, STOCKS
from src.orderbook.orderbook import Orderbook


# Init Logging Facilities
log = logging.getLogger(__name__)


class OrderTests(TestCase):
    def __init__(self):
        super().__init__()

        self.isin = 'FR0000120404'
        self.date = DATES.january[0]
        df_auction = pd.read_csv('/Volumes/Extreme ssd/auctions.csv')
        df_auction = df_auction[df_auction['isin'] == 'FR0000120404'][['date', 'auct_open_time', 'auct_open_price']]
        df_auction.date = pd.to_datetime(df_auction.date, format='%Y-%m-%d').dt.date

        path_orders = f'/Volumes/Extreme ssd/orders/FR0000120404/VHOX_FR0000120404_20170102.parquet'
        path_history = f'/Volumes/Extreme ssd/histories/FR0000120404/VHOXhistory_FR0000120404_20170102.parquet'
        path_trades = f'/Volumes/Extreme ssd/trades/FR0000120404/VHD_FR0000120404_20170102.parquet'
        path_removed_orders = '/Volumes/Extreme ssd/removed_orders/FR0000120404/removedOrders_FR0000120404_20170102.parquet'
        self.df_o = pd.read_parquet(path_orders)
        self.df_h = pd.read_parquet(path_history)
        self.df_t = pd.read_parquet(path_trades)
        self.df_removed = pd.read_parquet(path_removed_orders)
        opening_time = pd.to_datetime(df_auction[df_auction.date == dt.date(2017, 1, 2)].auct_open_time).item().time()
        self.opening_datetime = dt.datetime(2017, 1, 2) + dt.timedelta(hours=opening_time.hour, minutes=opening_time.minute, seconds=opening_time.second, microseconds=opening_time.microsecond)
        self.closing_datetime = dt.datetime(2017, 1, 2) + dt.timedelta(hours=17, minutes=30) # arbitrary
    def test_history_orders_no_change_in_update(self):
        """
        Case where, in history, an order is 'updated' because its expiry change.
        There are no other changes. The first order message should create a new 
        limit level, and the second message should not change it.
        """
        lob = Orderbook(self.date, self.isin)
        lob.set_auction_datetime1(self.opening_datetime)
        lob.set_auction_datetime2(self.closing_datetime)
        lob.set_removed_orders(self.df_removed)

        df_orders = self.df_h.loc[self.df_h.o_id_fd == 17480177072]
        for message in df_orders.to_dict('records'):
            lob.process(message)
        dic = {32.46: 150}
        self.assertEqual(lob.get_levels()['bids'], dic) 

    def test_order_update_quantity(self):
        """
        Order's quantity is changed before being filled during the auction.
        """
        lob = Orderbook(self.date, self.isin)
        lob.set_auction_datetime1(self.opening_datetime)
        lob.set_auction_datetime2(self.closing_datetime)
        lob.set_removed_orders(self.df_removed)
        
        o_id = 17566553290

        df_orders = self.df_o.loc[self.df_o.o_id_fd == o_id]
        for message in df_orders.to_dict('records'):
            lob.process(message)
            
        dic = {36.665: 30}

        self.assertEqual(lob.get_levels()['bids'], dic) 
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_hft, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_mixed, 10)
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_non, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_hft, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_mixed, 20)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_non, 0)
        

    def test_history_orders_change_in_price_then_quantity(self):
        """
        In hist, Order is submited at price p1, and size q1. Price is then changed
        to p2. Size is then changed to q2.
        """
        lob = Orderbook(self.date, self.isin)
        lob.set_auction_datetime1(self.opening_datetime)
        lob.set_auction_datetime2(self.closing_datetime)
        lob.set_removed_orders(self.df_removed)
        
        o_id = 17073232200

        df_orders = self.df_h.loc[self.df_h.o_id_fd == o_id]
        for message in df_orders.to_dict('records'):
            lob.process(message)

        dic = {30.51: 200}
        self.assertEqual(lob.get_levels()['bids'], dic)
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_hft, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_mixed, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.disclosed_size_non, 200)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_hft, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_mixed, 0)
        self.assertEqual(lob._orders[o_id].parent_limit.hidden_size_non, 0)
    

    def test_auction_prices_and_trades_january(self):
        """
        Test auction prices for a stock and for the month of january.
        """

        def reconstruct_orderbook(isin: str, date: dt.date) -> None:
            # READ FILES
            #---------------------------------------------------------------------------
            
            date_str = format(date, '%Y%m%d')
            date_datetime = dt.datetime.strptime(date_str, '%Y%m%d').date()
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

            counter = 0
            # Add order history (i.e., all orders present before starting the day)
            for message in df_history.to_dict('records'):
                orderbook.process(message)
                counter += 1

            # WE NOW ADD TO THE BOOK ALL ORDERS SUBMITTED FOR AUCTION 1
            #---------------------------------------------------------------------------
            for message in df_orders.to_dict('records'): 
                message_dtm = message['o_dtm_va']
                orderbook.process(message)
                
                if (message_dtm > orderbook.auction_datetime1) or (message_dtm > auction1_limit):
                    break
            
            # Add auction price to list
            estimated_auction_prices.append(orderbook.auction1_price)

            return orderbook
    
        true_auction_prices = [35.545, 35.945, 37.24, 37.34, 37.81, 38.0, 38.33,
                               38.36, 38.075, 38.25, 38.325, 38.01, 38.745, 38.435,
                               38.16, 37.965, 38.0, 38.475, 38.595, 38.5, 38.485, 38.2]
        estimated_auction_prices = []
        isin = STOCKS.all[0]

        for date in tqdm(DATES.january):
            date_str = format(date, '%Y%m%d')
            print(date)
            # Read trade file 
            trades_name = f'VHD_{isin}_{date_str}.parquet'
            trades_path = os.path.join(PATHS['trades'], isin, trades_name)
            df_trades = pd.read_parquet(trades_path)
            auction_trades = df_trades.loc[(df_trades.t_agg.isna()) & (df_trades.t_dtm_neg < dt.datetime(year=date.year, month=date.month, day=date.day, hour=9, second=30))]
            bids_filled = auction_trades[['t_id_b_fd', 't_q_exchanged']].groupby(by='t_id_b_fd').sum()
            asks_filled = auction_trades[['t_id_s_fd', 't_q_exchanged']].groupby(by='t_id_s_fd').sum()
            asks_filled_dict = asks_filled.to_dict()['t_q_exchanged']
            bids_filled_dict = bids_filled.to_dict()['t_q_exchanged']

            orderbook = reconstruct_orderbook(isin, date)

            bids_filled_estimated = {}
            asks_filled_estimated = {}
            for trade in orderbook.trades:
                try:
                    bids_filled_estimated[trade.t_id_b_fd] += trade.t_q_exchanged
                except KeyError:
                    bids_filled_estimated[trade.t_id_b_fd] = trade.t_q_exchanged
                
                try:
                    asks_filled_estimated[trade.t_id_s_fd] += trade.t_q_exchanged
                except KeyError:
                    asks_filled_estimated[trade.t_id_s_fd] = trade.t_q_exchanged

            for key in bids_filled_dict.keys():
                self.assertEqual(bids_filled_estimated[key], bids_filled_dict[key])
            for key in asks_filled_dict.keys():
                self.assertEqual(asks_filled_estimated[key], asks_filled_dict[key])


        self.assertEqual(estimated_auction_prices, true_auction_prices)


if __name__ == '__main__':
    test_class = OrderTests()
    test_class.test_history_orders_no_change_in_update()
    test_class.test_order_update_quantity()
    test_class.test_history_orders_change_in_price_then_quantity()
    test_class.test_auction_prices_and_trades_january()

