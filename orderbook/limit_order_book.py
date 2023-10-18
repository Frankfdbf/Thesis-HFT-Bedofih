# Import Built-Ins
import datetime 
from typing import Dict
import json

# Import Third-Party
import pandas as pd 
import numpy as np

# Import Homebrew
from .order import Order
from .trade import Trade 
from .removed_orders import LinkedList

class Orderbook:
    
    def __init__(self, date: datetime.date, isin: str) -> None:

        self.isin = isin 
        self.date = date 
                
        # lists for order priority 
        self.bid_queue = []
        self.ask_queue = [] 
        
        # dictionnaries to update orderbooks with unique prices by level 
        self.bid = {}
        self.ask = {}
        self.bid_detail = {}
        self.ask_detail = {}
        
        # variables for liquidity metrics 
        self.best_bid = 0 
        self.best_ask = 0
        self.best_bidQ = 0
        self.best_askQ = 0
        #### change to None ?
        
        # Linked list of orders that exit the orderbook (either canceled or filled)
        self.removed_orders = LinkedList()
        self.current_removed_order = None

        # data
        # self.current_order_id = 0 
        
        self.current_message = None 
        self.current_message_time = None ####

        # list to store all trades of session
        self.trades = []
        self.last_trading_price = None 
        
        # used to store market to limit orders during auction 
        # should be reset after each auction 
        self.market_limit_orders = []

        # stop orders waiting to be triggered 
        self.buy_stop_orders = []
        self.sell_stop_orders = []

        self.auction_datetime1 = None
        self.auction_datetime2 = None

    def __repr__(self):
        print('Best bid:', self.bid)
        print('Best ask:', self.ask)
        print('Buy stop orders:', self.buy_stop_orders)
        print('Sell stop orders:', self.sell_stop_orders)
        return ''

    def process_message(self, message: Dict) -> None:
        """
        Receives a message and then triggers the appropriate method. 
        """

        o_type   = message['o_type']    # Order type
        o_bs     = message['o_bs']      # Order side
        o_id_fd  = message['o_id_fd']   # Order fundamental id 
        action   = message['action']    # Action to take
        timing   = message['timing']    # History, auction or continuous
        #### We cannot always rely on current_message.time because current_message is only the last new order
        date_time = message['o_dtm_va']  # Time of validity of message
        self.current_message_time = date_time


        if action == 'ADD': # od_id_cha == 1 

            self._create_order_from_message(message)

            if timing in ('HISTORY', 'AUCTION'):  
                auction = True # in both cases, it cannot generate transactions
            elif timing == 'TRADING_DAY':
                auction = False # we can match orders  

            if o_bs == 'B':
                if o_type == '1':
                    self._add_buy_market_order(self.current_message, auction)
                elif o_type == '2': 
                    self._add_buy_limit_order(self.current_message, auction)
                elif ((o_type == '3') or (o_type == '4')): # change to in ('3', '4')
                    self._add_buy_stop_order(self.current_message)
                elif o_type == 'P':      
                    self._add_buy_pegged_order(self.current_message)
                elif o_type == 'K':
                    #### change to market (leave limit for testing auction)
                    self._add_buy_limit_order(self.current_message, auction)
                    #print('This should not appear.') ####
                    #self._add_buy_market_order(self.current_message, auction)

            elif o_bs == 'S':   
                if o_type == '1':
                    self._add_sell_market_order(self.current_message, auction)
                elif o_type == '2':
                    self._add_sell_limit_order(self.current_message, auction)
                elif ((o_type == '3') or (o_type == '4')): # change to in ('3', '4')
                    self._add_sell_stop_order(self.current_message)
                elif o_type == 'P':     
                    self._add_sell_pegged_order(self.current_message)
                elif o_type == 'K':
                    self._add_sell_limit_order(self.current_message, auction)
                    #### change to market (leave limit for testing auction)
                    #self._add_sell_market_order(self.current_message, auction)

        elif action == 'CANCEL': # if o_state == 4: cancelled by the broker 
            self.cancel_order(o_bs, o_id_fd)   
            print("[WARNING] Should not pass through here.")

        elif action == 'MODIFY_PRICE': # if o_state == 5: # order modification
            #new_price = self.current_message.get_o_price()
            #### CURRENT MESSAGE IS ORDER INSTANCE. BUT FOR MODIFIED IT IS STILL OLD ONE AS NONE CREATED
            new_price = message['o_price']
            self.set_order_price(o_bs, o_id_fd, new_price)
            #print(f"{o_id_fd} price modified to {new_price}")

        elif action == 'MODIFY_QUANTITY':
            #new_quantity = self.current_message.get_o_q_rem() # ? 
            #new_quantity = self.current_message.get_o_q_ini()
            #### CURRENT MESSAGE IS ORDER INSTANCE. BUT FOR MODIFIED IT IS STILL OLD ONE AS NONE CREATED
            new_quantity = message['o_q_ini']
            self.set_order_qty(o_bs, o_id_fd, new_quantity)
            #print(f"{o_id_fd} quantity modified to {new_quantity}")

        elif action == 'MODIFY_QUANTITY_&_PRICE':
            new_price = message['o_price']
            new_quantity = message['o_q_ini']
            self.set_order_price(o_bs, o_id_fd, new_price)
            self.set_order_qty(o_bs, o_id_fd, new_quantity)
        else:
            print("[WARNING] action is not recognised.")

        #### Necessary ?
        #self.update_orderbook() 
        
    def _create_order_from_message(self, message) -> None:
        '''
        Takes a message, transform the information into an order (if the message is not a cancellation)
        '''

        o_id_cha        = message['o_id_cha']
        o_id_fd         = message['o_id_fd']
        o_state         = message['o_state']
        #o_dt_be         = message['o_dt_be']
        o_dtm_be        = message['o_dtm_be']
        #o_dt_va         = message['o_dt_va']
        o_dtm_va        = message['o_dtm_va']
        o_bs            = message['o_bs']
        o_type          = message['o_type']
        o_execution     = message['o_execution']
        o_validity      = message['o_validity']
        o_dt_expiration = message['o_dt_expiration']
        o_price         = message['o_price']
        o_price_stop    = message['o_price_stop']
        o_q_ini         = message['o_q_ini']
        o_q_min         = message['o_q_min']
        o_q_dis         = message['o_q_dis']
        o_q_neg         = message['o_q_neg']
        o_app           = message['o_app']
        o_origin        = message['o_origin']
        o_account       = message['o_account']
        o_q_rem         = message['o_q_rem']
        o_member        = message['o_member']
    
        # During auction
        #if o_dtm_be.time() < self.auction_time1: 
        #### need to discuss time. dtm be is book entry, never changes throughout life of order
        # and, need to compare date AND time
        if o_dtm_va < self.auction_datetime1: 
            if o_type in ('1', 'K') and o_price == 0.0: # market & market to limit order (####market to limit not already limit)
                if o_bs == 'B':
                    o_price = 100_000
                elif o_bs == 'S':
                    o_price = 0.0
            
        # Continuous trading
        else:
            print('should not be continuous')#### for testing during auction 
            if o_type == '1': # market order
                if o_bs == 'B':
                    o_price = 100_000  # fake price to simulate high priority 
                elif o_bs == 'S':
                    o_price = 0.0       # fake price to simulate high priority 
                    
            elif o_type == 'K': # market-to-limit order
                o_price = self.get_price(o_bs, level=0)
        
        order = Order(
            o_id_cha = o_id_cha,
            o_id_fd = o_id_fd,
            o_state = o_state,
            o_dtm_be = o_dtm_be,
            o_dtm_va = o_dtm_va,
            o_bs = o_bs,
            o_type = o_type,
            o_execution = o_execution,
            o_validity = o_validity,
            o_dt_expiration = o_dt_expiration,
            o_price = o_price,
            o_price_stop = o_price_stop,
            o_q_ini = o_q_ini,
            o_q_min = o_q_min,
            o_q_dis = o_q_dis,
            o_q_neg = o_q_neg,
            o_app = o_app,
            o_origin = o_origin,
            o_account = o_account,
            o_q_rem = o_q_rem,
            o_member = o_member,
        )
        
        # after creating instance, store order to update price just after auction 
        #if o_dtm_be.time() < self.auction_time1:
        #### need to discuss time. dtm be is book entry, never changes throughout life of order
        # and, need to compare date AND time
        if o_dtm_va < self.auction_datetime1:
            if o_type in ('1', 'K'): # market & market to limit
                self.market_limit_orders.append(order)
        
        self.current_message = order 
        
    def _add_buy_market_order(self, order: Order, auction=False) -> None:
        if auction is True:
            # no trade during auction 
            # order is appended to order queue 
            self.bid_queue.append(order)
            self.bid_queue = sorted(self.bid_queue)
            self.update_orderbook()
            
        elif auction is False: 
            o_q_ini = order.o_q_ini   # can be modified
            while o_q_ini > 0:
                order_counterparty = self.get_next_sell_order()
                trade_qty = min(order_counterparty.o_q_ini, o_q_ini)
                trade_price = order_counterparty.o_price 
                
                # generate trade 
                trade = Trade(order.orderID, order_counterparty.orderID, trade_qty, trade_price)
                self.trades.append(trade)
                self.last_trading_price = trade_price
                
                # update quantities on both sides 
                o_q_ini -= trade_qty 
                order_counterparty.o_q_ini -= trade_qty 
                
                # remove matching order if completely filled 
                if order_counterparty.o_q_ini == 0:
                    self.ask_queue.pop(0)

    def _add_sell_market_order(self, order: Order, auction=False) -> None:
        if auction is True:
            # no trade during auction
            # order is appended to order queue 
            self.ask_queue.append(order)
            self.ask_queue = sorted(self.ask_queue)
            self.update_orderbook() 
            
        elif auction is False:
            o_q_ini = order.o_q_ini   # can be modified
            while o_q_ini > 0:
                order_counterparty = self.get_next_buy_order()
                trade_qty = min(order_counterparty.o_q_ini, o_q_ini)
                trade_price = order_counterparty.o_price
                
                # generate trade 
                trade = Trade(order_counterparty.orderID, order.orderID, trade_qty, trade_price)
                self.trades.append(trade)
                self.last_trading_price = trade_price
                
                # update quantities on both sides 
                o_q_ini -= trade_qty 
                order_counterparty.o_q_ini -= trade_qty 
                
                # remove matching order if completely filled 
                if order_counterparty.o_q_ini == 0:
                    self.bid_queue.pop(0)
    
    def _add_buy_limit_order(self, order: Order, auction=False) -> None:
        if auction is True:
            # no trade during auction 
            # order is appended to order queue 
            self.bid_queue.append(order)
            self.bid_queue = sorted(self.bid_queue)
            self.update_orderbook()
            
        elif auction is False:

            # if order generates trade 
            if order.o_price >= self.best_ask and len(list(self.ask.keys())) > 0:  
                order_counterparty = self.get_next_sell_order()
                o_q_rem = order.o_q_rem
                trade_price = order_counterparty.o_price 
    
                # while order is not completely filled 
                while o_q_rem > 0 and order.o_price >= trade_price:   
                    order_counterparty = self.get_next_sell_order()
                    trade_qty = min(order_counterparty.o_q_ini, o_q_rem)
                    trade_price = order_counterparty.o_price
                    
                    trade = Trade(order.orderID, order_counterparty.orderID, trade_qty, trade_price)
                    self.trades.append(trade) 
                    self.last_trading_price = trade_price
                    
                    o_q_rem -= trade_qty      
                    order_counterparty.o_q_ini -= trade_qty      
    
                    # remove order from ask queue if no qty remaining
                    if order_counterparty.o_q_ini == 0:
                        self.ask_queue.pop(0)
                        # update orderbook and get next tradable price 
                        self.update_orderbook()
                        trade_price = list(self.ask.keys())[0]                
    
                if o_q_rem > 0:
                    #print(o_q_rem)
                    order.set_o_q_rem(o_q_rem)
                    self.bid_queue.append(order)
                    self.bid_queue = sorted(self.bid_queue)
    
            # add new entry and then sort in ascending order
            # potentially update best bid
            elif order.o_price not in self.bid.keys():
                self.bid_queue.append(order)
                self.bid_queue = sorted(self.bid_queue)
            
            # if price is already in orderbook just add qty (no need to sort)
            else:
                self.bid_queue.append(order)
                self.bid_queue = sorted(self.bid_queue)

    def _add_sell_limit_order(self, order: Order, auction=False) -> None:
        if auction is True:
            # no trade during auction
            # order is appended to order queue 
            self.ask_queue.append(order)
            self.ask_queue = sorted(self.ask_queue)
            self.update_orderbook()
            
        elif auction is False:

            if order.o_price <= self.best_bid and len(self.bid.keys()) > 0: 
                order_counterparty = self.get_next_buy_order()
                o_q_rem = order.o_q_rem 
                trade_price = order_counterparty.o_price 
    
                # while order is not completely filled
                while o_q_rem > 0 and order.o_price <= trade_price:          
                    order_counterparty = self.get_next_buy_order()
                    trade_qty = min(order_counterparty.o_q_ini, o_q_rem)
                    trade = Trade(order_counterparty.orderID, order.orderID, trade_qty, trade_price)
                    self.trades.append(trade)
                    self.last_trading_price = trade_price
                
                    o_q_rem -= trade_qty      
                    order_counterparty.o_q_ini -= trade_qty   
    
                    # remove order from bid queue if no qty remaining
                    if order_counterparty.o_q_ini == 0:
                        self.bid_queue.pop(0)
                        # update orderbook and get next tradable price
                        self.update_orderbook()
                        trade_price = list(self.bid.keys())[0]
                
                # no tradable price but unfilled qty 
                if o_q_rem > 0:
                    order.set_o_q_rem(o_q_rem)
                    self.ask_queue.append(order)
                    self.ask_queue = sorted(self.ask_queue)
    
            # add new entry and then sort in ascending order
            # potentially update best ask
            elif order.o_price not in self.ask.keys():
                self.ask_queue.append(order)
                self.ask_queue = sorted(self.ask_queue)
            
            # if price is already in orderbook just add qty (no need to sort)
            else:
                self.ask_queue.append(order)
                self.ask_queue = sorted(self.ask_queue) 
            
    def _add_buy_stop_order(self, order : Order) -> None:
        # condition : stop order accepted ?
        self.buy_stop_orders.append(order)
        # should be sorted by ascending price, then by time of arrival
        self.buy_stop_orders = sorted(self.buy_stop_orders)


    
    def _add_sell_stop_order(self, order: Order) -> None:
        # condition : stop order accepted ? 
        self.sell_stop_orders.append(order)
        # should be sorted by descending price, then by time of arrival 
        self.sell_stop_orders = sorted(self.sell_stop_orders)

    def _add_buy_pegged_order(self, order: Order) -> None:
        print('NEED TO IMPLEMENT PEGGED ORDER !!')
    
    def _add_sell_pegged_order(self, order: Order) -> None:
        print('NEED TO IMPLEMENT PEGGED ORDER !!')

    def get_next_sell_order(self):
        return self.ask_queue[0]
      
    def get_next_buy_order(self):
        return self.bid_queue[0]
    
    def update_orderbook(self) -> None:
        """
        Based on bid and ask queues (lists of pending orders), create two dict's
        """

        #### New marin
        # Remove orders that have removed the book. Either filled or cancelled.
        # This is the new temporary implementation of removed orders.
        # compare datetime and datetime
        #"""
        while self.current_removed_order.data['o_dtm_br'] < self.current_message_time:
            o_bs = self.current_removed_order.data['o_bs']
            o_id_fd = self.current_removed_order.data['o_id_fd']
            self.cancel_order(o_bs, o_id_fd)
            self.current_removed_order = self.current_removed_order.next
        #"""

        #### implement stop orders

        self.bid = {}
        counter_bid = 0
        if len(self.bid_queue) > 0: # and counter_bid < ORDERBOOK_LEVELS:
            for order in self.bid_queue:
                if order.o_price not in self.bid.keys():
                    #self.bid[order.o_price] = order.o_q_rem
                    self.bid[order.o_price] = order.o_q_ini
                    counter_bid += 1
                    self.bid_detail[order.o_price] = {'DH': 0, 'DN': 0, 'DM': 0, 'HH': 0, 'HN': 0, 'HM': 0} 
                else:
                    #self.bid[order.o_price] += order.o_q_rem 
                    self.bid[order.o_price] += order.o_q_ini

                if order.o_q_dis > 0:
                    if order.o_member == 'HFT': # fully disclosed by hft 
                        #self.bid_detail[order.o_price]['DH'] += order.o_q_rem 
                        self.bid_detail[order.o_price]['DH'] += order.o_q_ini

                    elif order.o_member == 'NON': # fully disclosed by non-hft 
                        #self.bid_detail[order.o_price]['DN'] += order.o_q_rem 
                        self.bid_detail[order.o_price]['DN'] += order.o_q_ini

                    elif order.o_member == 'MIX': # fully disclosed by mix 
                        #self.bid_detail[order.o_price]['DM'] += order.o_q_rem 
                        self.bid_detail[order.o_price]['DM'] += order.o_q_ini
                

                #### UPDATE WITH Q INI CHANGE
                else: 
                    q_hidden = order.o_q_rem - order.o_q_dis
                    if order.o_member == 'HFT':
                        self.bid_detail[order.o_price]['HH'] += q_hidden
                        self.bid_detail[order.o_price]['DH'] += order.o_q_dis 

                    elif order.o_member == 'NON':
                        self.bid_detail[order.o_price]['HN'] += q_hidden
                        self.bid_detail[order.o_price]['DN'] += order.o_q_dis 

                    elif order.o_member == 'MIX':
                        self.bid_detail[order.o_price]['HM'] += q_hidden 
                        self.bid_detail[order.o_price]['DM'] += order.o_q_dis

            
            # update liquidity metrics 
            self.best_bid = list(self.bid.keys())[0]
            self.best_bidQ = list(self.bid.values())[0]

        self.ask = {}
        counter_ask = 0
        if len(self.ask_queue) > 0: # and counter_ask < ORDERBOOK_LEVELS:
            for order in self.ask_queue:
                if order.o_price not in self.ask.keys():
                    #self.ask[order.o_price] = order.o_q_rem
                    self.ask[order.o_price] = order.o_q_ini
                    counter_ask += 1
                    self.ask_detail[order.o_price] = {'DH': 0, 'DN': 0, 'DM': 0, 'HH': 0, 'HN': 0, 'HM': 0} 
                else:
                    #self.ask[order.o_price] += order.o_q_rem
                    self.ask[order.o_price] += order.o_q_ini

                #### UPDATE WITH Q INI CHANGE
                if order.o_q_dis > 0:
                    if order.o_member == 'HFT': # fully disclosed by hft 
                        self.ask_detail[order.o_price]['DH'] += order.o_q_rem 

                    elif order.o_member == 'NON': # fully disclosed by non-hft 
                        self.ask_detail[order.o_price]['DN'] += order.o_q_rem 

                    elif order.o_member == 'MIX': # fully disclosed by mix 
                        self.ask_detail[order.o_price]['DM'] += order.o_q_rem 
                
                else: 
                    q_hidden = order.o_q_rem - order.o_q_dis
                    if order.o_member == 'HFT':
                        self.ask_detail[order.o_price]['HH'] += q_hidden
                        self.ask_detail[order.o_price]['DH'] += order.o_q_dis 

                    elif order.o_member == 'NON':
                        self.ask_detail[order.o_price]['HN'] += q_hidden
                        self.ask_detail[order.o_price]['DN'] += order.o_q_dis 

                    elif order.o_member == 'MIX':
                        self.ask_detail[order.o_price]['HM'] += q_hidden 
                        self.ask_detail[order.o_price]['DM'] += order.o_q_dis

            # update liquidity metrics 
            self.best_ask = list(self.ask.keys())[0]
            self.best_askQ = list(self.ask.values())[0]
        
    def compute_price_from_auction(self):
        '''
        Implements auction process

        Returns
        -------
            auction_price 
        '''

        # compute cumulative shares 
        bids = list(self.bid.keys())        # all bid prices
        asks = list(self.ask.keys())        # all ask prices 
        prices = list(set(bids+asks))       # all prices 
        prices.sort() 

        bid_cum_qty = {}
        for price in self.bid.keys():
            bid_cum_qty[price] = 0

            # limit buy orders
            for p, q in self.bid.items():
                if p >= price:
                    bid_cum_qty[price] += q
            """
            #### buy stop orders -> decided not to take them into account
            for buy_stop_order in self.buy_stop_orders:
                if buy_stop_order.o_type == '3': # stop market
                    if (price >= buy_stop_order.o_price_stop):
                        bid_cum_qty[price] += buy_stop_order.o_q_ini
                elif buy_stop_order.o_type == '4': # stop limit
                    if (price >= buy_stop_order.o_price_stop) and ((price <= buy_stop_order.o_price)):
                        bid_cum_qty[price] += buy_stop_order.o_q_ini
            """
        ask_cum_qty = {}
        for price in self.ask.keys():
            ask_cum_qty[price] = 0

            # limit sell orders
            for p, q in self.ask.items():
                if p <= price:
                    ask_cum_qty[price] += q
            """
            #### sell stop orders -> decided not to take them into account
            for sell_stop_order in self.sell_stop_orders:
                if sell_stop_order.o_type == '3': # stop market
                    if (price <= sell_stop_order.o_price_stop):
                        ask_cum_qty[price] += sell_stop_order.o_q_ini
                elif sell_stop_order.o_type == '4': # stop limit
                    if (price <= sell_stop_order.o_price_stop) and ((price >= sell_stop_order.o_price)):
                        ask_cum_qty[price] += sell_stop_order.o_q_ini
            """
        #### Debugging
        #with open("/Users/australien/Desktop/bid_cum.json", "w") as file:
        #    json.dump(bid_cum_qty, file)
        #with open("/Users/australien/Desktop/ask_cum.json", "w") as file:
        #    json.dump(ask_cum_qty, file)
        #print(ask_cum_qty)
        #print(bid_cum_qty)

        #### New implementation
        # Auction price determination
        df_bid_cum_quantity = pd.DataFrame(index=bid_cum_qty.keys(), data=bid_cum_qty.values(), columns=['bid_q_cumulative'])
        df_ask_cum_quantity = pd.DataFrame(index=ask_cum_qty.keys(), data=ask_cum_qty.values(), columns=['ask_q_cumulative'])
        df_auction = pd.concat([df_ask_cum_quantity, df_bid_cum_quantity])
        df_auction.index = df_auction.index.astype(float)
        
        # Sort by prices
        df_auction.sort_index(inplace=True)

        # Merge rows with same prices
        df_auction = df_auction.groupby(by=df_auction.index).sum(min_count=1)

        # Fill NAs appropriately
        df_auction['ask_q_cumulative'].ffill(inplace=True)
        df_auction['bid_q_cumulative'].bfill(inplace=True)

        # Quantity exchanged i.e. minimum of the two columns (excluding NaN)
        quantity_exchanged = np.minimum(df_auction['ask_q_cumulative'].values, df_auction['bid_q_cumulative'].values)
        
        # Update the 'quantity_exchanged' column with NaN if any of the original columns have NaN
        df_auction['quantity_exchanged'] = np.where(df_auction[['ask_q_cumulative', 'bid_q_cumulative']].isna().any(axis=1), np.nan, quantity_exchanged)
        
        # Quantity left
        df_auction['quantity_left'] = np.abs(df_auction['ask_q_cumulative'] - df_auction['bid_q_cumulative'])

        # First rule
        max_quantity_exchanged = df_auction['quantity_exchanged'].max(axis=0)
        df_auction_max_quantity_exchanged = df_auction[df_auction.quantity_exchanged == max_quantity_exchanged]

        # Second rule 
        minimum_quantity_left = df_auction_max_quantity_exchanged.quantity_left.min(axis=0)
        auction_price =  df_auction_max_quantity_exchanged.loc[df_auction_max_quantity_exchanged.quantity_left == minimum_quantity_left].index[0]

        #### Testing/debugging 
        index = df_auction[df_auction.index == auction_price].index
        index_num = df_auction.index.get_loc(index[0])
        print(df_auction.iloc[index_num-10 : index_num+10])




        """
        ####EX CODE CHRISTOPHE
        auction = {}
        auction_qty = 0 
        quantity_left = 0
        for price in prices:
            # .get returns None if key not in dictionnary, replaced by 0
            bid_cum = bid_cum_qty.get(price)
            if bid_cum is None:
                bid_cum = 0
            ask_cum = ask_cum_qty.get(price)
            if ask_cum is None:
                ask_cum = 0

            auction[price] = {
                'q_traded': min(bid_cum, ask_cum) ,
                'q_left': abs(bid_cum - ask_cum)
            }
            auction 
            if auction[price]['q_traded'] > auction_qty:
                auction_qty = auction[price]['q_traded']
                quantity_left = auction[price]['q_left']
                auction_price = price
            elif auction[price]['q_traded'] == auction_qty:
                if auction[price]['q_left'] < quantity_left:
                    auction_qty = auction[price]['q_traded']
                    quantity_left = auction[price]['q_left']
                    auction_price = price
        """
        print('Auction price:', auction_price)
        return auction_price
        # to do: implement auction other rules 

    def generate_auction_trades(self):
        # First we need to determine the auction price 
        # then, make trades accordingly 
        
        auction_price = self.compute_price_from_auction()
        for order in self.market_limit_orders:
            order.set_o_price(auction_price)
        
        i_am_done = False 

        while i_am_done is False:
            buy_order = self.get_next_buy_order() 
            sell_order = self.get_next_sell_order()
            
            buy_order_o_id_fd = buy_order.get_o_id_fd()
            sell_order_o_id_fd = sell_order.get_o_id_fd() 
            
            buy_order_price = buy_order.get_o_price() 
            sell_order_price = sell_order.get_o_price()
            
            buy_order_qty = buy_order.get_o_q_rem()
            sell_order_qty = sell_order.get_o_q_rem() 
        
            if (buy_order_price < auction_price) or (sell_order_price > auction_price):
                i_am_done = True 
                break 
            
            else:
                trade_qty = min(buy_order_qty, sell_order_qty)
                
                trade = Trade(buy_order_o_id_fd, sell_order_o_id_fd, trade_qty, auction_price)
                self.trades.append(trade)
                self.last_trading_price = auction_price
                
                buy_order_o_q_rem = buy_order_qty - trade_qty
                sell_order_o_q_rem = sell_order_qty - trade_qty
                if buy_order_o_q_rem == 0:
                    self.bid_queue.pop(0)
                    sell_order.set_o_q_rem(sell_order_o_q_rem)
                else:
                    buy_order.set_o_q_rem(buy_order_o_q_rem)
                    self.ask_queue.pop(0)

    def empty_market_orders_queue(self):
        '''
        After auction, we should not have any market(-to-limit) orders. 
        '''
        self.market_limit_orders = []

    def cancel_order(self, o_bs, o_id_fd) -> None:
        '''
        Removes the order from the bid/ask queue 
        The order is uniquely identified by its o_id_fd. 
        '''
        # o_bs = self.get_o_bs_with_o_id_fd(o_id_fd)
        #### Shortened
        #"""
        if o_bs == 'B':
            side = self.bid_queue              
        elif o_bs == 'S':
            side = self.ask_queue

        for order in side:
            if order.o_id_fd == o_id_fd:
                order.o_id_cha += 1 # cancellation updates order id chain 
                side.remove(order)
        return
        """
        if o_bs == 'B':
            for order in self.bid_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain 
                    self.bid_queue.remove(order)
                    
        elif o_bs == 'S':
            for order in self.ask_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain 
                    self.ask_queue.remove(order)
        """

    # def get_o_bs_with_o_id_fd(self, o_id_fd) -> str:
    #     '''
    #     When we cancel an order, we need to know if it is a buy or a sell order. 
    #     The order is uniquely identified by its o_id_fd. 
    #     '''
    #     order = self.data[self.data['o_id_fd'] == o_id_fd].copy() 
    #     o_bs = order['o_bs'].iloc[0]
    #     return o_bs 
    
    def get_order_qty(self, o_bs, o_id_fd) -> int:
        '''
        Based on order o_id_fd, returns order qty
        '''
        #### Here q rem will be 0 one row too early (warning with partially filled orders)
        if o_bs == 'B':
            for order in self.bid_queue:
                if order.o_id_fd == o_id_fd:
                    #return order.o_q_rem 
                    return order.o_q_ini
                
        elif o_bs == 'S':
            for order in self.ask_queue:
                if order.o_id_fd == o_id_fd:
                    #return order.o_q_rem
                    return order.o_q_ini
    
    def set_order_qty(self, o_bs, o_id_fd, new_quantity) -> None:
        '''
        Modifies the quantity of an order in the orderbook.  
        The order is uniquely identified by its o_id_fd. 
        '''
        # o_bs = self.get_o_bs_with_o_id_fd(o_id_fd)

        #### o_q_ini and not o_q_rem

        if o_bs == 'B':
            for order in self.bid_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain
                    order.set_o_q_ini(new_quantity)
                    return ####
                
                    # update datetime details ? 
                    
        elif o_bs == 'S':
            for order in self.ask_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain
                    order.set_o_q_ini(new_quantity)
                    return ####
                    # update datetime details ? 

    def get_order_price(self, o_bs, o_id_fd):
        '''
        Based on order o_id_fd, returns order price 
        '''
        # o_bs = self.get_o_bs_with_o_id_fd(o_id_fd)
        if o_bs == 'B':
            for order in self.bid_queue:
                if order.o_id_fd == o_id_fd:
                    return order.o_price 
                
        elif o_bs == 'S':
            for order in self.ask_queue:
                if order.o_id_fd == o_id_fd:
                    return order.o_price


    def set_order_price(self, o_bs, o_id_fd, new_price) -> None:
        '''
        Modifies the price of an order in the orderbook.  
        The order is uniquely identified by its o_id_fd. 
        '''
        # o_bs = self.get_o_bs_with_o_id_fd(o_id_fd)

        if o_bs == 'B':
            for order in self.bid_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain
                    order.set_o_price(new_price)
                    self.bid_queue = sorted(self.bid_queue)
                    return ####
                    # update datetime details ? 
                    
        elif o_bs == 'S':
            for order in self.ask_queue:
                if order.o_id_fd == o_id_fd:
                    order.o_id_cha += 1 # cancellation updates order id chain
                    order.set_o_price(new_price)
                    self.ask_queue = sorted(self.ask_queue)
                    return ####
                    # update datetime details ? 
    


    def get_price(self, o_bs, level) -> float:
        '''
        Returns the price at a certain level in the orderbook. 
        '''
        if o_bs == 'B' and len(list(self.bid.keys())) > 0:
            if level < len(list(self.bid.keys())):
                return list(self.bid.keys())[level]
            else:
                return None 
            
        elif o_bs == 'S' and len(list(self.ask.keys())) > 0:
            if level < len(list(self.ask.keys())):
                return list(self.ask.keys())[level]
            else:
                return None 
    
    def get_qty(self, o_bs, level) -> int:
        '''
        Returns the total quantity at a certain level in the orderbook. 
        '''
        if o_bs == 'B' and len(list(self.bid.values())) > 0:
            if level < len(list(self.bid.values())):
                return list(self.bid.values())[level]
            else:
                return 0 # no qty available at that limit
            
        elif o_bs == 'S' and len(list(self.ask.values())) > 0:
            if level < len(list(self.ask.values())):
                return list(self.ask.values())[level]
            else:
                return 0 # no qty available at that limit
            
    def set_auction_datetime1(self, time: datetime.time) -> None:
        # research perspective: we know the time retrospectively 
        self.auction_datetime1 = time 

    def get_auction_datetime1(self):
        return self.auction_datetime1

    def set_auction_datetime2(self, time: datetime.time) -> None:
        # research perspective: we know the time retrospectively 
        self.auction_datetime2 = time 

    def get_auction_time2(self):
        return self.auction_datetime2
    
    def get_last_trading_price(self):
        return self.last_trading_price
    
    # useful ? 
    def get_time_from_current_message(self):
        '''
        Returns
        -------
        dt : TYPE
            datetime.
        dtm : TYPE
            microsecond.
        '''
        if self.current_order is None:
            self.set_current_message()
        return self.current_order.get_time_book_entrance()

    # useful ?   
    def get_time_from_next_cancelled_order(self):
        '''
        Returns
        -------
        dt : TYPE
            DESCRIPTION.
        dtm : TYPE
            DESCRIPTION.

        '''
        dt = self.cancelled_orders.iloc[self.cancelled_orders_id]['o_dt_mo']
        dtm = self.cancelled_orders.iloc[self.cancelled_orders_id]['o_dtm_mo']
        return (dt, dtm)

    def get_price_from_next_sell_stop_order(self):
        if len(self.sell_stop_orders) > 0:
            return self.sell_stop_orders[0].o_price_stop
        else:
            return None

    def get_price_from_next_buy_stop_order(self):
        if len(self.buy_stop_orders) > 0:
            return self.buy_stop_orders[0].o_price_stop
        else:
            return None
    

    def set_removed_orders(self, removed_orders: pd.DataFrame) -> None:
        """
        Pass as argument dataframe of removed orders. The function will update
        the linked list.
        Sets as current_removed_order, order to first check
        """
        self.removed_orders.populate(removed_orders, self.date)
        self.current_removed_order = self.removed_orders.head
        return
