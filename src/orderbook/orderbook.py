# Import Built-Ins
import datetime as dt
from typing import Dict, List
from itertools import islice
from collections import deque
import logging
import traceback

# Import Third-Party
import pandas as pd 
import numpy as np

# Import Homebrew
from logger import logger
from .order import Order, OrderList
from .trade import Trade 
from .limit_level import LimitLevel
from .auction import Auction
from src.utils.preprocessing.preprocess_message import preprocess_message


class Orderbook:
    """
    Instance for one day of orderbook. The orderbook's state can be saved and
    is updated after each order.
    For each message the class should either add, remove or modify an order. 
    After each message, it should update itself (checking for trades and deleted
    orders). 
    """

    def __init__(
        self, date: dt.date, isin: str, opening_auction_datetime: dt.datetime, 
        closing_auction_datetime: dt.datetime
    ) -> None:
        """
        Args:
            date (dt.date): date of the orders and trades.
            isin (str): isin code of the security.
            opening_auction_datetime (dt.datetime): datetime object for the opening auction.
            closing_auction_datetime (dt.datetime): datetime object for the closing auction.
        """
        # Fixed attributes.
        self.ISIN = isin 
        self.DATE = date

        # General objects.
        self.bids: Dict[float, LimitLevel] = {}
        self.asks: Dict[float, LimitLevel] = {}
        self.best_bid: LimitLevel = None
        self.best_ask: LimitLevel = None
        self._orders: Dict[int, Order] = {}

        # List of orders that exit the orderbook (either canceled or filled) and
        # list of trades.
        self.removed_orders: List[dict] = None
        self.trades: List[dict] = None

        # Containers to store contigent orders. 
        self.valid_for_closing: deque = deque()
        self.valid_for_auctions: list = [] ####new
        self.buy_stop_orders: Dict[float, Dict[str, deque]] = {}
        self.sell_stop_orders: Dict[float, Dict[str, deque]] = {}
        self.pegged_orders: Dict[int, Order] = {}
    
        self.opening_auction = Auction(opening_auction_datetime)
        self.closing_auction = Auction(closing_auction_datetime)

        # Current update data.
        self.current_message_datetime = None
        self.last_trading_price = None
        self.current_order = None
    
    @property
    def is_auction(self):
        return (((self.current_message_datetime > self.opening_auction.datetime) and not self.opening_auction.passed) 
                or ((self.current_message_datetime > self.closing_auction.datetime) and not self.closing_auction.passed))

    @property
    def is_before_auction(self):
        return self.current_message_datetime < self.opening_auction.datetime
    
    @property
    def spread(self):
        return round(self.best_ask.price - self.best_bid.price, 3)
    
    
    def process(self, message: dict) -> None:
        """
        Run auction if needed. Processes the given message (order). If it exists
        within the book, the order is updated. If it doesn't exist, it will be added.
        """
        self.current_message_datetime = message['o_dtm_va']
        
        self._check_for_order_cancelations() 
        self._check_for_auction()
        preprocess_message(message, self.is_before_auction, self.best_bid, self.best_ask)

        # Add or modify an order.
        try:
            self._modify(message)
        except KeyError:
            self._add(message)

        self._update_orderbook()
    
    
    def _check_for_auction(self) -> None:
        """
        Check if it is time for an auction. If so runs the appropriate methods.
        """
        if self.is_auction:
            # Opening auction process.
            self.opening_auction.run_auction(self)
            self.last_trading_price = self.opening_auction.price
            self._trigger_stop_orders()
    

    def _update_orderbook(self):
        """
        Update of the orderbook. Check for trades and update the last trade 
        price accordingly. Check for stop orders to be added to the book.
        """
        if not self.is_before_auction:
            self._check_for_trades()
            self._trigger_stop_orders()


    def _add(self, message) -> None:
        """
        Depending on the order type of the order, it is either added to the book
        or stored as a contigent order. A special case is made to check if the 
        order is only valid for the closing auction.
        """
        order = Order(
            o_id_cha = message['o_id_cha'],
            o_id_fd = message['o_id_fd'],
            o_member = message['o_member'],
            o_account = message['o_account'],
            o_bs = message['o_bs'],
            o_execution = message['o_execution'],
            o_validity = message['o_validity'],
            o_type = message['o_type'],
            o_price = message['o_price'],
            o_price_stop = message['o_price_stop'],
            o_q_ini = message['o_q_ini'],
            o_q_min = message['o_q_min'],
            o_q_dis = message['o_q_dis'],
            o_dt_expiration = message['o_dt_expiration'],
            o_dtm_be = message['o_dtm_be'],
            o_dtm_va = message['o_dtm_va'],
        )

        self.current_order = order #### testing

        if message['o_validity'] == '7': # valid for closing auction
            self.valid_for_closing.append(order)
            self._orders[order.o_id_fd] = order
            return
        elif message['o_validity'] == '2': #### quick way to fix valid for auction
            if self.opening_auction.passed == False:
                self.valid_for_auctions.append(order.o_id_fd)
            else:
                return

        match message['o_type']:
            case '1':
                self._add_limit_order(order)
            case '2':
                self._add_limit_order(order)
            case '3' | '4':
                self._add_stop_order(order)
            case 'P':
                # Add order threshold price (store in stop price)
                order.o_price_stop = order.o_price
                self._add_pegged_order(order)
            case 'K':
                self._add_limit_order(order)


    def _add_limit_order(self, order: Order) -> None:
        """
        Add a limit order to the book. Update or create the limit level.
        """
        if order.o_bs == 'B':
            side = self.bids
        else:
            side = self.asks

        if order.o_price not in side:
            # Limit level must be created
            limit_level = LimitLevel(order)
            self._orders[order.o_id_fd] = order
            side[limit_level.price] = limit_level

            if order.o_bs == 'B':
                if self.best_bid is None or limit_level.price > self.best_bid.price:
                    self.best_bid = limit_level
            else:
                if self.best_ask is None or limit_level.price < self.best_ask.price:
                    self.best_ask = limit_level
        else:
            # Limit level exists
            self._orders[order.o_id_fd] = order
            side[order.o_price].append(order)

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logger.debug(f'{order.o_dtm_va} - Added order to book: {order.o_id_fd}')
    

    def _add_stop_order(self, order: Order) -> None:
        """
        Add stop order to container before it is triggered.
        """
        if order.o_bs == 'B':
            side = self.buy_stop_orders
        else:
            side = self.sell_stop_orders

        if order.o_price_stop not in side:
            # Stop level must be created
            #container = deque() ### old
            #container.append(order) ### old

            ### new
            stop_limit = deque()
            stop_market = deque()

            if order.o_type == '3': # market stop order
                stop_market.append(order)
            else: #limit stop order
                stop_limit.append(order)

            container = {
                'stop_market': stop_market,
                'stop_limit': stop_limit,
            }

            self._orders[order.o_id_fd] = order
            side[order.o_price_stop] = container

        else:
            # Stop level exists
            self._orders[order.o_id_fd] = order
            #side[order.o_price_stop].append(order) ### old

            ### new
            if order.o_price == 100_000 or order.o_price == 0: # market stop order
                side[order.o_price_stop]['stop_market'].append(order)

            else: #limit stop order
                side[order.o_price_stop]['stop_limit'].append(order)


    def _add_pegged_order(self, order: Order) -> None:
        """
        Add pegged order to the book. Store the order in a hash table for fast lookup. 
        Stop price is the limit price the pegged order can go to. 
        """

        # Add order to pegged order hash table
        self.pegged_orders[order.o_id_fd] = order

        # Set the order limit price
        if order.o_bs == 'B':
            order.o_price = min(self.best_bid.price, order.o_price_stop)
        else:
            order.o_price = max(self.best_ask.price, order.o_price_stop)

        # Add the limit order
        self._add_limit_order(order)
        


    def _remove(self, o_id_fd: int) -> None:
        """
        Removes an order from the book. If the Limit Level is then empty, it is 
        also removed. If the removed LimitLevel was either the top bid or ask, 
        it is replaced by the next best price.
        """
        try:
            # Remove order from self._orders
            popped_item = self._orders.pop(o_id_fd)
        except KeyError:
            #raise NotImplementedError
            return False #### for now, let go removal of pegged and stop orders for testing

        #### testing
        # If stop order not triggered. Remove from stop orders list.
        if popped_item.o_type in ('3', '4'):
            
            if popped_item.o_bs == 'B':
                side = self.buy_stop_orders
            else:
                side = self.sell_stop_orders

            try:
                stop_order_dict = side[popped_item.o_price_stop]

                if popped_item.o_type == '3': # market stop order
                    stop_order_dict['stop_market'].remove(popped_item)
                else: #limit stop order
                    stop_order_dict['stop_limit'].remove(popped_item)

                #stop_order_list.remove(popped_item)
                if not stop_order_dict['stop_market'] and not stop_order_dict['stop_limit']:
                    side.pop(popped_item.o_price_stop)

                return popped_item
            
            except KeyError: #### prev ValueError
                # Stop order has been triggered, now a limit order.
                pass
        
        elif popped_item.o_type == 'P':
            self.pegged_orders.pop(o_id_fd)

        #### Testing
        if popped_item.o_validity == '7': # valid for closing auction
            self.valid_for_closing.remove(popped_item)
            return
        
        # Remove order from its doubly linked list
        popped_item.pop_from_list()

        # Remove limit from bids or asks, if no orders are left at that limit
        try:
            if popped_item.o_bs == 'B':
                # Bid
                if len(self.bids[popped_item.o_price]) == 0:
                    popped_limit_level = self.bids.pop(popped_item.o_price)

                    if popped_limit_level == self.best_bid:
                        self._set_best_bid()
            else:
                # Ask
                if len(self.asks[popped_item.o_price]) == 0:
                    popped_limit_level = self.asks.pop(popped_item.o_price)

                    if popped_limit_level == self.best_ask:
                        self._set_best_ask()

        except KeyError:
            raise NotImplementedError #### To be checked
            pass
        
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logger.debug(f'Successfully removed order: {popped_item.o_id_fd}.')
        return popped_item


    def _modify(self, message: dict) -> None:
        """
        Modifies an existing order in the book.
        It also updates the order's related LimitLevel's size, accordingly.
        If order update is a change in price, the order is removed from the
        limit level and a new order is created.
        """
        order = self._orders[message['o_id_fd']]
        self.current_order = order #### testing

        #                           CHANGE IN PRICE
        #-----------------------------------------------------------------------
        if order.o_price != message['o_price']:
            # Change in price, remove order, and add new one with new price
            #quantity_left = message['o_q_ini'] - order.o_q_neg
            #message['o_q_ini'] = quantity_left
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f'{message["o_dtm_va"]} - Modified order price: {order.o_id_fd}')
            q_neg = order.o_q_neg

            self._remove(message['o_id_fd'])
            self._add(message)

            ##### new
            order = self._orders[message['o_id_fd']]
            order.overwrite_quantity_negociated(q_neg)


        #                        CHANGE IN PRICE STOP
        #-----------------------------------------------------------------------
        elif order.o_price_stop != message['o_price_stop']:
            # Remove order from orders list.
            popped_order = self._orders.pop(message['o_id_fd'])

            # Remove order from stop orders list
            side = self.buy_stop_orders if popped_order.o_bs == 'B' else self.sell_stop_orders
            stop_order_dict = side[popped_order.o_price_stop]

            if popped_order.o_type == '3': # market stop order
                stop_order_dict['stop_market'].remove(popped_order)
            else: #limit stop order
                stop_order_dict['stop_limit'].remove(popped_order)

            #stop_order_list.remove(popped_item)
            if not stop_order_dict['stop_market'] and not stop_order_dict['stop_limit']:
                side.pop(popped_order.o_price_stop)

            # add stop order with new price
            self._add(message)

        #                         CHANGE IN QUANTITY
        #-----------------------------------------------------------------------
        #elif order.o_q_rem != message['o_q_rem']:
        elif order.o_q_ini != message['o_q_ini']:
            # Change in quantity
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f'{message["o_dtm_va"]} - Modified order quantity: {order.o_id_fd}')
            #### check what happens when order is partially filled (so far has not happened) o_state = '1'

            size_diff = message['o_q_ini'] - order.o_q_ini

            # Update order attributes
            order.o_q_ini = message['o_q_ini']
            order.o_q_rem += size_diff
            order.o_q_min = message['o_q_min']

            # Compute impact on limit level
            size_dis_diff = min(order.o_q_rem, message['o_q_dis']) - order.o_q_dis
            size_hid_diff = size_diff - size_dis_diff

            # Update order q-displayed
            order.o_q_dis = min(order.o_q_rem, message['o_q_dis'])

            # Update limit level attributes
            if order.root != None:                 
                # Root is equal to None for stop orders not triggered. ####
                order.parent_limit.size += size_diff
                order.parent_limit.disclosed_size_hft += size_dis_diff if order.o_member == 'HFT' else 0
                order.parent_limit.disclosed_size_mixed += size_dis_diff if order.o_member == 'MIX' else 0
                order.parent_limit.disclosed_size_non += size_dis_diff if order.o_member == 'NON' else 0
                order.parent_limit.hidden_size_hft += size_hid_diff if order.o_member == 'HFT' else 0
                order.parent_limit.hidden_size_mixed += size_hid_diff if order.o_member == 'MIX' else 0
                order.parent_limit.hidden_size_non += size_hid_diff if order.o_member == 'NON' else 0

        elif order.o_dt_expiration != message['o_dt_expiration']:
            order.o_dt_expiration = message['o_dt_expiration']

        #elif message['o_dtm_va'].date() < self.DATE:
            # History file (historic modifications), sometimes no changes.
            # Only extension of maturity.
        #    pass 

        else:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logger.error(f'{message["o_dtm_va"]} - Change not handled: {message}')
            pass
            #raise NotImplementedError


    def _trigger_stop_orders(self) -> None:
        """
        For bid, check if the last trading price is above the stop, if so, 
        trigger the stop orders. For ask, check if the last trading price is 
        below the stop, if so, trigger the stop orders.
        """
        buy_stop_prices_triggered = sorted(
            [stop_price for stop_price in self.buy_stop_orders.keys() if stop_price <= self.last_trading_price], 
            reverse=False
        )
        sell_stop_prices_triggered = sorted(
            [stop_price for stop_price in self.sell_stop_orders.keys() if stop_price >= self.last_trading_price], 
            reverse=True
        )

        if len(buy_stop_prices_triggered) == 0 and len(sell_stop_prices_triggered) == 0: return

        #### debugging
        #print(buy_stop_prices_triggered)
        #print(self.buy_stop_orders.keys())

        buy_stop_orders_triggered = {stop_price: self.buy_stop_orders.pop(stop_price) for stop_price in buy_stop_prices_triggered}
        sell_stop_orders_triggered = {stop_price: self.sell_stop_orders.pop(stop_price) for stop_price in sell_stop_prices_triggered}

        #orders_triggered = deque()

        #for stop_price in buy_stop_prices_triggered:
        #    orders = self.buy_stop_orders.pop(stop_price)
        for stop_price in list(buy_stop_orders_triggered):
            stop_orders_dict = buy_stop_orders_triggered.pop(stop_price)
            orders = stop_orders_dict['stop_market'] + stop_orders_dict['stop_limit']
            while orders:
                popped_order = orders.popleft()
                # Add limit or market order
                self._add_limit_order(popped_order)
                #orders_triggered.append(popped_order)
                self.current_order = popped_order
                self._check_for_trades()
            
            # Remove stop level
            #self.buy_stop_orders.pop(stop_price)

        #for stop_price in sell_stop_prices_triggered:
        #    orders = self.sell_stop_orders.pop(stop_price)
        for stop_price in list(sell_stop_orders_triggered):
            stop_orders_dict = sell_stop_orders_triggered.pop(stop_price)
            orders = stop_orders_dict['stop_market'] + stop_orders_dict['stop_limit']
            while orders:
                popped_order = orders.popleft()
                # Add limit or market order
                self._add_limit_order(popped_order)
                #orders_triggered.append(popped_order)
                self.current_order = popped_order
                self._check_for_trades()
            # Remove stop level
            #self.sell_stop_orders.pop(stop_price)
        
        
        self._trigger_stop_orders()


    def _fill_order(self, o_id_fd: int, trade_quantity: int) -> None: 
        """ Change order and limit level attributes after being filled by a trade. """

        # Get order object
        try:
            order = self._orders[o_id_fd]
        except KeyError:
            raise NotImplementedError

        new_quantity = order.o_q_rem - trade_quantity

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logger.debug(f'Successfully filled: {order.o_id_fd}; q_traded: {trade_quantity}; q_left: {new_quantity}.')

        # Order is filled entirely 
        if new_quantity == 0:
            
            # Remove order
            self._remove(order.o_id_fd)

            # Update best limit
            if order.o_bs == 'B':
                self._set_best_bid()
            else:
                self._set_best_ask()
        
        # Not filled entirely, update limit order book and order.
        elif new_quantity > 0:
            
            old_q_dis = order.o_q_dis

            # Update order attributes
            order.o_q_rem -= trade_quantity
            order.o_q_neg += trade_quantity
            order.o_q_dis = min(order.o_q_dis, order.o_q_rem)
            #order.o_q_hid = order.o_q_rem - order.o_q_dis

            # Update limit level attributes
            impact_q_dis = old_q_dis - order.o_q_dis
            impact_q_hid = trade_quantity - impact_q_dis

            order.parent_limit.size -= trade_quantity
            order.parent_limit.disclosed_size_hft -= (impact_q_dis if order.o_member == 'HFT' else 0)
            order.parent_limit.disclosed_size_mixed -= (impact_q_dis if order.o_member == 'MIX' else 0)
            order.parent_limit.disclosed_size_non -= (impact_q_dis if order.o_member == 'NON' else 0)
            order.parent_limit.hidden_size_hft -= (impact_q_hid if order.o_member == 'HFT' else 0)
            order.parent_limit.hidden_size_mixed -= (impact_q_hid if order.o_member == 'MIX' else 0)
            order.parent_limit.hidden_size_non -= (impact_q_hid if order.o_member == 'NON' else 0)
        
        else: #### to be deleted once we are sure this is not called
            raise NotImplementedError
        
    
    def _update_pegged_orders(self) -> None:
        """ Change pegged orders' price if needed. """
        pegged_orders_id = list(self.pegged_orders)
        for pegged_order_id in pegged_orders_id:

            # Get order
            order = self._orders[pegged_order_id]
            q_neg = order.o_q_neg # if any

            if order.o_bs == 'B':
                # Bid 
                if order.o_price < order.o_price_stop:
                    self._remove(order.o_id_fd)
                    self._add_pegged_order(order.reset())
            else:
                # Ask
                if order.o_price > order.o_price_stop:
                    self._remove(order.o_id_fd)
                    self._add_pegged_order(order.reset())

            ##### new
            order = self._orders[order.o_id_fd]
            order.overwrite_quantity_negociated(q_neg)


    def _check_for_order_cancelations(self, limit: dt.datetime=None) -> None:
        """ Check for canceled orders since the last message. Removes them if any. """
        datetime_limit = limit if limit != None else self.current_message_datetime

        while self.removed_orders[-1]['o_dtm_br'] < datetime_limit:
            removed_order = self.removed_orders.pop()
            if removed_order['o_state'] != '2':
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logger.debug(f'{removed_order["o_dtm_br"]} - Order cancelled: {removed_order["o_id_fd"]}.')
                self._remove(removed_order['o_id_fd'])
                
    
    def _check_for_trades(self) -> None:
        """
        Check for trades since the last message. 
        Updates the orders and limit levels involved if any trades.
        """
        current_id = self.current_order.o_id_fd
        current_price = self.current_order.o_price

        if self.current_order.o_bs == 'B':
            condition_1 = (self.trades[-1]['t_id_b_fd'] == current_id) and (self.trades[-1]['t_agg'] == 'A')
            condition_2 = (current_price >= self.trades[-1]['t_price'])
            
        else:
            condition_1 = (self.trades[-1]['t_id_s_fd'] == current_id) and (self.trades[-1]['t_agg'] == 'V')
            condition_2 = (current_price <= self.trades[-1]['t_price'])

        
        
        while condition_1 and condition_2 or self.trades[-1]['t_agg'] == '2':
            
            # Handle case where order is  next agressive order to trade but price is not aggressive yet (ie, change in price later that will make it aggresive)
            

            if self.trades[-1]['t_agg'] == '2':
                try:
                    self._orders[self.trades[-1]['t_id_b_fd']]
                    self._orders[self.trades[-1]['t_id_s_fd']]
                except KeyError:
                    break


            trade = self.trades.pop()
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logger.debug(f'{trade["t_dtm_neg"]} - Trade between {trade["t_id_b_fd"]} and {trade["t_id_s_fd"]}.')
            self._fill_order(trade['t_id_b_fd'], trade['t_q_exchanged'])
            self._fill_order(trade['t_id_s_fd'], trade['t_q_exchanged'])

            if self.last_trading_price != trade['t_price']:
                self.last_trading_price = trade['t_price']
                self._update_pegged_orders()

            # Reset condition
            current_id = self.current_order.o_id_fd
            current_price = self.current_order.o_price
            
            if self.current_order.o_bs == 'B':
                condition_1 = (self.trades[-1]['t_id_b_fd'] == current_id) and (self.trades[-1]['t_agg'] == 'A')
                condition_2 = (current_price >= self.trades[-1]['t_price'])
                
            else:
                condition_1 = (self.trades[-1]['t_id_s_fd'] == current_id) and (self.trades[-1]['t_agg'] == 'V')
                condition_2 = (current_price <= self.trades[-1]['t_price'])

        ####if self.opening_auction.passed: self._trigger_stop_orders()
        #self._trigger_stop_orders()
    
    
    def set_removed_orders(self, df_removed_orders: pd.DataFrame) -> None:
        """ Reversed list of removed orders. """
        df_removed_orders_reversed = df_removed_orders.sort_values(by='o_dtm_br', ascending=False)
        self.removed_orders = list(df_removed_orders_reversed.to_dict('records'))


    def set_trades(self, df_trades: pd.DataFrame) -> None:
        """ Reversed list of trades. """
        fields = ['t_dtm_neg', 't_id_b_fd', 't_id_s_fd', 't_q_exchanged', 't_price', 't_agg']
        df_trades = df_trades[fields]
        df_trades_reversed = df_trades.sort_values(by='t_dtm_neg', ascending=False)
        self.trades = list(df_trades_reversed.to_dict('records'))

    
    def _set_best_bid(self) -> None:
        """ Sets best bid after order deletion by cancelation or trade. """
        if len(self.bids) > 0:
            self.best_bid = self.bids[max(self.bids.keys())]
        else:
            self.best_bid = None
    

    def _set_best_ask(self) -> None:
        """ Sets best ask after order deletion by cancelation or trade. """
        if len(self.asks) > 0:
            self.best_ask = self.asks[min(self.asks.keys())]
        else:
            self.best_ask = None


    def get_levels(
        self, depth: int=None, detailed: bool=False
    ) -> Dict[str, Dict[float, int]]:
        """ Returns the price levels as a dict {'bids': {bid1, q1}, ...], 
        'asks': {ask1, q1}, ...]}

        Args:
            depth (int, optional): number of levels (per side) required. 
                Defaults to None.
            detailed (bool, optional): if true returns detailed quantity else 
                returns total quantity. Defaults to False.

        Returns:
            Dict[str, Dict[float, int]]: levels and their details.
        """
        bids_sorted = sorted(self.bids.keys(), reverse=True)
        asks_sorted = sorted(self.asks.keys())
        bids = list(islice(bids_sorted, depth)) if depth else list(bids_sorted)
        asks = list(islice(asks_sorted, depth)) if depth else list(asks_sorted)

        if detailed:
            levels_dict = {
                'bids' : [self.bids[bid] for bid in bids],
                'asks' : [self.asks[ask] for ask in asks]
                }
        else:
            levels_dict = {
                'bids' : {bid: self.bids[bid].size for bid in bids},
                'asks' : {ask: self.asks[ask].size for ask in asks}
                }
        
        return levels_dict