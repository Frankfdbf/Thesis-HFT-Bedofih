# Import Built-Ins
import datetime as dt
from typing import Dict, List
from itertools import islice
from collections import deque

# Import Third-Party
import pandas as pd 
import numpy as np

# Import Homebrew
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
    #### It should handle auction on its own.
    """

    def __init__(self, date: dt.date, isin: str, opening_auction_datetime: dt.datetime, closing_auction_datetime: dt.datetime):
        # Fixed attributes.
        self.ISIN = isin 
        self.DATE = date

        # General objects.
        self.bids: Dict[float, LimitLevel] = {}
        self.asks: Dict[float, LimitLevel] = {}
        self.best_bid: LimitLevel = None
        self.best_ask: LimitLevel = None
        self._orders: Dict[float, LimitLevel] = {}

        # List of orders that exit the orderbook (either canceled or filled)
        self.removed_orders: List[dict] = None

        # Containers to store contigent orders. 
        self.valid_for_closing: deque = deque()
        self.buy_stop_orders: Dict[float, deque] = {}
        self.sell_stop_orders: Dict[float, deque] = {}

        # Object to store all trades of session (for testing).
        self.df_trades = pd.DataFrame() #### temporary
        self.trades: List[Trade] = []
    
        # Auction data
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

    
    def process(self, message: dict) -> None:
        """
        Run auction if needed. Processes the given message (order). If it exists
        within the book, the order is updated. If it doesn't exist, it will be added.
        """
        self.current_message_datetime = message['o_dtm_va']
        self._remove_canceled_orders()
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
            # Start registering trades
            #return
            self._make_trades()
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

        match message['o_type']:
            case '1':
                self._add_limit_order(order)
            case '2':
                self._add_limit_order(order)
            case '3' | '4':
                self._add_stop_order(order)
            case 'P':
                self._add_pegged_order(order)
                raise NotImplementedError
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
            container = deque()
            container.append(order)
            self._orders[order.o_id_fd] = order
            side[order.o_price_stop] = container

        else:
            # Stop level exists
            self._orders[order.o_id_fd] = order
            side[order.o_price_stop].append(order)


    def _add_pegged_order(self, order: Order) -> None:
        """
        details
        """
        pass #### To be done


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
            try:
                if popped_item.o_bs == 'B':
                    stop_order_list = self.buy_stop_orders[popped_item.o_price_stop]
                    stop_order_list.remove(popped_item)
                    if not stop_order_list:
                        self.buy_stop_orders.pop(popped_item.o_price_stop)
                else:
                    stop_order_list = self.sell_stop_orders[popped_item.o_price_stop]
                    stop_order_list.remove(popped_item)
                    if not stop_order_list:
                        self.sell_stop_orders.pop(popped_item.o_price_stop)
                return popped_item
            
            except KeyError: #### prev ValueError
                # Stop order has been triggered, now a limit order.
                pass

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
                        if len(self.bids) > 0:
                            self.best_bid = self.bids[max(self.bids.keys())]
                        else:
                            self.best_bid = None
            else:
                # Ask
                if len(self.asks[popped_item.o_price]) == 0:
                    popped_limit_level = self.asks.pop(popped_item.o_price)

                    if popped_limit_level == self.best_ask:
                        if len(self.asks) > 0:
                            self.best_ask = self.asks[min(self.asks.keys())]
                        else:
                            self.best_ask = None

        except KeyError:
            raise NotImplementedError #### To be checked
            pass

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
        
        if order.o_price != message['o_price']:
            # Change in price, remove order, and add new one with new price
            self._remove(message['o_id_fd'])
            self._add(message)

        elif order.o_price_stop != message['o_price_stop']:
            # Change in stop price
            #### In testing
            # Remove order from orders list.
            popped_order = self._orders.pop(message['o_id_fd'])
            # Remove order from stop orders list
            if message['o_bs'] == 'B':
                stop_order_list = self.buy_stop_orders[popped_order.o_price_stop]
                stop_order_list.remove(popped_order)
                if not stop_order_list:
                    self.buy_stop_orders.pop(popped_order.o_price_stop)
            else:
                stop_order_list = self.sell_stop_orders[popped_order.o_price_stop]
                stop_order_list.remove(popped_order)
                if not stop_order_list:
                    self.sell_stop_orders.pop(popped_order.o_price_stop)
            # add stop order with new price
            self._add(message)
            #raise NotImplementedError

        #elif order.o_q_rem != message['o_q_rem']:
        elif order.o_q_ini != message['o_q_ini']:
            # Change in quantity
            #### check what happens when order is partially filled (so far has not happened) o_state = '1'
            size_diff = message['o_q_ini'] - order.o_q_ini
            size_dis_diff = message['o_q_dis'] - order.o_q_dis
            size_hid_diff = size_diff - size_dis_diff

            # Update order attributes
            order.o_q_ini = message['o_q_ini']
            order.o_q_rem = message['o_q_ini']
            order.o_q_neg = 0
            order.o_q_min = message['o_q_min']
            order.o_q_dis = message['o_q_dis']

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

        elif message['o_dtm_va'].date() < self.DATE:
            pass # History file (historic modifications), sometimes no changes

        else:
            print(message)
            raise NotImplementedError


    def _make_trades(self) -> None:
        """
        Check if there is a trade. Executes orders if needed.
        """
        ####debugging
        #print(self.buy_stop_orders)
        #print(self.sell_stop_orders)

        # Exclude stop not triggered and orders for closing auction.
        if not self.current_order.root: return
        
        while self.current_order.o_q_rem > 0:
            
            # Set variables
            if self.current_order.o_bs == 'B':
                # Break if there are no order facing.
                if self.best_ask == None: return

                # Set bid and ask order, as well as trade price
                order_bid = self.current_order
                order_ask = self.best_ask.orders.head
                trade_price = order_ask.o_price
                t_agg = 'B'
            else:
                # Break if there are no order facing.
                if self.best_bid == None: return

                # Set bid and ask order, as well as trade price
                order_bid = self.best_bid.orders.head
                order_ask = self.current_order
                trade_price = order_bid.o_price
                t_agg = 'S'

            # Break if no trades are possible
            if order_bid.o_price < order_ask.o_price: return
            ####testing
            
            #### debugging
            print(self.get_levels(5))
            print(self.best_bid.orders)
            print(self.best_ask.orders)
            


            trade_quantity = min(order_bid.o_q_rem, order_ask.o_q_rem)
            self._fill_order(order_bid.o_id_fd, trade_quantity)
            self._fill_order(order_ask.o_id_fd, trade_quantity)

            # Register the trade
            trade = Trade(
                bid_id=order_bid.o_id_fd,
                ask_id=order_ask.o_id_fd,
                quantity=trade_quantity,
                price=trade_price,
                t_agg=t_agg,
                bid_member=order_bid.o_member,
                ask_member=order_ask.o_member,
                dtm_neg=self.current_message_datetime)
            self.trades.append(trade)

            #### debugging
            row = {
                'bid_id': order_bid.o_id_fd,
                'ask_id': order_ask.o_id_fd,
                'quantity': trade_quantity,
                'price': trade_price,
                't_agg': t_agg,
                'bid_member': order_bid.o_member,
                'ask_member': order_ask.o_member,
                'dtm_neg': self.current_message_datetime
            }
            self.df_trades = pd.concat([self.df_trades, pd.DataFrame([row])], ignore_index=True)
            #### debugging

            if order_bid.o_q_rem == 0:
                # Order bid is filled completely
                self._remove(order_bid.o_id_fd)

                if len(self.bids) > 0:
                    self.best_bid = self.bids[max(self.bids.keys())]
                else:
                    self.best_bid = None

            if order_ask.o_q_rem == 0:
                # Order ask is filled completely
                self._remove(order_ask.o_id_fd)

                if len(self.asks) > 0:
                    self.best_ask = self.asks[min(self.asks.keys())]
                else:
                    self.best_ask = None


    def _trigger_stop_orders(self) -> None:
        """
        For bid, check if the last trading price is above the stop, if so, 
        trigger the stop orders. For ask, check if the last trading price is 
        below the stop, if so, trigger the stop orders.
        """
        buy_stop_prices_triggered = sorted(
            [stop_price for stop_price in self.buy_stop_orders.keys() if stop_price < self.last_trading_price], 
            reverse=True
        )
        sell_stop_prices_triggered = sorted(
            [stop_price for stop_price in self.sell_stop_orders.keys() if stop_price > self.last_trading_price], 
            reverse=False
        )

        for stop_price in buy_stop_prices_triggered:
            orders = self.buy_stop_orders[stop_price]
            while orders:
                popped_order = orders.popleft()
                # Add limit or market order
                self._add_limit_order(popped_order)
            # Remove stop level
            self.buy_stop_orders.pop(stop_price)

        for stop_price in sell_stop_prices_triggered:
            orders = self.sell_stop_orders[stop_price]
            while orders:
                popped_order = orders.popleft()
                # Add limit or market order
                self._add_limit_order(popped_order)
            # Remove stop level
            self.sell_stop_orders.pop(stop_price)


    def _fill_order(self, o_id_fd: int, trade_quantity: int) -> None: 
        """
        Change order and limit level attributes after being filled by a trade.
        """
        order = self._orders[o_id_fd]

        # Update order attributes
        order.o_q_rem -= trade_quantity
        order.o_q_neg += trade_quantity
        #order.o_q_min ####
        #order.o_q_dis

        # Update limit level attributes
        order.parent_limit.size -= trade_quantity
        #order.parent_limit.disclosed_size_hft += size_dis_diff if order.o_member == 'HFT' else 0
        #order.parent_limit.disclosed_size_mixed += size_dis_diff if order.o_member == 'MIX' else 0
        #order.parent_limit.disclosed_size_non += size_dis_diff if order.o_member == 'NON' else 0
        #order.parent_limit.hidden_size_hft += size_hid_diff if order.o_member == 'HFT' else 0
        #order.parent_limit.hidden_size_mixed += size_hid_diff if order.o_member == 'MIX' else 0
        #order.parent_limit.hidden_size_non += size_hid_diff if order.o_member == 'NON' else 0


    def _remove_canceled_orders(self) -> None:
        """
        Check for canceled orders since the last message. Removes them if any.
        """
        while self.removed_orders[-1]['o_dtm_br'] < self.current_message_datetime:
            removed_order = self.removed_orders.pop()
            if removed_order['o_state'] != '2':
                self._remove(removed_order['o_id_fd'])


    def set_removed_orders(self, df_removed_orders: pd.DataFrame) -> None:
        """
        Reversed list of removed orders.
        """
        df_removed_orders_reversed = df_removed_orders.sort_values(by='o_dtm_br', ascending=False)
        self.removed_orders = list(df_removed_orders_reversed.to_dict('records'))


    def get_levels(self, depth: int=None) -> Dict[str, Dict[float, int]]:
        """
        Returns the price levels as a dict {'bids': {bid1, q1}, ...], 'asks': {ask1, q1}, ...]}
        """
        bids_sorted = sorted(self.bids.keys(), reverse=True)
        asks_sorted = sorted(self.asks.keys())
        bids = list(islice(bids_sorted, depth)) if depth else list(bids_sorted)
        asks = list(islice(asks_sorted, depth)) if depth else list(asks_sorted)

        levels_dict = {
            'bids' : {bid: self.bids[bid].size for bid in bids},
            'asks' : {ask: self.asks[ask].size for ask in asks}
            }
        
        return levels_dict