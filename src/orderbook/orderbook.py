# Import Built-Ins
import datetime as dt
from typing import Dict, List
from itertools import islice

# Import Third-Party
import pandas as pd 
import numpy as np

# Import Homebrew
from .order import Order
from .trade import Trade 
from .limit_level import LimitLevel

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

    def __init__(self, date: dt.date, isin: str) -> None:
        self.bids: Dict[float, LimitLevel] = {}
        self.asks: Dict[float, LimitLevel] = {}
        self.best_bid: LimitLevel = None
        self.best_ask: LimitLevel = None
        self._orders: Dict[int: Order] = {}

        self.isin = isin 
        self.date = date

        self.current_message_datetime = None
        self.last_trading_price = None 
        
        # List of orders that exit the orderbook (either canceled or filled)
        self.removed_orders: List[dict] = None

        # List to store all trades of session
        self.df_trades = pd.DataFrame() #### temporary
        self.trades: List[Trade] = []
    
        # Objects to store orders that do enter the orderbook as soon they are sent. 
        #self.market_limit_orders = [] #### check if needed
        self.valid_for_closing = []
        self.buy_stop_orders = []
        self.sell_stop_orders = []

        # Auction data
        self.auction_datetime1 = None
        self.auction1_passed = False
        self.auction1_price = None

        self.auction_datetime2 = None
        self.auction2_passed = False
        self.auction2_price = None


    @property
    def is_auction(self):
        return (((self.current_message_datetime > self.auction_datetime1) and not self.auction1_passed) 
                or ((self.current_message_datetime > self.auction_datetime2) and not self.auction2_passed))


    @property
    def is_before_auction(self):
        return self.current_message_datetime < self.auction_datetime1

    
    def process(self, message: dict) -> None:
        """
        Run auction if needed.
        Processes the given message (order).
        If it exists within the book, the order is updated.
        If it doesn't exist, it will be added.
        """
        self.current_message_datetime = message['o_dtm_va']
        
        # Check for canceled orders.
        self._remove_canceled_orders()
        
        if self.is_auction:
            # Opening auction process.
            self._run_auction()
            self.auction1_passed = True

        # Add or modify an order.
        message = self._clean_message(message)
        try:
            self._modify(message)
        except KeyError:
            self._add(message)

        # Update orderbook.
        self._update()


    def _add(self, message) -> None:
        """
        Details
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

        if message['o_validity'] == '7': # valid for closing auction
            self.valid_for_closing.append(order)
            return

        match message['o_type']:
            case '1':
                self._add_limit_order(order)
            case '2':
                self._add_limit_order(order)
            case ['3', '4']:
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
        details
        """
        pass #### To be done


    def _add_pegged_order(self, order: Order) -> None:
        """
        details
        """
        pass #### To be done


    def _remove(self, o_id_fd: int) -> None:
        """
        Removes an order from the book.
        If the Limit Level is then empty, it is also removed from the book's
        relevant tree.
        If the removed LimitLevel was either the top bid or ask, it is replaced
        by the next best price.
        """
        try:
            # Remove order from self._orders
            popped_item = self._orders.pop(o_id_fd)
        except KeyError:
            #raise NotImplementedError
            return False #### for now, let go removal of pegged and stop orders

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
        
        if order.o_price != message['o_price']:
            # Change in price, remove order, and add new one with new price
            self._remove(message['o_id_fd'])
            self._add(message)

        elif order.o_price_stop != message['o_price_stop']:
            # Change in stop price
            raise NotImplementedError

        elif order.o_q_rem != message['o_q_rem']:
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
            order.parent_limit.size += size_diff
            order.parent_limit.disclosed_size_hft += size_dis_diff if order.o_member == 'HFT' else 0
            order.parent_limit.disclosed_size_mixed += size_dis_diff if order.o_member == 'MIX' else 0
            order.parent_limit.disclosed_size_non += size_dis_diff if order.o_member == 'NON' else 0
            order.parent_limit.hidden_size_hft += size_hid_diff if order.o_member == 'HFT' else 0
            order.parent_limit.hidden_size_mixed += size_hid_diff if order.o_member == 'MIX' else 0
            order.parent_limit.hidden_size_non += size_hid_diff if order.o_member == 'NON' else 0

        elif message['o_dtm_va'].date() < self.date:
            pass # History file (historic modifications), sometimes no changes

        else:
            print(message)
            raise NotImplementedError


    def _update(self):
        """
        Update of the orderbook. Check for trades and update the last trade 
        price accordingly. Check for stop orders to be added to the book.
        """
        #### add stop orders trigger
        if not self.is_before_auction:
            # Start registering trades
            pass


    def _remove_canceled_orders(self) -> None:
        """
        Check for canceled orders. Removes them if any.
        """
        while self.removed_orders[-1]['o_dtm_br'] < self.current_message_datetime:
            # Remove canceled orders
            removed_order = self.removed_orders.pop()
            if removed_order['o_state'] != '2':
                self._remove(removed_order['o_id_fd'])
    

    def _run_auction(self) -> None:
        """
        Run auction, set auction price. Make trades.
        """
        # Get auction price
        price = self._calculate_auction_price()
        self.auction1_price = price
        self.last_trading_price = price
        
        # Makes trades
        invalid_bid_quantity = 0
        invalid_ask_quantity = 0
        order_bid = self.best_bid.orders.head
        order_ask = self.best_ask.orders.head

        while order_bid.o_price >= self.auction1_price and order_ask.o_price <= self.auction1_price:
            trade_quantity = min(order_bid.o_q_rem, order_ask.o_q_rem)
            self._fill_order(order_bid.o_id_fd, trade_quantity)
            self._fill_order(order_ask.o_id_fd, trade_quantity)

            # Register the trade
            trade = Trade(
                bid_id=order_bid.o_id_fd,
                ask_id=order_ask.o_id_fd,
                quantity=trade_quantity,
                price=self.auction1_price,
                bid_member=order_bid.o_member,
                ask_member=order_ask.o_member,
                dtm_neg=self.auction_datetime1)
            self.trades.append(trade)

            #### debugging
            row = {
                'bid_id': order_bid.o_id_fd,
                'ask_id': order_ask.o_id_fd,
                'quantity': trade_quantity,
                'price': self.auction1_price,
                'bid_member': order_bid.o_member,
                'ask_member': order_ask.o_member,
                'dtm_neg': self.auction_datetime1
            }
            self.df_trades = pd.concat([self.df_trades, pd.DataFrame([row])], ignore_index=True)
            #### debugging

            if order_bid.o_q_rem == 0:
                # Order bid is filled completely
                self._remove(order_bid.o_id_fd)

                if len(self.bids) > 0:
                    self.best_bid = self.bids[max(self.bids.keys())]
                    current_bid = self.best_bid
                    order_bid = current_bid.orders.head
                else:
                    self.best_bid = None

            if order_ask.o_q_rem == 0:
                # Order ask is filled completely
                self._remove(order_ask.o_id_fd)

                if len(self.asks) > 0:
                    self.best_ask = self.asks[min(self.asks.keys())]
                    current_ask = self.best_ask
                    order_ask = current_ask.orders.head
                else:
                    self.best_ask = None

            if self.best_bid == None or self.best_ask == None:
                break
        
        #### debugging
        #self.df_trades.to_csv('/Users/australien/Desktop/estimated_trades.csv')


    def _fill_order(self, o_id_fd: int, trade_quantity: int) -> None: 
        """
        Change quantity remaining of an order after being partially or 
        completely filled by a trade.
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


    def _calculate_auction_price(self) -> float:
        """
        Calculate the auction price. Only take into account limit, market limit 
        and market orders. We implement three rules to find the auction price.
        """
        # Compute cumulative shares 
        bid_cum_qty: Dict[float, int] = {}
        ask_cum_qty: Dict[float, int] = {}

        for price in self.bids.keys():
            bid_cum_qty[price] = 0
            for p, data in self.bids.items():
                if p >= price:
                    bid_cum_qty[price] += data.size

        for price in self.asks.keys():
            ask_cum_qty[price] = 0
            for p, data in self.asks.items():
                if p <= price:
                    ask_cum_qty[price] += data.size
        
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
        df_auction['quantity_left'] = df_auction['ask_q_cumulative'] - df_auction['bid_q_cumulative']

        # First rule (i.e. maximise q exchanged)
        max_quantity_exchanged = df_auction['quantity_exchanged'].max(axis=0)
        df_auction_max_quantity_exchanged = df_auction[df_auction.quantity_exchanged == max_quantity_exchanged]

        # Second rule (i.e. minimise q left)
        minimum_quantity_left = df_auction_max_quantity_exchanged.quantity_left.abs().min(axis=0)

        # Thirt rule (i.e. max p if q left is seller, min p if q left is buyer)
        df_minimum_quantity_left = df_auction_max_quantity_exchanged.loc[df_auction_max_quantity_exchanged.quantity_left.abs() == minimum_quantity_left]

        if len(df_minimum_quantity_left.quantity_left.value_counts()) > 1:
            print('AUCTION: Need to implement fourth rule to find price.')
            raise NotImplementedError

        if minimum_quantity_left > 0:
            auction_price = df_minimum_quantity_left.index.min()
        else:
            auction_price = df_minimum_quantity_left.index.max()

        return auction_price


    def _clean_message(self, message: dict) -> dict:
        """
        Modifies price for market, stop market and market to limit orders (init 0).
        For market and market to limit orders during auction, price are 0 or 100k.
        For market limit orders during continuous trading, price equals best bid
        (ask) for sell (ask) orders.
        Do other preprocessing of the message.
        """
        if self.is_before_auction: 
            if message['o_type'] in ('1', 'K', '3') and message['o_price'] == 0.0: 
                # market, stop market, and market to limit order (not already limit)
                if message['o_bs'] == 'B':
                    message['o_price'] = 100_000
                elif message['o_bs'] == 'S':
                    message['o_price'] = 0.0
        else:
            if message['o_type'] in ('1', '3'): 
                # market order and stop market
                if message['o_bs'] == 'B':
                    message['o_price'] = 100_000
                elif message['o_bs'] == 'S':
                    message['o_price'] = 0.0
                    
            elif message['o_type'] == 'K': 
                # market-to-limit order
                if message['o_bs'] == 'B':
                    message['o_price'] = self.best_ask.price   
                elif message['o_bs'] == 'S':
                    message['o_price'] = self.best_bid.price
        
        # Other preprocessing 
        message = preprocess_message(message)
        return message
    

    def set_removed_orders(self, removed_orders: pd.DataFrame) -> None:
        """
        Pass as argument dataframe of removed orders. Returns a list with last
        item is first deletion. The list is used to check for canceled orders.
        """
        removed_orders_reversed = removed_orders.sort_values(by='o_dtm_br', ascending=False)
        self.removed_orders = list(removed_orders_reversed.to_dict('records'))
    

    def set_auction_datetime1(self, time: dt.time) -> None:
        self.auction_datetime1 = time 


    def set_auction_datetime2(self, time: dt.time) -> None:
        self.auction_datetime2 = time 


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