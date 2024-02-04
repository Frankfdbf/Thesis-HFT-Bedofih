# Import Built-Ins
import datetime as dt
from typing import Dict

# Import Third-Party
import pandas as pd 
import numpy as np
import math

# Import Homebrew
from logger import logger
from .limit_level import LimitLevel
from .trade import Trade


class Auction:
    """
    Auction object.
    Keep information such as auction time, the auction price or if the auction 
    has passed. It also performs the related actions such as finding the auction
    price and making the auction trades.
    """
    def __init__(self, datetime: dt.datetime):
        self.datetime = datetime
        self.passed = False
        self.price = None


    def run_auction(self, orderbook):
        """
        Runs auction process: sets auction price, makes auction trades.
        """
        self._calculate_uncrossing_price(orderbook.bids, orderbook.asks)
        self._execute_auction_trades(orderbook)
        self.passed = True

        logger.debug(f'Opening auction - {self.datetime} - Passed.')

        while len(orderbook.valid_for_auctions) > 0:
            order_id = orderbook.valid_for_auctions.pop()
            orderbook._remove(order_id)

    
    def _calculate_uncrossing_price(self, bids: Dict[float, LimitLevel], 
                                    asks: Dict[float, LimitLevel]) -> None:
        """
        Calculate the auction price. Only take into account limit, market limit 
        and market orders. We implement three rules to find the auction price.
        """
        # Compute cumulative shares 
        bid_cum_qty: Dict[float, int] = {}
        ask_cum_qty: Dict[float, int] = {}

        for price in bids.keys():
            bid_cum_qty[price] = 0
            for p, data in bids.items():
                if p >= price:
                    bid_cum_qty[price] += data.size

        for price in asks.keys():
            ask_cum_qty[price] = 0
            for p, data in asks.items():
                if p <= price:
                    ask_cum_qty[price] += data.size
        
        # Auction price determination
        df_bid_cum_quantity = pd.DataFrame(index=bid_cum_qty.keys(), data=bid_cum_qty.values(), columns=['bid_q_cumulative'])
        df_ask_cum_quantity = pd.DataFrame(index=ask_cum_qty.keys(), data=ask_cum_qty.values(), columns=['ask_q_cumulative'])
        df_auction = pd.concat([df_ask_cum_quantity, df_bid_cum_quantity])
        df_auction.index = df_auction.index.astype(float)

        # Sort by prices & merge rows with same prices
        df_auction.sort_index(inplace=True)
        df_auction = df_auction.groupby(by=df_auction.index).sum(min_count=1)

        # Fill NAs appropriately
        df_auction['ask_q_cumulative'].ffill(inplace=True)
        df_auction['bid_q_cumulative'].bfill(inplace=True)

        # Quantity exchanged i.e. minimum of the two columns (excluding NaN), add a column with that information
        quantity_exchanged = np.minimum(df_auction['ask_q_cumulative'].values, df_auction['bid_q_cumulative'].values)
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

        self.price = auction_price
        return 


    def _execute_uncrossing_trades(self, orderbook) -> None:
        """
        Make the auction trades given an auction price. 
        """
        order_bid = orderbook.best_bid.orders.head
        order_ask = orderbook.best_ask.orders.head

        while order_bid.o_price >= self.price and order_ask.o_price <= self.price:
            trade_quantity = min(order_bid.o_q_rem, order_ask.o_q_rem)
            orderbook._fill_order(order_bid.o_id_fd, trade_quantity)
            orderbook._fill_order(order_ask.o_id_fd, trade_quantity)

            # Register the trade.
            trade = Trade(
                bid_id=order_bid.o_id_fd,
                ask_id=order_ask.o_id_fd,
                quantity=trade_quantity,
                price=self.price,
                bid_member=order_bid.o_member,
                ask_member=order_ask.o_member,
                dtm_neg=self.datetime)
            orderbook.trades.append(trade)

            # Update limit level lists and get new orders in line.
            if order_bid.o_q_rem == 0:
                # Order bid is filled completely.
                orderbook._remove(order_bid.o_id_fd)

                if len(orderbook.bids) > 0:
                    orderbook.best_bid = orderbook.bids[max(orderbook.bids.keys())]
                    current_bid = orderbook.best_bid
                    order_bid = current_bid.orders.head
                else:
                    orderbook.best_bid = None

            if order_ask.o_q_rem == 0:
                # Order ask is filled completely.
                orderbook._remove(order_ask.o_id_fd)

                if len(orderbook.asks) > 0:
                    orderbook.best_ask = orderbook.asks[min(orderbook.asks.keys())]
                    current_ask = orderbook.best_ask
                    order_ask = current_ask.orders.head
                else:
                    orderbook.best_ask = None

            if orderbook.best_bid == None or orderbook.best_ask == None:
                # One (both) side(s) of the book is (are) empty.
                break

    def _execute_auction_trades(self, orderbook) -> None:
        """
        Check for trades since the last message. 
        Updates the orders and limit levels involved if any trades.
        """
        #while self.trades[-1]['t_dtm_neg'] < self.current_message_datetime:
        #while isinstance(orderbook.trades[-1]['t_agg'], float):
        while orderbook.trades[-1]['t_agg'] not in ['A', 'V']:
            #### Normally dtype str; if nan, dtype = float
            trade = orderbook.trades.pop()
            orderbook._fill_order(trade['t_id_b_fd'], trade['t_q_exchanged'])
            orderbook._fill_order(trade['t_id_s_fd'], trade['t_q_exchanged'])
            orderbook.last_trading_price = trade['t_price']