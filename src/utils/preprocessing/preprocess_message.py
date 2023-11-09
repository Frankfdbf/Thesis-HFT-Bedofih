# Import Built-Ins

# Import Third-Party

# Import Homebrew
from src.orderbook.limit_level import LimitLevel

def preprocess_message(message:dict, is_before_auction: bool, 
                       best_bid: LimitLevel, best_ask: LimitLevel) -> dict:
    """
    Make a few changes to order message before sending it to the orderbook class.
    Correct the o_q_dis information. 
    Modifies price for market, stop market and market to limit orders (init 0).
    For market and market to limit orders during auction, price are 0 or 100k.
    For market limit orders during continuous trading, price equals best bid
    (ask) for sell (ask) orders.
    Do other preprocessing of the message.
    """

    # Set o_q_dis correctly (default: 0 if not iceberg order)
    if message['o_q_dis'] == 0: 
        message['o_q_dis'] = message['o_q_ini'] 

    # Alias for o_cha_id
    message['o_id_cha'] = message['o_cha_id']

    # Stop orders that are flagged as limit or market back to stop orders
    if message['o_price_stop'] != 0:
        if message['o_type'] == '1':
            message['o_type'] = '3' # stop market
        elif message['o_type'] == '2':
            message['o_type'] = '4' # stop limit

    # Modify o_price accordingly
    if is_before_auction: 
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
                message['o_price'] = best_ask.price
            elif message['o_bs'] == 'S':
                message['o_price'] = best_bid.price

    return message