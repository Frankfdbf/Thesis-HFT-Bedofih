# Import Built-Ins

# Import Third-Party

# Import Homebrew


def preprocess_message(message):
    """
    Make a few changes to order message before sending it to the orderbook class.
    Correct the o_q_dis information. 
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

    return message 