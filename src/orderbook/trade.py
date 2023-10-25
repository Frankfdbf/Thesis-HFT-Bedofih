# Import Built-Ins

# Import Third-Party

# Import Homebrew


class Trade:
    """
    Trade class. Uses same nomenclature as trade files from EUROFIDAI. Used to
    check if orders are matched correctly during continuous period.
    """
    __slots__ = ['t_capital', 't_price', 't_id_b_fd', 't_id_s_fd', 
                 't_q_exchanged', 't_tr_nb', 't_b_type', 't_s_type', 
                 't_dtm_neg']
    
    class_counter = 1

    def __init__(self, bid_id, ask_id, quantity, price, bid_member, ask_member, dtm_neg):
        self.t_capital = price * quantity
        self.t_price = price 
        self.t_id_b_fd = int(bid_id)
        self.t_id_s_fd = int(ask_id)
        self.t_q_exchanged = quantity
        self.t_tr_nb = Trade.class_counter
        self.t_b_type = bid_member
        self.t_s_type = ask_member
        self.t_dtm_neg = dtm_neg
         
        Trade.class_counter += 1

    def __repr__(self):
        trade_msg = f"Trade: {self.t_q_exchanged} @ {self.t_price} between {self.t_id_b_fd} and {self.t_id_s_fd}"
        return trade_msg