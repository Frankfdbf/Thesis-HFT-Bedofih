class Trade:
    
    class_counter = 1

    def __init__(self, buy_orderID, sell_orderID, qty, price):
        self.buy_orderID = int(buy_orderID) 
        self.sell_orderID = int(sell_orderID)
        self.qty = qty 
        self.price = price 
        self.tradeID = Trade.class_counter 
        Trade.class_counter += 1

    def __repr__(self):
        trade_msg = f"Trade {self.tradeID}: {self.qty} @ {self.price}"
        trade_msg+= f"\nbetween {self.buy_orderID} and {self.sell_orderID}\n"
        return trade_msg