# Import Built-Ins

# Import Third-Party

# Import Homebrew
from .order import OrderList, Order


class LimitLevel:
    """
    Limit level class. Allows for total quantity, and detail about quantity 
    provided by HFTs as well as iceberg orders.
    """
    __slots__ = ['price', 'size', 
                 'disclosed_size_hft', 'disclosed_size_mixed', 'disclosed_size_non',
                 'hidden_size_hft', 'hidden_size_mixed', 'hidden_size_non',
                 'orders']

    def __init__(self, order: Order) -> None:
        # Data Values
        self.price = order.o_price
        self.size = order.o_q_rem

        self.disclosed_size_hft = order.o_q_dis if order.o_member == 'HFT' else 0
        self.disclosed_size_mixed = order.o_q_dis if order.o_member == 'MIX' else 0
        self.disclosed_size_non = order.o_q_dis if order.o_member == 'NON' else 0
        self.hidden_size_hft = order.o_q_rem - order.o_q_dis if order.o_member == 'HFT' else 0
        self.hidden_size_mixed =  order.o_q_rem - order.o_q_dis if order.o_member == 'MIX' else 0
        self.hidden_size_non =  order.o_q_rem - order.o_q_dis if order.o_member == 'NON' else 0

        # Doubly-Linked-list
        self.orders = OrderList(self)
        self.append(order)


    def __repr__(self):
        return str((self.price, self.size))
    

    def __len__(self):
        return len(self.orders)


    def append(self, order: Order) -> None:
        """
        Wrapper function to make appending an order easier.
        """
        return self.orders.append(order)