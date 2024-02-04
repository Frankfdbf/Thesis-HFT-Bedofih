# Import Built-Ins
from __future__ import annotations
import datetime as dt 

# Import Third-Party

# Import Homebrew


class OrderList:
    """
    Doubly-Linked List Container Class.
    Stores head and tail orders, as well as count.
    Keeps a reference to its parent LimitLevel Instance.
    This container was added because it makes deleting the LimitLevels easier.
    """
    __slots__ = ['head', 'tail', 'count', 'parent_limit']

    def __init__(self, parent_limit) -> None:
        self.head = None
        self.tail = None
        self.count = 0
        self.parent_limit = parent_limit
        

    def __len__(self) -> int:
        return self.count

    def append(self, order: Order) -> None:
        """
        Appends an order to this List.
        Same as LimitLevel append, except it automatically updates head and tail
        if it's the first order in this list.
        """
        if not self.tail:
            order.root = self
            self.tail = order
            self.head = order
            self.count += 1
        else:
            self.tail.append(order)


    def __repr__(self):
        return str([order for order in self.__iter__()])
    

    def __iter__(self):
        current = self.head
        while current:
            yield current.o_id_fd
            current = current.next_item


class Order:
    """
    Doubly-Linked List Order item.
    Keeps a reference to root, as well as previous and next order in line.
    It also performs any and all updates to the root's tail, head and count
    references, as well as updating the related LimitLevel's size, whenever
    a method is called on this instance.
    """
    __slots__ = ['o_id_cha', 'o_id_fd', 'o_member', 'o_account', 'o_bs', 
                 'o_execution', 'o_validity', 'o_type', 'o_price', 'o_price_stop',
                 'o_q_ini', 'o_q_rem', 'o_q_neg', 'o_q_min', 'o_q_dis', 'o_dt_expiration', 'o_dtm_be',
                 'o_dtm_va', 'next_item', 'previous_item', 'root']

    def __init__(self, o_id_cha: int, o_id_fd: str,
                 o_member: str, o_account: int, o_bs: str, o_execution: str, o_validity: int, o_type: str,     
                 o_price, o_price_stop,
                 o_q_ini, o_q_min, o_q_dis,
                 o_dt_expiration: dt.datetime, o_dtm_be: dt.datetime, o_dtm_va: dt.datetime,
                 next_item=None, previous_item=None, root=None):
        
        # Data Values
        self.o_id_cha        = o_id_cha  # not used
        self.o_id_fd         = o_id_fd   

        self.o_member        = o_member
        self.o_account       = o_account
        self.o_bs            = o_bs
        self.o_execution     = o_execution # not used
        self.o_validity      = o_validity
        self.o_type          = o_type

        self.o_price         = o_price 
        self.o_price_stop    = o_price_stop

        self.o_q_ini         = o_q_ini
        self.o_q_rem         = o_q_ini  # when created, remaining shares is the same, then it decreases up to 0                             
        self.o_q_neg         = 0        # do not trust o_q_rem because it is updated. when created, no quantity negotiated 
        self.o_q_min         = o_q_min 
        self.o_q_dis         = o_q_dis 
        
        self.o_dt_expiration = o_dt_expiration # not needed, have book release
        self.o_dtm_be        = o_dtm_be  # maybe needed for time priority
        self.o_dtm_va        = o_dtm_va        

        # DLL Attributes
        self.next_item = next_item
        self.previous_item = previous_item
        self.root = root


    @property
    def parent_limit(self):
        return self.root.parent_limit


    #def append(self, order: Order):
    def append(self, order):
        """
        Append an order.
        """
        if self.next_item is None:
            self.next_item = order
            self.next_item.previous_item = self
            self.next_item.root = self.root

            # Update Root Statistics in OrderList root obj
            self.root.count += 1
            self.root.tail = order

            # Update quantities
            displayed_qty = min(order.o_q_rem, order.o_q_dis)

            self.parent_limit.size += order.o_q_rem
            self.parent_limit.disclosed_size_hft += displayed_qty if order.o_member == 'HFT' else 0
            self.parent_limit.disclosed_size_mixed += displayed_qty if order.o_member == 'MIX' else 0
            self.parent_limit.disclosed_size_non += displayed_qty if order.o_member == 'NON' else 0
            self.parent_limit.hidden_size_hft += order.o_q_rem - displayed_qty if order.o_member == 'HFT' else 0
            self.parent_limit.hidden_size_mixed += order.o_q_rem - displayed_qty if order.o_member == 'MIX' else 0
            self.parent_limit.hidden_size_non += order.o_q_rem - displayed_qty if order.o_member == 'NON' else 0

        else:
            ####
            print('Does this happen ? Yes it does.')
            self.next_item.append(order)

    def overwrite_quantity_negociated(self, q_neg) -> None:
        """
        Override to change quantity negociated, quantity remaining and update 
        the limit level accordingly. This is used in orderbook when an order partially filled 
        """

        self.o_q_neg = q_neg
        self.o_q_rem = self.o_q_ini - q_neg

        old_q_dis = self.o_q_dis
        self.o_q_dis = min(self.o_q_dis, self.o_q_rem)

        impact_q_dis = old_q_dis - self.o_q_dis
        impact_q_hid = q_neg - impact_q_dis

        # Update quantities
        self.parent_limit.size -= q_neg
        self.parent_limit.disclosed_size_hft -= (impact_q_dis if self.o_member == 'HFT' else 0)
        self.parent_limit.disclosed_size_mixed -= (impact_q_dis if self.o_member == 'MIX' else 0)
        self.parent_limit.disclosed_size_non -= (impact_q_dis if self.o_member == 'NON' else 0)
        self.parent_limit.hidden_size_hft -= (impact_q_hid if self.o_member == 'HFT' else 0)
        self.parent_limit.hidden_size_mixed -= (impact_q_hid if self.o_member == 'MIX' else 0)
        self.parent_limit.hidden_size_non -= (impact_q_hid if self.o_member == 'NON' else 0)


    def pop_from_list(self):
        """
        Pops this item from the DoublyLinkedList it belongs to.

        :return: Order() instance values as tuple
        """
        if self.previous_item is None:
            # We're head
            self.root.head = self.next_item

        if self.next_item is None:
            # We're tail
            self.root.tail = self.previous_item
        
        if self.next_item:
            self.next_item.previous_item = self.previous_item

        if self.previous_item:
            self.previous_item.next_item = self.next_item
        
        

        # Update the Limit Level and root
        self.root.count -= 1
        self.parent_limit.size -= self.o_q_rem
        #### This is clearly not definit
        
        self.parent_limit.disclosed_size_hft -= self.o_q_dis if self.o_member == 'HFT' else 0
        self.parent_limit.disclosed_size_mixed -= self.o_q_dis if self.o_member == 'MIX' else 0
        self.parent_limit.disclosed_size_non -= self.o_q_dis if self.o_member == 'NON' else 0
        self.parent_limit.hidden_size_hft -= self.o_q_rem - self.o_q_dis if self.o_member == 'HFT' else 0
        self.parent_limit.hidden_size_mixed -= self.o_q_rem - self.o_q_dis if self.o_member == 'MIX' else 0
        self.parent_limit.hidden_size_non -= self.o_q_rem - self.o_q_dis if self.o_member == 'NON' else 0
        
        return self.__repr__()
    

    def reset(self):
        """
        Resets DLL attributes (used for pegged orders).
        """
        self.next_item = None
        self.previous_item = None
        self.root = None

        return self


    def __str__(self):
        return self.__repr__()


    def __repr__(self):
        return str((self.o_id_fd, self.o_bs, self.o_price, self.o_q_rem, self.o_dtm_va))