# Import Built-Ins
import datetime

# Import Third-Party
import pandas as pd

# Import Homebrew


class Node():
    """
    Node containing removed order information.
    """

    def __init__(self, data):
        self.data = data
        self.next = None
    
    def __repr__(self):
        return self.data

class LinkedList():
    """
    Linked list to store each removed order. Each node contains an order.
    Nodes are organised chronologically (by removal time). Only removals during
    the date analysed are taken into account.
    """

    def __init__(self):
        self.head = None
        self.count = 0
    
    def __iter__(self):
        node = self.head
        while node:
            yield node
            node = node.next
    
    def __len__(self):
        return self.count

    def populate(self, exited_order: pd.DataFrame, date: datetime.date) -> None:
        """
        Populate the linked list with nodes.
        Each node corresponds to a row/removed order.
        """

        exited_orders_sorted = exited_order.sort_values(by='o_dtm_br', ascending=False)
        for row in exited_orders_sorted.to_dict('records'):
            if row['o_dtm_br'].date() != date:
                continue
            else:
                node = Node(row)
                node.next = self.head
                self.head = node
                self.count += 1