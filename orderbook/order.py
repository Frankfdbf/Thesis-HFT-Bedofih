# Import Built-Ins
import os
import datetime as dt 

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew
from constants.constants import O_TYPES


class Order2:
    """
    A class to represent an order object.

    Attributes
    ==========
    o_id_cha: 
        characteristic id 
    o_id_fd:
        fundamental id
    o_dt_be: 
        datetime book entrance
    o_dtm_be:
        microsecond book entrance
    o_dt_br: 
        datetime book release 
    o_dtm_br:
        microsecond book release 
    o_dt_va: 
        datetime order validity
    o_dtm_va:
        microsecond order validity
    o_dt_mo:
        datetime order modification
    o_dtm_mo:
        microsecond order modification
    o_dt_en:
        datetime order entry 
    o_bs:
        buy order or sell order
    o_type:
        type of the order
    o_execution:
        execution instruction
    o_validity:
        order validity
    o_dt_expiration:
        datetime expiration
    o_state:
        stae of the order
    o_price:
        order price
    o_price_stop:
        stop price
    o_q_ini:
        initial quantity
    o_q_min:
        minimum quantity
    o_q_dis:
        quantity disclosed
    o_q_neg:
        quantity negotiated
    o_app:
        application indicator
    o_origin:
        origin of the order
    o_account:
        indicator of the client account
    o_nb_tr: 
        number of transaction of the order
    o_q_rem:
        quantity remaining
    o_dt_upd: 
        update date
    o_member:
        type of member 
    """

    class_counter = 1

    def __init__(self, o_id_fd, o_dt_be, o_dtm_be,
                o_dt_va, o_dtm_va, o_bs, o_type, o_execution, o_validity, o_dt_expiration,
                o_price, o_price_stop, o_q_ini, o_q_min, o_q_dis, o_app, o_origin,
                o_account, o_member, update_id=True):
        
        self.orderID = Order.class_counter
        if update_id is True:
            Order.class_counter += 1
         
        # known at order initiation 
        ## identifiers 
        self.o_id_cha        = 1  
        self.o_id_fd         = o_id_fd   

        # member identification 
        self.o_member        = o_member
        self.o_account       = o_account 

        self.o_bs            = o_bs

        # price characteristics 
        self.o_price         = o_price 
        self.o_price_stop    = o_price_stop
        
        # quantity characteristics 
        self.o_q_ini         = o_q_ini
        self.o_q_rem         = o_q_ini  # when created, remaining shares is the same, then it decreases up to 0
                                        # do not trust o_q_rem because it is updated 
        self.o_q_neg         = 0        # when created, no quantity negotiated 
        self.o_q_min         = o_q_min 
        self.o_q_dis         = o_q_dis 
        
        self.o_type          = o_type
        self.o_execution     = o_execution
        
        ## datetime characteristics 
        self.o_dt_be         = o_dt_be      
        self.o_dtm_be        = o_dtm_be  
        self.o_validity      = o_validity 
        self.o_dt_expiration = o_dt_expiration         
        self.o_dt_va         = o_dt_va    
        self.o_dtm_va        = o_dtm_va  

        # not sure what to do with these info
        self.o_app = o_app 
        self.o_origin = o_origin 
        
        # not known at order initiation (but potentially updatable)
        self.o_dt_mo     = None 
        self.o_dtm_mo    = None
        self.o_dt_br     = None    
        self.o_dtm_br    = None  
        self.o_execution = None 
        self.o_state     = None 
    
    def __repr__(self):
        str_repr = f"\nOrder n: {self.orderID} - {self.o_id_fd}\n"
        str_repr+= f"{O_TYPES[self.o_type]}\n"
        str_repr+= f"Dir: {self.o_bs} - {self.o_q_ini}q @ {self.o_price}"
        str_repr+= f"\nby {self.o_member} \nat {self.o_dt_be}:{self.o_dtm_be}" 
        if self.o_price_stop != 0:
            str_repr += f"\nStop price: {self.o_price_stop}"
        return str_repr

    def __lt__(self, other):
        
        if self.o_type != '3': # can be different for stop orders  

            # orders are sorted according to price-time order priority 
            if self.o_bs == 'B':
                if self.o_price != other.o_price:
                    return self.o_price > other.o_price 
                elif self.o_dt_be != other.o_dt_be:
                    return self.o_dt_be < other.o_dt_be 
                else:
                    return self.o_dtm_be < other.o_dtm_be 
        
            if self.o_bs == 'S':
                if self.o_price != other.o_price:
                    return self.o_price < other.o_price
                elif self.o_dt_be != other.o_dt_be:
                    return self.o_dt_be < other.o_dt_be 
                else:
                    return self.o_dtm_be < other.o_dtm_be 
        
        elif self.o_type == '3':
            if self.o_bs == 'B':
                # buy stop order with top priority is the one with the 
                # lowest o_price_stop 
                # time priority remains the same 
                if self.o_price_stop != other.o_price_stop:
                    return self.o_price_stop < other.o_price_stop
                elif self.o_dt_be != other.o_dt_be:
                    return self.o_dt_be < other.o_dt_be
                else:
                    return self.o_dtm_be < other.o_dtm_be
            
            elif self.o_bs == 'S':
                # sell stop order with top priority is the one with 
                # highest o_price_stop 
                # time priority remains the same
                if self.o_price_stop != other.o_price_stop:
                    return self.o_price_stop > other.o_price_stop 
                elif self.o_dt_be != other.o_dt_be:
                    return self.o_dt_be < other.o_dt_be 
                else:
                    return self.o_dtm_be < other.o_dtm_be

    
    def get_time_book_entrance(self) -> tuple:
        '''
        Returns datetime and microsecond of book entrance. 
        '''
        dt = self.o_dt_be 
        dtm = self.o_dtm_be 
        return (dt, dtm)
    
    def get_o_id_cha(self):
        return self.o_id_cha 
    
    def get_o_id_fd(self):
        return self.o_id_fd 
    
    def get_o_bs(self):
        return self.o_bs 
    
    def get_o_type(self):
        return self.o_type
    
    def get_o_state(self):
        return self.o_state 
    
    def get_o_q_rem(self):
        return self.o_q_rem
    def set_o_q_rem(self, qty):
        self.o_q_rem = qty

    def get_o_q_ini(self):
        return self.o_q_ini 
    def set_o_q_ini(self, qty):
        self.o_q_ini = qty
        
    def get_o_q_neg(self):
        return self.o_q_neg
    def set_o_q_neg(self, qty):
        self.o_q_neg = qty 
        
    def get_o_price(self):
        return self.o_price
    def set_o_price(self, price):
        self.o_price = price 

    def get_o_price_stop(self):
        return self.o_price_stop
    def set_o_price_stop(self, value):
        self.o_price_stop = value

# todo 
# https://stackoverflow.com/questions/2627002/whats-the-pythonic-way-to-use-getters-and-setters



# not known at initiation 
# o_dt_mo, o_dtm_mo
# o_dt_br, o_dtm_br
# o_state 
# o_q_neg : will be incremented later 
# o_q_rem : gets updated 
# o_dt_upd : will be filled later 
# o_nb_tr

# not useful ? 
# o_d_i 
# o_dt_en, o_sq_nb, o_sq_nbm, o_dt_p, o_dtm_p, o_currency,
# o_price_dfpg : always zero 
# o_disoff : always zero 

"""
    A class to represent an order

    Attributes
    ==========
    o_isin: str
        isin code
    o_d_i: 
        date of integration
    o_id_cha: 
        characteristic id 
    o_id_fd:
        fundamental id
    o_dt_be: 
        datetime book entrance
    o_dtm_be:
        microsecond book entrance
    o_dt_br:
        datetime book release 
    o_dtm_br:
        microsecond book release 
    o_dt_va: 
        datetime order validity
    o_dtm_va:
        microsecond order validity
    o_dt_mo:
        datetime order modification
    o_dtm_mo:
        microsecond order modification
    o_dt_en:
        datetime order entry 
    o_sq_nb: 
        sequence number
    o_sq_nbm:
        modified sequence number
    o_dt_p:
        datetime priority
    o_dtm_p:
        microsecond order priority
    o_currency:
        order currency
    o_bs:
        buy order or sell order
    o_type:
        type of the order
    o_execution:
        execution instruction
    o_validity:
        order validity
    o_dt_expiration:
        datetime expiration
    o_state:
        stae of the order
    o_price:
        order price
    o_price_stop:
        stop price
    o_price_dfpg:
        difference price for pegged orders
    o_disoff:
        discretionary offset
    o_q_ini:
        initial quantity
    o_q_min:
        minimum quantity
    o_q_dis:
        quantity disclosed
    o_q_neg:
        quantity negotiated
    o_app:
        application indicator
    o_origin:
        origin of the order
    o_account:
        indicator of the client account
    o_nb_tr: 
        number of transaction of the order
    o_q_rem:
        quantity remaining
    o_dt_upd: 
        update date
    o_member:
        type of member 
    
    """


class Order:
    """
    A class to represent an order object.

    Attributes
    ==========
    o_id_cha: 
        characteristic id 
    o_id_fd:
        fundamental id
    o_dt_be: 
        datetime book entrance
    o_dtm_be:
        microsecond book entrance
    o_dt_br: 
        datetime book release 
    o_dtm_br:
        microsecond book release 
    o_dt_va: 
        datetime order validity
    o_dtm_va:
        microsecond order validity
    o_dt_mo:
        datetime order modification
    o_dtm_mo:
        microsecond order modification
    o_dt_en:
        datetime order entry 
    o_bs:
        buy order or sell order
    o_type:
        type of the order
    o_execution:
        execution instruction
    o_validity:
        order validity
    o_dt_expiration:
        datetime expiration
    o_state:
        stae of the order
    o_price:
        order price
    o_price_stop:
        stop price
    o_q_ini:
        initial quantity
    o_q_min:
        minimum quantity
    o_q_dis:
        quantity disclosed
    o_q_neg:
        quantity negotiated
    o_app:
        application indicator
    o_origin:
        origin of the order
    o_account:
        indicator of the client account
    o_nb_tr: 
        number of transaction of the order
    o_q_rem:
        quantity remaining
    o_dt_upd: 
        update date
    o_member:
        type of member 
    """

    class_counter = 1

    def __init__(self, o_id_cha, o_id_fd, o_state, o_dtm_be, o_dtm_va, o_bs, 
                 o_type, o_execution, o_validity, o_dt_expiration, o_price, 
                 o_price_stop, o_q_ini, o_q_min, o_q_dis, o_q_neg, o_app, 
                 o_origin, o_account, o_q_rem, o_member, update_id=True):


        self.orderID = Order.class_counter
        if update_id is True:
            Order.class_counter += 1
         
        # known at order initiation 
        ## identifiers 
        self.o_id_cha        = 1  
        self.o_id_fd         = o_id_fd   

        # member identification 
        self.o_member        = o_member
        self.o_account       = o_account 

        self.o_bs            = o_bs

        # price characteristics 
        self.o_price         = o_price 
        self.o_price_stop    = o_price_stop
        
        # quantity characteristics 
        self.o_q_ini         = o_q_ini
        self.o_q_rem         = o_q_ini  # when created, remaining shares is the same, then it decreases up to 0
                                        # do not trust o_q_rem because it is updated 
        self.o_q_neg         = 0        # when created, no quantity negotiated 
        self.o_q_min         = o_q_min 
        self.o_q_dis         = o_q_dis 
        
        self.o_type          = o_type
        self.o_execution     = o_execution
        
        ## datetime characteristics 
        #self.o_dt_be         = o_dt_be      
        self.o_dtm_be        = o_dtm_be  
        self.o_validity      = o_validity 
        self.o_dt_expiration = o_dt_expiration         
        #self.o_dt_va         = o_dt_va    
        self.o_dtm_va        = o_dtm_va  

        # not sure what to do with these info
        self.o_app = o_app 
        self.o_origin = o_origin 
        
        # not known at order initiation (but potentially updatable)
        self.o_dt_mo     = None 
        self.o_dtm_mo    = None
        self.o_dt_br     = None    
        self.o_dtm_br    = None  
        self.o_execution = None 
        self.o_state     = None 
    
    def __repr__(self):
        str_repr = f"\nOrder n: {self.orderID} - {self.o_id_fd}\n"
        str_repr+= f"{O_TYPES[self.o_type]}\n"
        str_repr+= f"Dir: {self.o_bs} - {self.o_q_ini}q @ {self.o_price}"
        str_repr+= f"\nby {self.o_member} \nat {self.o_dtm_be}" 
        if self.o_price_stop != 0:
            str_repr += f"\nStop price: {self.o_price_stop}"
        return str_repr

    def __lt__(self, other) -> bool:
        """
        Orders are sorted according to price-time order priority
        """

        if self.o_type == '3': # Stop market orders
            # Price priority: buy (sell) stop order with top priority is the 
            # one with the lowest (highest) o_price_stop
            if self.o_price_stop != other.o_price_stop:
                # Buy
                if self.o_bs == 'B':
                    return self.o_price_stop < other.o_price_stop
                # Sell
                elif self.o_bs == 'S':
                    return self.o_price_stop > other.o_price_stop
            # Time priority
            else:
                return self.o_dtm_be < other.o_dtm_be
            
        else: # Other types of orders
            # Price priority
            if self.o_price != other.o_price:
                # Buy
                if self.o_bs == 'B':
                    return self.o_price > other.o_price
                # Sell
                elif self.o_bs == 'S':  
                    return self.o_price < other.o_price 
            # Time priority
            else:
                return self.o_dtm_be < other.o_dtm_be 
    
    def get_time_book_entrance(self) -> dt:
        '''
        Returns datetime and microsecond of book entrance. 
        '''
        #dt = self.o_dt_be 
        #dtm = self.o_dtm_be 
        #return (dt, dtm)
        return self.o_dtm_be
    
    def get_o_id_cha(self):
        return self.o_id_cha 
    
    def get_o_id_fd(self):
        return self.o_id_fd 
    
    def get_o_bs(self):
        return self.o_bs 
    
    def get_o_type(self):
        return self.o_type
    
    def get_o_state(self):
        return self.o_state 
    
    def get_o_q_rem(self):
        return self.o_q_rem
    def set_o_q_rem(self, qty):
        self.o_q_rem = qty

    def get_o_q_ini(self):
        return self.o_q_ini 
    def set_o_q_ini(self, qty):
        self.o_q_ini = qty
        
    def get_o_q_neg(self):
        return self.o_q_neg
    def set_o_q_neg(self, qty):
        self.o_q_neg = qty 
        
    def get_o_price(self):
        return self.o_price
    def set_o_price(self, price):
        self.o_price = price 

    def get_o_price_stop(self):
        return self.o_price_stop
    def set_o_price_stop(self, value):
        self.o_price_stop = value
