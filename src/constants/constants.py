# Import Built-Ins
import os
import datetime

# Import Third-Party
import pandas as pd

# Import Homebrew
from .stocks.stocks import Stocks
from .dates.dates import Dates2017


# Paths to folders
PATHS = {}
#PATHS['root'] = '/Volumes/Extreme ssd'
PATHS['root'] = '/Users/australien/Documents/IESEG/Master 2/Data'
PATHS['raw'] = os.path.join(PATHS['root'], 'raw')
PATHS['events'] = os.path.join(PATHS['root'], 'events')
PATHS['trades'] = os.path.join(PATHS['root'], 'trades')
PATHS['orders'] = os.path.join(PATHS['root'], 'orders')
PATHS['histories'] = os.path.join(PATHS['root'], 'histories')
PATHS['removed_orders'] = os.path.join(PATHS['root'], 'removed_orders')
PATHS['limit_order_books'] = os.path.join(PATHS['root'], 'limit_order_books')
PATHS['volume_by_interval'] = os.path.join(PATHS['root'], 'volume_by_interval')

# Stocks/list of isins
STOCKS = Stocks()

# Calendar, date and time constants
MONTHS_STR = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
DATES = Dates2017()

MARKET_OPEN  = datetime.time(hour=9, minute=0, second=0)

MARKET_CLOSE = datetime.time(hour=17, minute=30, second=0)
CLOSING_AUCTION_CUTOFF = datetime.time(hour=17, minute=35, second=0)

# Other
O_TYPES = {
    '1': 'Market order',
    '2': 'Limit order',
    '3': 'Stop market order',
    '4': 'Stop limit order',
    'P': 'Pegged order',
    'K': 'Market to limit order'
}