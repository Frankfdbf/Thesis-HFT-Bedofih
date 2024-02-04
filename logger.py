# Import
import logging

# Set logger
# Logging setup
logger = logging
logger.basicConfig(filename='orderbook.log', filemode='w', format='%(levelname)s - %(message)s', level=logging.DEBUG)