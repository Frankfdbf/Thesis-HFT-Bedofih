
import numpy as np
from utils.time_utils import timeit

path = '/Volumes/Extreme ssd/raw/01/20170102/DE000HV0JKP4-FR0000572349/FR0000188799/VHOX_FR0000188799_20170102.csv'
from preprocessing import preprocess_orders
from preprocessing.preprocess_orders import preprocess_orders_improved

@timeit
def save_file(df, output):
    df.to_csv(output)


for _ in range(10):
    df = preprocess_orders(path)
output = '/Users/australien/Desktop/orders_normal.csv'
save_file(df, output)


for _ in range(10):
    df = preprocess_orders_improved(path)
output = '/Users/australien/Desktop/orders_improved.csv'
save_file(df, output)


