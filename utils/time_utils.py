# Import Built-Ins
from functools import wraps
import time

# Import Third-Party

# Import Homebrew


def timeit(func):
    """ Decorator to measure execution time of a function."""
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} took {total_time:.4f} seconds.')
        return result
    return timeit_wrapper