# Import Built-Ins

# Import Third-Party
from pathlib import Path

# Import Homebrew
from .time_utils import timeit

def check_empty_csv(df, path):
    """
    Checks is the file is empty.
    If it is, returns True and a message with file info.
    """
    if len(df) == 0:
        p = Path(path)
        print(f'File: \'{p.stem}\' is empty (path: \'{p.parent}\').')
        return True
    else:
        False