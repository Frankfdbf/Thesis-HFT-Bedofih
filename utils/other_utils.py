# Import Built-Ins

# Import Third-Party
import pandas as pd
from pathlib import Path

# Import Homebrew

def check_empty_csv(path):
    """
    Checks is the file is empty.
    If it is, returns True and a message with file info.
    """
    df = pd.read_csv(path)
    if df.empty:
        p = Path(path)
        print(f'File: \'{p.stem}\' is empty (path: \'{p.parent}\').')
        return True
    else:
        False