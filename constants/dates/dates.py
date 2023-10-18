# Import Built-Ins
import os

# Import Third-Party
import pandas as pd

# Import Homebrew


class Dates2017:
    """
    Class object to store the dates that will be used for analysis. Categories
    include: 
    - month, 
    - day of the week.
    """
    
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dates_2017.parquet')
        self.data = pd.read_parquet(self.path)

        # All dates
        self.all = list(self.data.dates.dt.date)
        
        # Months
        self.january = list(self.data[self.data.dates.dt.month == 1].dates.dt.date)
        self.february = list(self.data[self.data.dates.dt.month == 2].dates.dt.date)
        self.march = list(self.data[self.data.dates.dt.month == 3].dates.dt.date)
        self.april = list(self.data[self.data.dates.dt.month == 4].dates.dt.date)
        self.may = list(self.data[self.data.dates.dt.month == 5].dates.dt.date)
        self.june = list(self.data[self.data.dates.dt.month == 6].dates.dt.date)
        self.july = list(self.data[self.data.dates.dt.month == 7].dates.dt.date)
        self.august = list(self.data[self.data.dates.dt.month == 8].dates.dt.date)
        self.september = list(self.data[self.data.dates.dt.month == 9].dates.dt.date)
        self.october = list(self.data[self.data.dates.dt.month == 10].dates.dt.date)
        self.november = list(self.data[self.data.dates.dt.month == 11].dates.dt.date)
        self.december = list(self.data[self.data.dates.dt.month == 12].dates.dt.date)
        
        # Weekdays
        self.mondays = list(self.data[self.data.dates.dt.weekday == 0].dates.dt.date)
        self.tuesdays = list(self.data[self.data.dates.dt.month == 1].dates.dt.date)
        self.wednesdays = list(self.data[self.data.dates.dt.month == 2].dates.dt.date)
        self.thursdays = list(self.data[self.data.dates.dt.month == 3].dates.dt.date)
        self.fridays = list(self.data[self.data.dates.dt.month == 4].dates.dt.date)