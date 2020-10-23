from datetime import datetime

from comparator import Comparator
from timer import timer


@timer
def main():
    station = 'Działoszyn - PM10'
    from_datetime = datetime(year=2020, month=3, day=1, hour=0, minute=0)
    to_datetime = datetime(year=2020, month=6, day=30, hour=23, minute=0)
    comparator = Comparator(station, from_datetime, to_datetime)
    comparator.compare().plot()


if __name__ == '__main__':
    main()
