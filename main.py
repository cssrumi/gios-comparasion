from datetime import datetime

from comparator import Comparator


def main():
    station = 'Działoszyn - PM10'
    from_datetime = datetime(year=2020, month=3, day=20, hour=0, minute=0)
    to_datetime = datetime(year=2020, month=4, day=30, hour=23, minute=0)
    comparator = Comparator(station, from_datetime, to_datetime)
    data = comparator\
        .compare()\
        .plot()\
        .data()
    [print(d.timestamp, d.mongo_value, d.pg_value) for d in data]


if __name__ == '__main__':
    main()
