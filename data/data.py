from collections import namedtuple

Data = namedtuple('Data', ['station', 'timestamp', 'value'])

DataComparison = namedtuple('DataComparison', ['timestamp', 'pg_value', 'mongo_value'])
