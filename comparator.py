from datetime import datetime
from typing import List

import matplotlib.pyplot as plt
from frozenlist import FrozenList

from data import DataComparison
from mongodata import InstallationQuery as MQuery
from pgdata import InstallationQuery as PGQuery


class Comparator:
    def __init__(self, station: str, from_dt: datetime, to_dt: datetime):
        self._station = station
        self._from_dt = from_dt
        self._to_dt = to_dt
        self._comparison: FrozenList[DataComparison] = None

    def compare(self) -> List[DataComparison]:
        with MQuery() as mq:
            m_data_list = mq.find(self._station, self._from_dt, self._to_dt)

        with PGQuery() as pgq:
            pg_data_list = [pgq.find_first(self._station, d.timestamp) for d in m_data_list]

        pg_data = {d.timestamp: d for d in pg_data_list if d}
        m_data = {d.timestamp: d for d in m_data_list if d.timestamp in pg_data.keys()}

        comp = (DataComparison(timestamp=key, pg_value=pg_data[key].value, mongo_value=m_data[key].value)
                for key in pg_data.keys())
        self._comparison = FrozenList(sorted(comp, key=lambda c: c.timestamp))
        return self._comparison

    def plot(self):
        plt.plot(
            range(len(self._comparison)),
            [c.mongo_value - c.pg_value
             for c in self._comparison]
        )
        plt.ylabel('delta')
        plt.xlabel('pomiar')
        plt.show()
