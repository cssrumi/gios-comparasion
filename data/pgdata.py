from datetime import datetime
from typing import Optional, List, Iterable, Generator

import postgresql.driver as pg_driver
import pytz
from postgresql.driver.dbapi20 import Connection
from postgresql.driver.pq3 import Statement
from postgresql.types import Row

from config import get_config
from data.data import Data

_config = get_config()


class InstallationQuery:
    def __init__(self):
        self._db: Connection = None
        self._installations_query: Statement = None

    def __enter__(self):
        self._db = pg_driver.connect(
            user=_config.pgUser,
            password=_config.pgPassword,
            host=_config.pgHost,
            port=_config.pgPort,
            database=_config.pgDB
        )
        self._installations_query = self._db.prepare("SELECT * FROM GIOS.ALL_COMPARABLE_INSTALLATIONS "
                                                     "WHERE STATION = $1 AND TIMESTAMP = $2")
        self._installations_batch = self._db.prepare("SELECT * FROM GIOS.ALL_COMPARABLE_INSTALLATIONS "
                                                     "WHERE STATION = $1 AND TIMESTAMP = ANY($2)")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._installations_query.close()
        self._installations_batch.close()
        self._db.close()

    def find_first(self, station: str, timestamp: datetime) -> Optional[Data]:
        if not self._installations_query:
            raise RuntimeError("This function is accessible only from context manager")
        result = self._installations_query.first(station, timestamp.astimezone(pytz.UTC))
        return InstallationQuery.deserialize(result)

    def find_in_batch(self, station: str, timestamps: Iterable[datetime], chunk_size=500) -> List[Data]:
        if self._db.closed:
            raise RuntimeError("This function is accessible only from context manager")
        result = []
        for chunk in InstallationQuery._chunks(timestamps, chunk_size):
            rows = self._installations_batch.rows(station, set(chunk))
            result.extend((InstallationQuery.deserialize(row) for row in rows if row))
        return result

    @staticmethod
    def deserialize(result: Row) -> Optional[Data]:
        if not result:
            return
        station = result['station']
        timestamp: datetime = result['timestamp'].astimezone(pytz.UTC)
        value = float(result['value'])
        return Data(station=station, timestamp=timestamp, value=value)

    @staticmethod
    def _chunks(sliceable, chunk_size: int) -> Generator:
        chunk_size = max(1, chunk_size)
        return (sliceable[i:i + chunk_size] for i in range(0, len(sliceable), chunk_size))
