from datetime import datetime
from typing import Optional

import postgresql.driver as pg_driver
import pytz
from postgresql.driver.dbapi20 import Connection
from postgresql.driver.pq3 import Statement
from postgresql.types import Row

from config import get_config
from data import Data

_config = get_config()


class InstallationQuery:
    def __init__(self) -> 'InstallationQuery':
        self._db: Connection = None
        self._comparable_installations_query: Statement = None

    def __enter__(self):
        self._db = pg_driver.connect(
            user=_config.pgUser,
            password=_config.pgPassword,
            host=_config.pgHost,
            port=_config.pgPort,
            database=_config.pgDB
        )
        self._comparable_installations_query = self._db.prepare("SELECT * FROM GIOS.ALL_COMPARABLE_INSTALLATIONS "
                                                                "WHERE STATION = $1 AND TIMESTAMP = $2")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._comparable_installations_query.close()
        self._db.close()

    def find_first(self, station: str, timestamp: datetime) -> Optional[Data]:
        if not self._comparable_installations_query:
            raise RuntimeError("This function is accessible only from context manager")
        result = self._comparable_installations_query.first(station, timestamp)
        return InstallationQuery.deserialize(result)

    @staticmethod
    def deserialize(result: Row) -> Optional[Data]:
        if result:
            station = result['station']
            timestamp: datetime = result['timestamp']
            timestamp = timestamp.astimezone(pytz.timezone('POLAND'))
            value = float(result['value'])
            return Data(station=station, timestamp=timestamp, value=value)
