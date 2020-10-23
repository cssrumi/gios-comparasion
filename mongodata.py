from datetime import datetime, timedelta
from typing import List, Optional

import pymongo
import pytz
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from config import get_config
from data import Data

_config = get_config()


class InstallationQuery:
    def __init__(self):
        self._query_ts_format = '%Y-%m-%d %H:%M'
        self._client: MongoClient = None
        self._collection: Collection = None

    def __enter__(self):
        self._client = pymongo.MongoClient(InstallationQuery._create_connection_uri())
        self._collection = self._client[_config.mongoDB][_config.mongoCollection]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()
        self._collection = None
        self._client = None

    def find(self, station: str, from_datetime: datetime, to_datetime: datetime) -> List[Data]:
        if not self._client:
            raise RuntimeError("This function is accessible only from context manager")
        from_dtf = from_datetime.strftime(self._query_ts_format)
        to_dtf = to_datetime.strftime(self._query_ts_format)
        result: Cursor = self._collection.find({'Params': station,
                                                'Dates': {
                                                    '$gte': from_dtf,
                                                    '$lt': to_dtf
                                                }})
        return [data
                for doc in result
                for data in self.deserialize_doc(doc)]

    def find_first(self, station: str = None):
        if station:
            return self._collection.find_one({'Params': station})
        return self._collection.find_one()

    def deserialize_doc(self, doc: dict) -> List[Data]:
        station = doc['Params'][0]
        deserialized = []
        for i, date in enumerate(doc['Dates']):
            timestamp = pytz.UTC.localize(datetime.strptime(date, self._query_ts_format)) - timedelta(hours=2)
            try:
                value = doc['Data'][i]['value']
                deserialized.append(Data(station=station, timestamp=timestamp, value=value))
            except Exception:
                pass
        return deserialized

    @staticmethod
    def deserialize(result: dict) -> Optional[Data]:
        pass

    @staticmethod
    def _create_connection_uri() -> str:
        return 'mongodb://{}:{}@{}:{}/?authSource=admin&ssl=false'.format(
            _config.mongoUser,
            _config.mongoPassword,
            _config.mongoHost,
            _config.mongoPort
        )
