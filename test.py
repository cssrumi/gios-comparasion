from datetime import datetime

import pytz

dt1 = pytz.timezone("Poland")\
    .localize(datetime(year=2020, month=3, day=10, hour=2)).astimezone(pytz.UTC)
print(dt1)

dt2 = pytz.timezone("Poland")\
    .localize(datetime(year=2020, month=4, day=10, hour=2)).astimezone(pytz.UTC)
print(dt2)
