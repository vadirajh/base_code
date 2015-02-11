import time, json
from datetime import datetime as dt
def retjson():
   your_date = dt.now()
   data = json.dumps(time.mktime(your_date.timetuple())*1000)
   return data
