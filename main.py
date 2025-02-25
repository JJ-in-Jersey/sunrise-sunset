from datetime import datetime as dt, timedelta as td
import requests
import time
import json

if __name__ == '__main__':

    start = dt(2024, 12, 1)
    end = dt(2026, 1, 31)
    request_head = 'https://aa.usno.navy.mil/api/rstt/oneday?date='
    request_tail = 'coords=40.78,-74.01&tz=-5&dst=true'

    date = start
    while date <= end:
        request_string = request_head + date.strftime('%Y-%m-%d&') + request_tail
        print(request_string)
        for _ in range(5):
            try:
                response = requests.get(request_string)
                response.raise_for_status()
                response_dict = json.loads(response.text)
                pass
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(1)


        date = date + td(days=1)
