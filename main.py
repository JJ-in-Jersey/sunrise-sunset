from datetime import datetime as dt, timedelta as td
import requests
import time
import json
import pandas as pd
from pathlib import Path
from tt_file_tools.file_tools import write_df, print_file_exists

if __name__ == '__main__':

    frame_path = Path('/Users/jason/Fair Currents/sunrise-sunset.csv')

    frame = pd.DataFrame(columns=['date', 'fracillum', 'Begin Civil Twilight', 'End Civil Twilight', 'Rise', 'Set'])

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
                fracillum = response_dict['properties']['data']['fracillum']
                sd = response_dict['properties']['data']['sundata']
                frame_dict = {'date': date, 'fracillum': fracillum, sd[0]['phen']: sd[0]['time'], sd[1]['phen']: sd[1]['time'], sd[3]['phen']: sd[3]['time'], sd[4]['phen']: sd[4]['time']}
                frame.loc[len(frame)] = frame_dict
            except requests.exceptions.RequestException as e:
                print(e)
                time.sleep(1)
        date = date + td(days=1)
    print_file_exists(write_df(frame, frame_path))
