from datetime import datetime as dt, timedelta as td
import requests
import json
import pandas as pd
from tt_file_tools.file_tools import write_df, print_file_exists
from tt_date_time_tools.date_time_tools import time_to_degrees
from tt_os_abstraction.os_abstraction import env

if __name__ == '__main__':

    frame_path = env('user_profile').joinpath('Fair Currents/sunrise-sunset.csv')
    frame = pd.DataFrame(columns=['date', 'fracillum', 'Begin Civil Twilight', 'Rise',  'Set', 'End Civil Twilight'])

    start = dt(2024, 12, 1)
    end = dt(2026, 1, 31)
    request_head = 'https://aa.usno.navy.mil/api/rstt/oneday?ID=FrCrnts&date='
    request_tail = 'coords=40.78,-74.01&tz=-5&dst=true'

    date = start
    while date <= end:
        print(f'date: {date.date()}')
        request_string = request_head + date.strftime('%Y-%m-%d&') + request_tail
        try:
            response = requests.get(request_string)
            response.raise_for_status()
            response_dict = json.loads(response.text)
            fracillum = response_dict['properties']['data']['fracillum']
            sd = response_dict['properties']['data']['sundata']
            frame_dict = {'date': date, 'fracillum': fracillum, sd[0]['phen']: sd[0]['time'].split(' ')[0] + ':00', sd[1]['phen']: sd[1]['time'].split(' ')[0] + ':00', sd[3]['phen']: sd[3]['time'].split(' ')[0] + ':00', sd[4]['phen']: sd[4]['time'].split(' ')[0] + ':00'}
            frame.loc[len(frame)] = frame_dict
        except requests.exceptions.RequestException as e:
            print(e)
        date = date + td(days=1)

    frame['BCT degrees'] = frame['Begin Civil Twilight'].apply(time_to_degrees)
    frame['RISE degrees'] = frame['Rise'].apply(time_to_degrees)
    frame['SET degrees'] = frame['Set'].apply(time_to_degrees)
    frame['ECT degrees'] = frame['End Civil Twilight'].apply(time_to_degrees)

    print_file_exists(write_df(frame, frame_path))
