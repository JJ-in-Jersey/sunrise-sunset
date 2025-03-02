from datetime import datetime as dt, timedelta as td

import requests
import json
import pandas as pd
from pathlib import Path
from tt_file_tools.file_tools import write_df, print_file_exists, list_all_files, read_df
from tt_date_time_tools.date_time_tools import time_to_degrees
from tt_os_abstraction.os_abstraction import env

if __name__ == '__main__':

    # twilight and golden hour a so close to 30 minutes it's not worth the accuracy to download and calculate

    file_list = list_all_files(env('user_profile').joinpath('Fair Currents'))
    tt_files = [Path(file) for file in file_list if "transit times" in file]

    frame_path = env('user_profile').joinpath('Fair Currents/sunrise-sunset.csv')
    start = dt(2024, 12, 1)
    end = dt(2026, 1, 31)
    request_head = 'https://aa.usno.navy.mil/api/rstt/oneday?ID=FrCrnts&date='
    request_tail = 'coords=40.78,-74.01&tz=-5&dst=true'

    frame = pd.DataFrame(columns=['date', 'curphase',
                                  'sr', 'srd', 'ss', 'ssd', 'st', 'std',
                                  'mr', 'mrd', 'ms', 'msd', 'mt', 'mtd'])
    date = start
    while date <= end:
        print(f'date: {date.date()}')
        request_string = request_head + date.strftime('%Y-%m-%d&') + request_tail
        try:
            response = requests.get(request_string)
            response.raise_for_status()
            response_dict = json.loads(response.text)
            sd = {d['phen']: pd.to_datetime(d['time'].split(' ')[0] + ':00', format='mixed') for d in response_dict['properties']['data']['sundata']}
            md = {'Rise': None, 'Set': None, 'Upper Transit': None}
            md =  md | {d['phen']: pd.to_datetime(d['time'].split(' ')[0]  + ':00', format='mixed') for d in response_dict['properties']['data']['moondata']}
            frame_dict = {'date': date, 'curphase': response_dict['properties']['data']['curphase'],
                          'sr': sd['Rise'], 'srd': time_to_degrees(sd['Rise']),
                          'ss': sd['Set'], 'ssd': time_to_degrees(sd['Set']),
                          'st': sd['Upper Transit'], 'std': time_to_degrees(sd['Upper Transit']),
                          'mr': md['Rise'], 'mrd': time_to_degrees(md['Rise']),
                          'ms': md['Set'], 'msd': time_to_degrees(md['Set']),
                          'mt': md['Upper Transit'], 'mtd': time_to_degrees(md['Upper Transit'])
                          }
            frame.loc[len(frame)] = frame_dict
        except requests.exceptions.RequestException as e:
            print(e)
        date = date + td(days=1)

    print_file_exists(write_df(frame, frame_path))

    for csv_file in tt_files:
        print(f'Adding sun and moon data to {csv_file}')
        tt_frame = read_df(csv_file)
        for row in range(len(frame)):
            target_date = f'{frame.loc[row]['date'].month}/{frame.loc[row]['date'].day}/{frame.loc[row]['date'].year}'  # strftime hack
            tt_frame.loc[tt_frame.date == target_date, 'sr'] = frame.loc[row]['sr']
            tt_frame.loc[tt_frame.date == target_date, 'ss'] = frame.loc[row]['ss']
            tt_frame.loc[tt_frame.date == target_date, 'st'] = frame.loc[row]['st']
            tt_frame.loc[tt_frame.date == target_date, 'mr'] = frame.loc[row]['mr']
            tt_frame.loc[tt_frame.date == target_date, 'ms'] = frame.loc[row]['ms']
            tt_frame.loc[tt_frame.date == target_date, 'mt'] = frame.loc[row]['mt']
            tt_frame.loc[tt_frame.date == target_date, 'mp'] = frame.loc[row]['curphase']
        write_df(tt_frame, csv_file)
