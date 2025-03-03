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

    start = dt(2024, 12, 1)
    # end = dt(2026, 1, 31)
    end = dt(2025, 2, 1)

    moon_path = env('user_profile').joinpath('Fair Currents/major-moon-phases.csv')
    if moon_path.exists():
        moon_frame = read_df(moon_path)
        moon_frame.date = pd.to_datetime(moon_frame.date)
    else:
        moon_frame = pd.DataFrame(columns=['date', 'phase'])
        for y in range(3):
            moon_request_head = 'https://aa.usno.navy.mil/api/moon/phases/year?year=' + str(start.date().year + y)
            print(str(start.date().year + y))
            try:
                response = requests.get(moon_request_head)
                response.raise_for_status()
                response_dict = json.loads(response.text)
                for d in response_dict['phasedata']:
                    moon_frame.loc[len(moon_frame)] = {'date': dt(d['year'], d['month'], d['day']), 'phase':d['phase']}
            except requests.exceptions.RequestException as e:
                print(e)
        moon_frame = moon_frame[(moon_frame['date'] >= start - td(days=1)) & (moon_frame['date'] <= end + td(days=1))]
        print_file_exists(write_df(moon_frame, moon_path))

    sun_path = env('user_profile').joinpath('Fair Currents/sunrise-sunset.csv')
    if sun_path.exists():
        sun_frame = read_df(sun_path)
        sun_frame.date = pd.to_datetime(sun_frame.date)
    else:
        sun_frame = pd.DataFrame(columns=['date', 'sr', 'ss', 'st', 'mr', 'ms', 'mt', 'mp', 'fc'])
        sun_request_head = 'https://aa.usno.navy.mil/api/rstt/oneday?ID=FrCrnts&date='
        sun_request_tail = 'coords=40.78,-74.01&tz=-5&dst=true'
        date = start
        while date <= end:
            print(f'date: {date.date()}')
            request_string = sun_request_head + date.strftime('%Y-%m-%d&') + sun_request_tail
            try:
                response = requests.get(request_string)
                response.raise_for_status()
                response_dict = json.loads(response.text)
                sd = {d['phen']: pd.to_datetime(d['time'].split(' ')[0] + ':00', format='mixed') for d in response_dict['properties']['data']['sundata']}
                md = {'Rise': None, 'Set': None, 'Upper Transit': None}
                md =  md | {d['phen']: pd.to_datetime(d['time'].split(' ')[0]  + ':00', format='mixed') for d in response_dict['properties']['data']['moondata']}
                frame_dict = {'date': date,
                              'sr': time_to_degrees(sd['Rise']),
                              'ss': time_to_degrees(sd['Set']),
                              'st': time_to_degrees(sd['Upper Transit']),
                              'mr': time_to_degrees(md['Rise']),
                              'ms': time_to_degrees(md['Set']),
                              'mt': time_to_degrees(md['Upper Transit']),
                              'mp': response_dict['properties']['data']['curphase'],
                              'fc': response_dict['properties']['data']['fracillum'][:-1]
                              }
                sun_frame.loc[len(sun_frame)] = frame_dict
            except requests.exceptions.RequestException as e:
                print(e)
            date = date + td(days=1)

        for idx, row in moon_frame.iterrows():
            if idx == 0:
                sun_frame.loc[sun_frame.date == row.date, 'mp'] = row.phase
                sun_frame.loc[sun_frame.date == row.date + td(days=1), 'mp'] = row.phase
            elif idx == len(sun_frame) - 1:
                sun_frame.loc[sun_frame.date == row.date - td(days=1), 'mp'] = row.phase
                sun_frame.loc[sun_frame.date == row.date, 'mp'] = row.phase
            else:
                sun_frame.loc[sun_frame.date == row.date - td(days=1), 'mp'] = row.phase
                sun_frame.loc[sun_frame.date == row.date, 'mp'] = row.phase
                sun_frame.loc[sun_frame.date == row.date + td(days=1), 'mp'] = row.phase

        print_file_exists(write_df(sun_frame, sun_path))

    for csv_file in tt_files:
        print(f'Adding sun and moon data to {csv_file}')
        tt_frame = read_df(csv_file)
        for row in range(len(sun_frame)):
            target_date = f'{sun_frame.loc[row]['date'].month}/{sun_frame.loc[row]['date'].day}/{sun_frame.loc[row]['date'].year}'  # strftime hack
            for c in sun_frame.columns.to_list()[1:]:
                tt_frame.loc[tt_frame.date == target_date, c] = sun_frame.loc[row][c]
        write_df(tt_frame, csv_file)
