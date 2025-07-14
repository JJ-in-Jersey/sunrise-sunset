from string import Template
import requests
import json
import pandas as pd
from pathlib import Path

from tt_dataframe.dataframe import DataFrame
from tt_file_tools.file_tools import print_file_exists, list_all_files
from tt_date_time_tools.date_time_tools import time_to_degrees
from tt_os_abstraction.os_abstraction import env
from datetime import datetime as dt, timedelta as td

if __name__ == '__main__':
    # twilight and golden hour a so close to 30 minutes it's not worth the accuracy to download and calculate

    fair_currents = list_all_files(env('user_profile').joinpath('Fair Currents'))
    tt_files = [Path(file) for file in fair_currents if "transit times" in file]

    start = dt(2024, 12, 1)
    end = dt(2026, 1, 31)

    moon_path = env('user_profile').joinpath('Fair Currents/major-moon-phases.csv')
    if moon_path.exists():
        moon_frame = DataFrame(csv_source=moon_path)
        moon_frame.date = pd.to_datetime(moon_frame.date)
    else:
        moon_frame = DataFrame(columns=['date', 'phase'])
        for y in range(3):
            moon_request_head = 'https://aa.usno.navy.mil/api/moon/phases/year?year=' + str(start.date().year + y)
            print(str(start.date().year + y))
            try:
                response = requests.get(moon_request_head)
                response.raise_for_status()
                response_dict = json.loads(response.text)
                for d in response_dict['phasedata']:
                    moon_frame.loc[len(moon_frame)] = {'date': dt(d['year'], d['month'], d['day']), 'phase': d['phase']}
            except requests.exceptions.RequestException as e:
                print(e)
        moon_frame = moon_frame[(moon_frame['date'] >= start) & (moon_frame['date'] <= end)]
        print_file_exists(moon_frame.write(moon_path))

    sun_temp = Template(f'{str(env('user_profile').joinpath('Fair Currents/sunrise-sunset'))}-$BASE.csv')

    sun_path = Path(sun_temp.substitute(BASE='original'))
    if sun_path.exists():
        sun_frame = DataFrame(csv_source=sun_path)
        sun_frame.date = pd.to_datetime(sun_frame.date)
    else:
        sun_frame = DataFrame(columns=['date', 'sunrise', 'sunset', 'sun_transit', 'moonrise', 'moonset', 'moon_transit', 'moon_phase'])
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
                md = md | {d['phen']: pd.to_datetime(d['time'].split(' ')[0] + ':00', format='mixed') for d in response_dict['properties']['data']['moondata']}
                frame_dict = {'date': date,
                              'sunrise': time_to_degrees(sd['Rise']),
                              'sunset': time_to_degrees(sd['Set']),
                              'sun_transit': time_to_degrees(sd['Upper Transit']),
                              'moonrise': None if md['Rise'] is None else time_to_degrees(md['Rise']),
                              'moonset': None if md['Set'] is None else time_to_degrees(md['Set']),
                              'moon_transit': None if md['Upper Transit'] is None else time_to_degrees(md['Upper Transit']),
                              'moon_phase': response_dict['properties']['data']['curphase']
                              }
                frame_dict = {col: frame_dict.get(col) for col in sun_frame.columns}
                frame_dict = {k: v for k, v in frame_dict.items() if pd.notna(v)}
                sun_frame.loc[len(sun_frame)] = frame_dict
            except requests.exceptions.RequestException as e:
                print(e)
            date = date + td(days=1)
        print_file_exists(sun_frame.write(sun_path))

    sun_path = Path(sun_temp.substitute(BASE='combined'))
    for idx, row in moon_frame.iterrows():
        if idx == 0:
            sun_frame.loc[sun_frame.date == row.date, 'moon_phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date + td(days=1), 'moon_phase'] = row.phase
        elif idx == len(sun_frame) - 1:
            sun_frame.loc[sun_frame.date == row.date - td(days=1), 'moon_phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date, 'moon_phase'] = row.phase
        else:
            sun_frame.loc[sun_frame.date == row.date - td(days=1), 'moon_phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date, 'moon_phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date + td(days=1), 'moon_phase'] = row.phase
    print_file_exists(sun_frame.write(sun_path))

    sun_path = Path(sun_temp.substitute(BASE='final'))
    for idx, row in sun_frame.iterrows():
        if row['moonset'] > row['moonrise'] or row['moonset'] < row['moonrise']:
            sun_frame.loc[idx, 'moon_start'] = row['moonrise']
            sun_frame.loc[idx, 'moon_end'] = row['moonset']
        if pd.isna(row['moonset']):
            sun_frame.loc[idx, 'moon_start'] = row['moonrise']
            sun_frame.loc[idx, 'moon_end'] = 360
        if pd.isna(row['moonrise']):
            sun_frame.loc[idx, 'moon_start'] = 0
            sun_frame.loc[idx, 'moon_end'] = row['moonset']
        if pd.isna(row['moon_transit']):
            sun_frame.loc[idx, 'moon_start'] = row['moonrise']
            sun_frame.loc[idx, 'moon_end'] = row['moonset']
            sun_frame.loc[idx, 'moon_transit'] = 0
    print_file_exists(sun_frame.write(sun_path))

    for csv_file in tt_files:
        print(f'Adding sun and moon data to {csv_file}')
        tt_frame = DataFrame(csv_source=csv_file)
        for row in range(len(sun_frame)):
            target_date = str(sun_frame.loc[row]['date'].date())
            for c in sun_frame.columns.to_list()[1:]:
                tt_frame.loc[tt_frame.date == target_date, c] = sun_frame.loc[row][c]
        tt_frame.write(csv_file)
