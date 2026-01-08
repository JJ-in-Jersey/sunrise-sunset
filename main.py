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
    # tt_files = [Path(file) for file in fair_currents if 'fair currents' in file or 'savitsky golay' in file]
    tt_files = [Path(file) for file in fair_currents if 'aggregate_transit_times' in file and 'csv' in file]

    start = dt(2025, 12, 1)
    end = dt(2027, 1, 31)

    moon_path = env('user_profile').joinpath('Fair Currents/major-moon-phases.csv')
    if moon_path.exists():
        moon_frame = DataFrame(csv_source=moon_path)
        moon_frame['date'] = pd.to_datetime(moon_frame.date)
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
        moon_frame = moon_frame[(moon_frame['date'] >= start) & (moon_frame['date'] <= end)].reset_index()
        print_file_exists(moon_frame.write(moon_path))

    sun_temp = Template(f'{str(env('user_profile').joinpath('Fair Currents/sunrise-sunset'))}-$BASE.csv')

    sun_path = Path(sun_temp.substitute(BASE='original'))
    if sun_path.exists():
        sun_frame = DataFrame(csv_source=sun_path)
        sun_frame['date'] = pd.to_datetime(sun_frame.date)
    else:
        frames = []
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
                sd = {d['phen']: d['time'].split()[0].split(':') for d in response_dict['properties']['data']['sundata']}
                md = {d['phen']: d['time'].split()[0].split(':') for d in response_dict['properties']['data']['moondata']}
                sunrise = date.replace(hour=int(sd['Rise'][0]), minute=int(sd['Rise'][1]))
                sunset = date.replace(hour=int(sd['Set'][0]), minute=int(sd['Set'][1]))
                sunrise_golden_hour = sunrise - pd.Timedelta(minutes=30)
                sunset_golden_hour = sunset + pd.Timedelta(minutes=30)
                frame_dict = {'date': date,
                              'sunrise': sunrise,
                              'sunset': sunset,
                              'sunrise golden hour': sunrise_golden_hour,
                              'sunset golden hour': sunset_golden_hour,
                              'moon rise': None if md.get('Rise') is None else date.replace(hour=int(md['Rise'][0]), minute=int(md['Rise'][1])),
                              'moon transit': None if md.get('Upper Transit') is None else date.replace(hour=int(md['Upper Transit'][0]), minute=int(md['Upper Transit'][1])),
                              'moon set': None if md.get('Set') is None else date.replace(hour=int(md['Set'][0]), minute=int(md['Set'][1])),
                              'sunrise angle': time_to_degrees(':'.join(sd['Rise'])),
                              'sunset angle': time_to_degrees(':'.join(sd['Set'])),
                              'sunrise golden hour angle': time_to_degrees(sunrise_golden_hour),
                              'sunset golden hour angle': time_to_degrees(sunset_golden_hour),
                              'moon rise angle': None if md.get('Rise') is None else time_to_degrees(':'.join(md['Rise'])),
                              'moon transit angle': None if md.get('Upper Transit') is None else time_to_degrees(':'.join(md['Upper Transit'])),
                              'moon set angle': None if md.get('Set') is None else time_to_degrees(':'.join(md['Set'])),
                              'moon phase': response_dict['properties']['data']['curphase']
                              }
                frames.append(frame_dict)
            except requests.exceptions.RequestException as e:
                print(e)
            date = date + td(days=1)
        sun_frame = DataFrame(frames)
        print_file_exists(sun_frame.write(sun_path))

    sun_path = Path(sun_temp.substitute(BASE='final'))
    for idx, row in moon_frame.iterrows():
        if idx == 0:
            sun_frame.loc[sun_frame.date == row.date, 'moon phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date + td(days=1), 'moon phase'] = row.phase
        elif idx == len(sun_frame) - 1:
            sun_frame.loc[sun_frame.date == row.date - td(days=1), 'moon phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date, 'moon phase'] = row.phase
        else:
            sun_frame.loc[sun_frame.date == row.date - td(days=1), 'moon phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date, 'moon phase'] = row.phase
            sun_frame.loc[sun_frame.date == row.date + td(days=1), 'moon phase'] = row.phase
    print_file_exists(sun_frame.write(sun_path))

    for csv_file in tt_files:
        print(f'Adding sun and moon data to {csv_file}')
        frame = DataFrame(csv_source=csv_file)
        for row in range(len(sun_frame)):
            target_date = str(sun_frame.loc[row]['date'].date())
            for c in sun_frame.columns.to_list()[1:]:
                frame.loc[frame.date == target_date, c] = sun_frame.loc[row][c]
        frame.write(csv_file)
