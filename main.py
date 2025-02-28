from datetime import datetime as dt, timedelta as td
import requests
import json
import pandas as pd
from pathlib import Path
from tt_file_tools.file_tools import write_df, print_file_exists, list_all_files, read_df
from tt_date_time_tools.date_time_tools import time_to_degrees
from tt_os_abstraction.os_abstraction import env

if __name__ == '__main__':

    file_list = list_all_files(env('user_profile').joinpath('Fair Currents'))
    tt_files = [Path(file) for file in file_list if "transit times" in file]

    frame_path = env('user_profile').joinpath('Fair Currents/sunrise-sunset.csv')
    frame = pd.DataFrame(columns=['date', 'RISE degrees', 'SET degrees', 'fracillum', 'fracillum index', 'Begin Civil Twilight', 'Rise',  'End Golden Hour', 'Begin Golden Hour', 'Set', 'End Civil Twilight'])

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
            fracillum = int(response_dict['properties']['data']['fracillum'][:-1])
            sd = response_dict['properties']['data']['sundata']
            frame_dict = {'date': date, 'fracillum': fracillum, sd[0]['phen']: sd[0]['time'].split(' ')[0] + ':00', sd[1]['phen']: sd[1]['time'].split(' ')[0] + ':00', sd[3]['phen']: sd[3]['time'].split(' ')[0] + ':00', sd[4]['phen']: sd[4]['time'].split(' ')[0] + ':00'}
            frame.loc[len(frame)] = frame_dict
        except requests.exceptions.RequestException as e:
            print(e)
        date = date + td(days=1)

    frame['fracillum index'] = frame.fracillum.apply(lambda x: int(x/5))
    frame['BCT degrees'] = frame['Begin Civil Twilight'].apply(time_to_degrees)
    frame['RISE degrees'] = frame['Rise'].apply(time_to_degrees)
    frame['End Golden Hour'] = pd.to_datetime(frame['Begin Civil Twilight'], format='mixed') + pd.Timedelta(hours=1)
    frame['End Golden Hour'] = frame['End Golden Hour'].dt.time
    frame['EGH degrees'] = frame['End Golden Hour'].apply(time_to_degrees)
    frame['Begin Golden Hour'] = pd.to_datetime(frame['End Civil Twilight'], format='mixed') - pd.Timedelta(hours=1)
    frame['Begin Golden Hour'] = frame['Begin Golden Hour'].dt.time
    frame['BGH degrees'] = frame['End Golden Hour'].apply(time_to_degrees)
    frame['SET degrees'] = frame['Set'].apply(time_to_degrees)
    frame['ECT degrees'] = frame['End Civil Twilight'].apply(time_to_degrees)

    print_file_exists(write_df(frame, frame_path))

    for csv_file in tt_files:
        print(f'Adding RISE and SET to {csv_file}')
        tt_frame = read_df(csv_file)
        for row in range(len(frame)):
            target_date = f'{frame.loc[row]['date'].month}/{frame.loc[row]['date'].day}/{frame.loc[row]['date'].year}'  # strftime hack
            tt_frame.loc[tt_frame.date == target_date, 'RISE Degrees'] = frame.loc[row]['RISE degrees']
            tt_frame.loc[tt_frame.date == target_date, 'SET Degrees'] = frame.loc[row]['SET degrees']
        write_df(tt_frame, csv_file)
        pass