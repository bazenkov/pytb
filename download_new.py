import os
import datetime
import time
import rest as r
import tqdm
import pandas as pd
import json


def timeint_div(start_ts, end_ts, segments):
    date_x1 = start_ts
    date_x2 = date_x1
    delta = (end_ts - start_ts) // segments
    intervals = []
    while date_x2 < end_ts:
        date_x2 += delta
        intervals.append((date_x1, date_x2))
        date_x1 = date_x2
    intervals.reverse()
    return intervals


def check_interval(start_time, end_time, file):

    if not start_time or not end_time:
        df_prev = pd.read_csv(file)
        start_ts = df_prev['ts'].iloc[-1]
        end_ts = int(time.time()) * 1000

    else:
        start_ts = time.mktime(time.strptime(start_time, "%d.%m.%Y %H:%M:%S"))
        end_ts = time.mktime(time.strptime(end_time, "%d.%m.%Y %H:%M:%S"))

    return start_ts, end_ts


def get_data(name, _id, start_time, end_time, meter_folder, keys):

    for key in keys:
        file = meter_folder + f'/{key}.csv'
        start_ts, end_ts = check_interval(start_time, end_time, file)
        data = {}
        date_start = datetime.datetime.fromtimestamp(start_ts / 1000).strftime("%d.%m.%Y %H:%M:%S")
        date_end = datetime.datetime.fromtimestamp(end_ts / 1000).strftime("%d.%m.%Y %H:%M:%S")
        print(f'Downloading data for {name}, parameter: {key}, interval: {date_start}-{date_end}')
        data_key = r.get_timeseries(bearer_token, _id, [key], start_ts, end_ts).json()

        segments = 1
        while list(data_key.keys())[0] != key:
            time.sleep(5)
            segments += 1
            intervals = timeint_div(start_ts, end_ts, segments)
            data_key = {key: []}
            print('Too large amount of data, getting segment')
            for interval in intervals:
                date_start = datetime.datetime.fromtimestamp(interval[0] / 1000).strftime('%d.%m')
                date_end = datetime.datetime.fromtimestamp(interval[1] / 1000).strftime('%d.%m')
                print(f'{date_start}-{date_end}')
                segment = r.get_timeseries(bearer_token, _id, [key], interval[0], interval[1]).json()
                if list(segment.keys())[0] != key:

                    break
                else:
                    data_key[key] += segment[key]

        data['ts'] = []
        data[key] = []
        for value in data_key[key]:
            data[key].append(value['value'])
            data['ts'].append(value['ts'])
        result = pd.DataFrame(data)
        result.sort_values(by=['ts'])
        print(f'Writing to csv...', end=' ')
        if not os.path.isfile(file):
            result.to_csv(file, index=False)
        else:
            result.to_csv(file, mode='a', index=False,  header=False)
        print('Done')


if __name__ == "__main__":

    with open('config.txt', 'r') as f:
        params = json.load(f)

    bearer_token = r.get_token()[0]
    _dir, start_time, end_time, keys, meter_ids = tuple(params.values())

    for name, _id in meter_ids.items():
        _dir = r'D:\ics_data'
        meter_folder = _dir + f'/{name}'
        if not os.path.isdir(meter_folder):
            os.mkdir(meter_folder)
        get_data(name, _id, start_time, end_time, meter_folder, keys)

