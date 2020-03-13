import os
import datetime
import time
import io
#import rest as r
#import tqdm
import pandas as pd
import json
import os.path as path
import sys
import csv
import tb_rest as tb

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
        #Here the timestamp is multiplied by 1000
        #It shouldn't
        df_prev = pd.read_csv(file)
        start_ts = df_prev['ts'].iloc[-1]
        end_ts = int(time.time()) * 1000

    else:
        #Here the timestamp is not multiplied.
        #
        start_ts = time.mktime(time.strptime(start_time, "%d.%m.%Y %H:%M:%S"))
        end_ts = time.mktime(time.strptime(end_time, "%d.%m.%Y %H:%M:%S"))

    return start_ts, end_ts

def print_to_csv(file, values, key, mode = 'w',  delimeter = ','):
    with io.open(file, mode, newline='', encoding='utf-8') as csvfile:
        if mode == 'w':
            csvfile.write(f"ts{delimeter}{key}\n")
        for row in values:
            csvfile.write(f"{row['ts']}{delimeter}{row['value']}\n")


def get_data(tb_url, bearer_token, name, _id, start_time, end_time, device_folder, keys):
    def str_ts(ts):
        return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")

    for key in keys:
        file = meter_folder + f'/{key}.csv'
        start_ts, end_ts = check_interval(start_time, end_time, file)
        data = {}
        #date_start = datetime.datetime.fromtimestamp(start_ts / 1000).strftime("%d.%m.%Y %H:%M:%S")
        #date_end = datetime.datetime.fromtimestamp(end_ts / 1000).strftime("%d.%m.%Y %H:%M:%S")
        print(f'Downloading data for {name}, parameter: {key}, interval: {str_ts(start_ts)}-{str_ts(end_ts)}')
        #print(f'Downloading data for {name}, parameter: {key}, interval: {start_time}-{end_time}')
        data_key = tb.get_timeseries(tb_url, _id, bearer_token, [key], 
                                    tb.toJsTimestamp(start_ts), tb.toJsTimestamp(end_ts)).json()
        segments = 1
        
        #The next piece of code is turned off
        while False and list(data_key.keys())[0] != key:
            time.sleep(5)
            segments += 1
            intervals = timeint_div(start_ts, end_ts, segments)
            data_key = {key: []}
            print('Too large amount of data, getting segment')
            for interval in intervals:
                #date_start = datetime.datetime.fromtimestamp(interval[0] / 1000).strftime('%d.%m')
                #date_end = datetime.datetime.fromtimestamp(interval[1] / 1000).strftime('%d.%m')
                print(f'{str_ts(interval[0])}-{str_ts(interval[1])}')
                segment = tb.get_timeseries(bearer_token, _id, [key], 
                                        tb.toJsTimestamp(interval[0]), tb.toJsTimestamp(interval[1])).json()
                if list(segment.keys())[0] != key:
                    break
                else:
                    data_key[key] += segment[key]

        #data['ts'] = []
        #data[key] = []
        #for value in data_key[key]:
        #    data[key].append(value['value'])
        #    data['ts'].append(value['ts'])
        #result = pd.DataFrame(data)
        #result.sort_values(by=['ts'])
        print(f'Writing to csv...', end=' ')
        if not os.path.isfile(file):#write to file
            #result.to_csv(file, index=False)
            print_to_csv(device_folder, data_key, key, delimeter = ',')
        else:#append to file
            #result.to_csv(file, mode='a', index=False,  header=False)
            print_to_csv(device_folder, data_key, key, mode = 'a',  delimeter = ',')
        print('Done')


def get_data_noseg(tb_url, bearer_token, name, _id, start_time, end_time, device_folder, keys):
    def str_ts(ts):
        return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")

    for key in keys:
        file = device_folder + f'/{key}.csv'
        start_ts, end_ts = check_interval(start_time, end_time, file)
        data = {}
        print(f'Downloading data for {name}, parameter: {key}, interval: {str_ts(start_ts)}-{str_ts(end_ts)}')
        resp = tb.get_timeseries(tb_url, _id, bearer_token, [key], 
                                    tb.toJsTimestamp(start_ts), tb.toJsTimestamp(end_ts), 
                                    limit = tb.SEC_IN_DAY)
        if resp.status_code != 200:
            print(f"ERROR at key {key} for device {name}.")
            print(f"Code={resp.status_code}")
            print(resp.json())
        else:
            data_key = resp.json()
            print(f'Writing to csv...', end=' ')
            sorted_values = sorted(data_key[key], key = lambda x: x['ts'])
            #if not os.path.isfile(file):
            #write to a clean file
            print_to_csv(file, sorted_values, key, delimeter = ',')
            #else:
            #append to file
            #    print_to_csv(file, sorted_values, key, mode = 'a',  delimeter = ',')
            print('Done')

def get_tb_params(argv):
    access_file = argv[1]
    tb_params = tb.load_access_parameters(access_file)
    return tb_params["url"], tb_params["user"], tb_params["password"]

config_file = "config.txt"
def get_config_params(argv):
    if len(argv) > 2:
        config_file = argv[2]
    with open(config_file, 'r') as f:
        params = json.load(f)
    return tuple(params.values())

def check_dir(folder):
    if not path.exists(folder):
        os.mkdir(folder)

if __name__ == "__main__":
    tb_url, tb_user, tb_password = get_tb_params(sys.argv)
    data_dir, start_time, end_time, keys, meter_ids = get_config_params(sys.argv)
    check_dir(data_dir)
    bearer_token = tb.get_token(tb_url, tb_user, tb_password)[0]
    for name, _id in meter_ids.items():
        meter_folder = f'{data_dir}/{name}'
        check_dir(meter_folder)
        get_data_noseg(tb_url, bearer_token, name, _id, start_time, end_time, meter_folder, keys)

