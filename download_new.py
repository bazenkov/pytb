import os
import datetime
import time
import io
#import rest as r
#import tqdm
#import pandas as pd
import json
import os.path as path
import sys
import csv
import tb_rest as tb


class ConnectionError(Exception):
    def __init__(self, tb_connection, resp):
        self.connection = tb_connection
        self.resp = resp
        self.message = resp.message

class TbConnection:
    TOKEN_EXPIRE_SEC = datetime.timedelta(seconds = 700)
    
    def __init__(self, tb_url, tb_user, tb_password):
        print(f"Authorization at {tb_url} as {tb_user}")
        bearer_token, refresh_token, resp = tb.getToken(tb_url, tb_user, tb_password)    
        if not tb.request_success(resp):
            raise ConnectionError(self, resp)
        self.user = tb_user
        self.password = tb_password
        self.token = bearer_token
        self.refresh_token = refresh_token
        self.url = tb_url
        self.token_time = datetime.datetime.now()
    
    def expired(self):
        return datetime.datetime.now() - self.token_time >= self.TOKEN_EXPIRE_SEC

    def update_token(self, force = False):
        if self.expired() or force:
            print("Refreshing token ...")
            self.token, self.refresh_token, tokenAuthResp = tb.refresh_token(self.url, self.token, self.refresh_token)
            if not tb.request_success(tokenAuthResp):
                print("Token refresh failed, obtaining a new token ...")
                self.token, self.refresh_token, new_resp = tb.getToken(self.url, self.user, self.password)   
                if not tb.request_success(new_resp):
                    print("Authorization request failed")
                    raise ConnectionError(self, new_resp)
    def get_token(self):
        self.update_token()
        return self.token

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
        # df_prev = pd.read_csv(file)
        # start_ts = df_prev['ts'].iloc[-1]
        # end_ts = int(time.time()) * 1000
        raise NotImplementedError("Must read from file here")

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
            #print(row['ts'])
            pretty_time = str_ts(round(tb.fromJsTimestamp(row['ts'])))
            csvfile.write(f"{pretty_time}{delimeter}{row['value']}\n")
            #csvfile.write(f"{row['ts']}{delimeter}{row['value']}\n")

def str_ts(ts):
        return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")

def get_data(tb_url, bearer_token, name, _id, start_time, end_time, device_folder, keys):
    
    for key in keys:
        file = device_folder + f'/{key}.csv'
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
                segment = tb.get_timeseries(tb_url, bearer_token, _id, [key], 
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

def key_file(device_folder, key):
    return device_folder + f'/{key}.csv'

DELIMETER = ','

def get_data_noseg(tb_connection, device_folder, device_name, device_id, start_time, end_time, keys):
    for key in keys:
        file = key_file(device_folder, key)
        start_ts, end_ts = check_interval(start_time, end_time, file)
        #data = {}
        print(f'Downloading data for {device_name}, parameter: {key}, interval: {str_ts(start_ts)}-{str_ts(end_ts)}')
        resp = tb.get_timeseries(tb_connection.url, device_id, tb_connection.get_token(), [key], 
                                    tb.toJsTimestamp(start_ts), tb.toJsTimestamp(end_ts), 
                                    limit = tb.SEC_IN_DAY)
        if not tb.request_success(resp):
            print(f"ERROR at key {key} for device {device_name}.")
            print(f"Code={resp.status_code}")
            print(resp.json())
        else:
            data_key = resp.json()
            print(f'Writing to csv...', end=' ')
            sorted_values = sorted(data_key[key], key = lambda x: x['ts'])
            print_to_csv(file, sorted_values, key, delimeter = DELIMETER)
            print('Done')

def get_last_time(device_folder, key):
    with open(key_file(device_folder, key), 'r') as file:
        lines = file.read().splitlines()
        ts = float(lines[-1].split(DELIMETER)[0])
        last_time = datetime.datetime().fromtimestamp(ts)
    return last_time

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

def load_all_data(tb_connection, device_folder, devices, start_time, end_time, keys):
    for device_name, _id in devices.items():
        device_folder = f'{data_dir}/{device_name}'
        check_dir(device_folder)
        #get_data_seg(tb_connection, device_folder, device_name, _id, start_time, end_time, keys)
        get_data_noseg(tb_connection, device_folder, device_name, _id, start_time, end_time, keys)


if __name__ == "__main__":
    tb_url, tb_user, tb_password = get_tb_params(sys.argv)
    data_dir, start_time, end_time, keys, devices = get_config_params(sys.argv)
    check_dir(data_dir)
    tb_connection = TbConnection(tb_url, tb_user, tb_password)
    load_all_data(tb_connection, data_dir, devices, start_time, end_time, keys)

