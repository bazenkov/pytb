import os
import datetime
import time
import io
import json
import os.path as path
import sys
from collections import namedtuple
from unidecode import unidecode
#import csv
import tb_rest as tb


TIME_FORMAT = "%d.%m.%Y %H:%M:%S"
FILE_MODE = 'a'
DELIMETER = ','
MIN_TIME_DELTA = datetime.timedelta(seconds=2)
SEC_IN_HOUR = 60*60
TIME_SEGMENT = datetime.timedelta(seconds = 3*SEC_IN_HOUR)

TimeSegment = namedtuple('TimeSegment', ['start', 'end'] )

class ConnectionError(Exception):
    def __init__(self, tb_connection, resp):
        self.connection = tb_connection
        self.resp = resp
        self.message = resp.message

class TbConnection:
    TOKEN_EXPIRE_SEC = datetime.timedelta(seconds = 800)
    
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

def time_segments(start_time, end_time, delta = TIME_SEGMENT):
    if not long_interval(start_time, end_time):
        raise ValueError(f"End time = {end_time} < start time {start_time}")
    (num_seg, last_seg) = divmod(end_time - start_time, delta)
    segments = []
    seg_start_time = start_time
    for _ in range(num_seg):
        segments.append(TimeSegment(seg_start_time, seg_start_time + delta))
        seg_start_time += delta
    if last_seg > MIN_TIME_DELTA:
        segments.append(TimeSegment(seg_start_time, seg_start_time + last_seg))
    return segments

def check_interval(start_time, end_time, file, file_mode):
    def check_in_file():
        if os.path.exists(file):
            with open(file, 'r') as f:
                lines = f.readlines()
                if len(lines) > 1:
                    time_line = lines[-1].split(DELIMETER)[0]
                    #return datetime.datetime.strptime(time_line, TIME_FORMAT)
                    return datetime.datetime.fromtimestamp(tb.fromJsTimestamp(time_line))
        return None
    if file_mode == 'a':
        start_time_file = check_in_file()
        if start_time_file:
            #shift start time, so the first time point is not duplicated
            start_time_file = start_time_file + datetime.timedelta(milliseconds=100)       
            start_time = max(start_time, start_time_file)
    if not end_time:
        end_time = datetime.datetime.now()
    #start_ts = start_time.timestamp()#time.mktime(time.strptime(start_time, "%d.%m.%Y %H:%M:%S"))
    #end_ts = end_time.timestamp()#time.mktime(time.strptime(end_time, "%d.%m.%Y %H:%M:%S"))
    return start_time, end_time

def print_to_csv(file, values, key, mode,  delimeter = DELIMETER):
    def print_header():
        csvfile.write(f"ts{delimeter}{key}\n")

    need_header = False
    if not path.exists(file) or mode == 'w':
        need_header = True
    with io.open(file, mode, newline='', encoding='utf-8') as csvfile:
        if need_header:
            print_header()
        for row in values:
            #pretty_time = str_ts(round(tb.fromJsTimestamp(row['ts'])))
            csvfile.write(f"{row['ts']}{delimeter}{row['value']}\n")

def str_ts(ts):
    return datetime.datetime.fromtimestamp(ts).strftime(TIME_FORMAT)

def key_file(device_folder, key):
    return device_folder + f'/{key}.csv'

def long_interval(start_time, end_time):
    return  end_time - start_time >= MIN_TIME_DELTA

def get_data_noseg(tb_connection, file, device_name, device_id, start_time, end_time, key, file_mode = FILE_MODE):
    if long_interval(start_time, end_time):
        print(f'Downloading data for {device_name}, key: {key}, \
                    interval: {start_time.strftime(TIME_FORMAT)}-{end_time.strftime(TIME_FORMAT)}')
        resp = tb.get_timeseries(tb_connection.url, device_id, tb_connection.get_token(), [key], 
                                    tb.toJsTimestamp(start_time.timestamp()), 
                                    tb.toJsTimestamp(end_time.timestamp()), 
                                    limit = tb.SEC_IN_DAY)
        if not tb.request_success(resp):
            print(f"ERROR at key {key} for device {device_name}.")
            print(f"Code={resp.status_code}")
            print(resp.json())
        else:
            data_key = resp.json()
            if len(data_key)>0:
                print(f'Writing to csv...', end=' ')
                sorted_values = sorted(data_key[key], key = lambda x: x['ts'])
                print_to_csv(file, sorted_values, key, mode = file_mode)
                print('Done')
            else:
                print('No data found')
        return resp
    else:
        print(f"Skipped for {device_name}, key: {key}, \
                    interval: {start_time.strftime(TIME_FORMAT)}-{end_time.strftime(TIME_FORMAT)} \
                    is too short")
        return None

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
    if 'file_mode' not in params :
        params['file_mode'] = FILE_MODE
    params['start_time'] = datetime.datetime.strptime(params['start_time'], TIME_FORMAT)
    if params['end_time']:
        params['end_time'] = datetime.datetime.strptime(params['end_time'], TIME_FORMAT)
    else:
        params['end_time'] = datetime.datetime.now()
    if 'time_segment_hrs' in params.keys():
        params['time_delta'] = datetime.timedelta(seconds = SEC_IN_HOUR * int(params['time_segment_hrs']))#in hours
    return params

def check_dir(folder):
    if not path.exists(folder):
        os.mkdir(folder)

def load_all_data(tb_connection, data_dir, devices, start_time, end_time, time_delta, keys, file_mode):
    for device_name, _id in devices.items():
        load_device_data(tb_connection, data_dir, device_name, _id, start_time, end_time, time_delta, keys, file_mode)
    

def load_device_data(tb_connection, data_dir, device_name, device_id, start_time, end_time, time_delta, keys, file_mode):
    device_folder = f'{data_dir}/{device_name}'
    check_dir(device_folder)
    for key in keys:
        file = key_file(device_folder, key)
        key_start_time, key_end_time = check_interval(start_time, end_time, file, file_mode)
        if long_interval(key_start_time, key_end_time):
            #divide time into segments
            segments = time_segments(key_start_time, key_end_time, delta = time_delta)
            if segments:
                if file_mode == 'w':#if mode = 'write', the first segment is written in 'w' mode
                    resp = get_data_noseg(tb_connection, file, device_name, device_id, segments[0].start, segments[0].end, key, file_mode = 'w')
                else:
                    resp = get_data_noseg(tb_connection, file, device_name, device_id, segments[0].start, segments[0].end, key, file_mode = 'a')
                if not tb.request_success(resp):
                    return resp
                #other segments is written in 'append' mode
                for seg in segments[1:]:
                    resp = get_data_noseg(tb_connection, file, device_name, device_id, seg.start, seg.end, key, file_mode = 'a')
                    if not tb.request_success(resp):
                        return resp
        else:
            print(f"WARNING: small segment for device {device_name}, key {key}: {key_start_time} to {key_end_time}")

def name_id_list(entities):
    return {valid_name(x['name']):x['id']['id'] for x in entities}


FORBIDDEN_SYMBOLS = ",\"*/:<>?\\|+;=()[] "
REPLACE_SYMBOLS = "_"*len(FORBIDDEN_SYMBOLS)
TRANS_TABLE = FORBIDDEN_SYMBOLS.maketrans(FORBIDDEN_SYMBOLS, REPLACE_SYMBOLS)

def valid_name(device_name):
    return unidecode(device_name.translate(TRANS_TABLE)).replace("__", "_").replace("___", "_").strip("_")

if __name__ == "__main__":
    tb_url, tb_user, tb_password = get_tb_params(sys.argv)
    #data_dir, start_time, end_time, keys, devices, file_mode, device_type = get_config_params(sys.argv)
    params = get_config_params(sys.argv)
    check_dir(params['folder'])
    tb_connection = TbConnection(tb_url, tb_user, tb_password)
    if params['device_type']:#if device type is specified, ignore the specified devices
        devices, resp = tb.get_tenant_devices(tb_connection.url, tb_connection.get_token(), params['device_type'])
        if not tb.request_success(resp):
            raise ConnectionError(self, resp)
        params['devices'] = name_id_list(devices)
    print("*** Download started ***")
    print(f"Total interval: {params['start_time'].strftime(TIME_FORMAT)}-{params['end_time'].strftime(TIME_FORMAT)}")
    load_all_data(tb_connection, params['folder'], params['devices'], 
                    params['start_time'], params['end_time'], params['time_delta'],
                    params['keys'], params['file_mode'])
    print("*** Download finished ***")