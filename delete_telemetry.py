"""Delete telemetry from Thingsboard for the specified devices, keys and time.
"""
#import io
#import json
import sys
import json
from queue import Queue
import download_new as dwn
import tb_rest as tb
import datetime as dt


TIME_FORMAT = "%d.%m.%Y %H:%M:%S"
MAX_TRY = 50

def delete_all_data(tb_connection, devices, keys, start_time, end_time):
    """
    devices - {'name':'id',...}
    """

    device_queue = Queue()
    for name, _id in devices.items():
        elem = {'name': name, 'id': _id, 'num_try': 0}
        device_queue.put(elem)

    done = False
    while not done:
        if not device_queue.empty():
            elem = device_queue.get()
            print(f"Delete telemetry for {elem['name']}. \
                From {start_time.strftime(TIME_FORMAT)} to {end_time.strftime(TIME_FORMAT)}...", end = " ")         
            resp = tb.delete_telemetry(tb_connection.url, tb_connection.get_token(), elem['id'], keys, start_time, end_time)
            if tb.request_success(resp):
                print("Success")
            elif resp.status_code == 204:
                print("No data")
            else:
                print("ERROR")
                print(f"Code={resp.status_code}")
                print(resp.json())
                if elem['num_try'] < MAX_TRY:
                    elem['num_try'] += 1
                    device_queue.put(elem)
                else:
                    print(f"Max number of tries={elem['num_try']} reached for {elem['name']}")
        else:
            done = True


def get_config_params(config_file):
    with open(config_file, 'r') as f:
        params = json.load(f)
    params['start_time'] = dt.datetime.strptime(params['start_time'], TIME_FORMAT)
    if params['end_time']:
        params['end_time'] = dt.datetime.strptime(params['end_time'], TIME_FORMAT)
    else:
        params['end_time'] = dt.datetime.now()
    return params

if __name__ == "__main__":
    params = get_config_params(sys.argv[1])
    if "password" not in params.keys():
        params['password'] = input("Enter password: ")
    tb_connection = tb.TbConnection(params['url'], params['user'], params['password'])
    if 'device_type' in params.keys():#if device type is specified, ignore the specified devices
        devices, resp = tb.get_tenant_devices(tb_connection.url, tb_connection.get_token(), params['device_type'])
        if not tb.request_success(resp):
            raise tb.ConnectionError(self, resp)
        params['devices'] = dwn.name_id_dict(devices)
    print("*** Purge started ***")
    print(f"Total interval: {params['start_time'].strftime(TIME_FORMAT)}-{params['end_time'].strftime(TIME_FORMAT)}")
    delete_all_data(tb_connection, params['devices'], params['keys'], 
                    params['start_time'], params['end_time'])
    print("*** Purge finished ***")