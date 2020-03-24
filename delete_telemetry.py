"""Delete telemetry from Thingsboard for the specified devices, keys and time.
"""
#import io
#import json
import sys
#import os.path as path
import download_new as dwn
import tb_rest as tb
import datetime as dt


TIME_FORMAT = "%d.%m.%Y %H:%M:%S"

def delete_all_data(tb_connection, devices, keys, start_time, end_time):
    """
    devices - {'name':'id',...}
    """
    for name, _id in devices.items():
        print(f"Delete telemetry for {name}. From {start_time.strftime(TIME_FORMAT)} to {end_time.strftime(TIME_FORMAT)}...", end = " ")
        #for k in keys:
        #    print(k, end = " ")            
        resp = tb.delete_telemetry(tb_connection.url, tb_connection.get_token(), _id, keys, start_time, end_time)
        if tb.request_success(resp):
            print("Success")
        else:
            print("ERROR")
            print(f"Code={resp.status_code}")
            print(resp.json())


def get_config_params(argv):
    config_file = argv[2]
    with open(config_file, 'r') as f:
        params = json.load(f)
    params['start_time'] = dt.datetime.strptime(params['start_time'], TIME_FORMAT)
    if params['end_time']:
        params['end_time'] = dt.datetime.strptime(params['end_time'], TIME_FORMAT)
    else:
        params['end_time'] = dt.datetime.now()
    return params

if __name__ == "__main__":
    tb_url, tb_user, tb_password = dwn.get_tb_params(sys.argv)
    params = dwn.get_config_params(sys.argv)
    tb_connection = tb.TbConnection(tb_url, tb_user, tb_password)
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