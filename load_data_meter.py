import urllib.request
import requests
import datetime
import tb_rest as tb
import json as json

def list_devices(tb_url, jwtToken, types_keys, customerId):
    device_list = []
    for type in types_keys.keys():
        resp = tb.get_devices(tb_url, jwtToken, customerId, device_type=type, limit=300)
        if len(resp)>0:
            print('Loaded type \'' + type + '\' : ' + str(len(resp)))
            for device_data in resp:
                device_list.append({'id':device_data['id']['id'], 'name':device_data['name'], 'type':type, 'keys':types_keys[type]})
            
    return device_list

def load_telemetry(tb_url, jwtToken, device_list, startTs, endTs):
    for device in device_list:
        resp = tb.getTimeseries(tb_url, device['id'], jwtToken, device['keys'], startTs, endTs, limit=100000)
        if resp.status_code == 200:
            device['data'] = resp.json()
        else:
            print(resp)
    return device_list


def format_ts(ts):
    dt = datetime.datetime.fromtimestamp(tb.fromJsTimestamp( ts ) )
    dt = dt.replace(microsecond=0)
    return str(dt)

def print_csv(device_list, folder):
    for i,device in enumerate(device_list):
        #table = make_table(device['data'])
        fileName = folder + '/' + device['type'] + ' ' + str(i+1) + '.csv'
        CSV_DELIM = ';'
        with open(fileName, 'w') as file:
            file.write( CSV_DELIM.join([device['id'], device['name']]) + '\n' )
            for key in device['data'].keys():
                #key_ts = [ str(data_entry['ts']) for data_entry in device['data'][key] ]
                sorted_data = sorted(device['data'][key], key = lambda x: x['ts'])
                key_ts = [ format_ts( data_entry['ts'] ) for data_entry in sorted_data ]
                key_val = [ str(data_entry['value']) for data_entry in sorted_data ]
                row_ts = ['ts_' + key ] + key_ts
                row_val = [key] + key_val   
                file.write(CSV_DELIM.join(row_ts) + '\n')
                file.write(CSV_DELIM.join(row_val) + '\n')
            #file.write('ts;'+ ';'.join( sorted(device.keys() ) ) + '\n' )    
    
if __name__ == "__main__":
    file = "tb.access"
    device_type = 'Mercury'
    keys = ['P1','P2', 'P3', 'U1','U2','U3','I1', 'I2','I3' ,'Q1','Q2','Q3']
    types_keys = {device_type:keys}
    #startTs = tb.toJsTimestamp(datetime.datetime(2019, 4, 1, hour = 0).timestamp())
    startTs = tb.toJsTimestamp(datetime.datetime(2019, 10, 1, hour = 0).timestamp())
    endTs = tb.toJsTimestamp(datetime.datetime(2019, 11, 16, hour = 12).timestamp())

    params = tb.load_access_parameters(file)
    tb_url, tb_user, tb_password, customerId = [params["url"], params["user"], params["password"], params["ipu_customer_id"]]

    print('Loading token ...')
    bearerToken, refreshToken, resp = tb.getToken(tb_url, tb_user, tb_password)
    devices = list_devices(tb_url, bearerToken, types_keys, customerId)[0:2]
   # for d in devices:
   #     d["keys"] = keys

    #deviceId = params["device_id"]
    #devices = [{'id':deviceId, 'keys':keys, 'type':'Mercury', 'name':'Smart Meter 45'}]
    
    
    print('Total: ' + str(len(devices)) + ' devices')
    print(devices)
    print('Loading telemetry ...')
    devices = load_telemetry(tb_url, bearerToken, devices, startTs, endTs)
    folder = './csv/Mercury'
    #jsonFile = devices[0]['name'] + '.json'
    #with open(jsonFile, 'w') as file:
    #    json.dump(devices, file)
    #with open(jsonFile, 'w') as file:
    #    file.write(str(devices))
    print_csv(devices, folder)
    
   




