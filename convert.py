#Convert raw database dump to the dataset
#The dataset contains the following files:
#device_id_1.csv
#...
#device_id_n.csv
#
#devices.csv
#location.csv
#
#Each device file contains the timestamps and the values as follows:
# ts;temperature;humidity
#1583010039249;18.9;NaN
#1583010040249;19.0;65.0
#
#File devices.csv contains information about devices identity:
#label;name;type;tb_id;cmdb_id
#
#File location.csv contains information about where each device was placed at
#the given time
#
#device;ts;location
#
#Input is a csv file which contains the rows dumped from ts_kv table in the database:
#DEVICE;1ea47494dc14d40bd76a73c738b665f;Temperature;1583010011665;;;;-1.8
#DEVICE;1ea4e683c20f6a0a4e57bde087d24ee;temperature;1583010039249;;;;18.9
#
#The header is usually absent in the file, but the column names are:
#entity_type ; entity_id ; key ; ts ; bool_v ; str_v ; long_v ; dbl_v
#
#
#
#Usage
#convert device.csv ts_kv.csv output_folder
#
#The output csv files is named like device_name.csv
#The name consists of the device name where non-latin symbols and symbols not allowed for file naming are removed or replaced by _
#If the specified output folder already contains a file device_id.csv, 
#the new data are appended at the end. 
#No consistency check is made at this stage
#

from sys import argv
import json
import os
import os.path as path
import petl
import tb_rest as tb

def check_dir(folder):
    if not path.exists(folder):
        os.mkdir(folder)

def devices_path(folder):
    return folder + "/devices.json"

def load_devices(file):
    '''
    File header:
    id;additional_info;customer_id;type;name;label;search_text;tenant_id
    '''
    tbl_devices = petl.io.csv.fromcsv(file, delimiter=';')
    tbl_devices = petl.cutout(tbl, 'customer_id', 'serch_text', 'tenant_id')
    #PETL docs:
    #https://petl.readthedocs.io/en/stable/util.html#petl.util.lookups.dictlookupone
    devices = petl.dictlookupone(tbl_devices, 'id')
    return devices

def check_devices(folder, tb_access_file = None):
    device_file = devices_path(folder)
    if path.exists(device_file):
        devices = json.load(open(device_file, 'r'))
    elif tb_access_file:
        tb_params = tb.load_access_parameters(tb_access_file)
        tb_con = tb.TbConnection(tb_params['url'], tb_params['user'], tb_params['password'])
        devices, resp = tb.get_tenant_devices(tb_con.url, tb_con.get_token(), get_credentials = True)
        json.dump(devices, open(device_file, 'w'))
    else:
        raise ValueError("File devices.json is not present and access to Thingsboard is not provided")
    return dict([(d['id']['id'], d) for d in devices])

#def create_device_tables(devices):
    '''devices is the list of structures:
    {
		"id": {
			"entityType": "DEVICE",
			"id": "e93418a0-468x-11xy-bd76-a73c738b665f"
		},
		"createdTime": 1580738590506,
		"additionalInfo": null,
		"tenantId": {
			"entityType": "TENANT",
			"id": "84966700-4687-11ea-bd76-a73c738b665f"
		},
		"customerId": {
			"entityType": "CUSTOMER",
			"id": "d1cf6e60-3e57-11e9-b50c-f987795349fe"
		},
		"name": "(Ф. 00) Smart Meter 96",
		"type": "Mercury",
		"label": null,
		"token": "DEVICE_TOKEN"
	}

    The output is a list of petl table containers with headers:
    ts,key_1,key_2
    '''

def transform(ts_kv_table, devices):
    """The table has the following structure:
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
    | entity_type | entity_id                       | key           | ts            | bool_v | str_v | long_v | dbl_v |
    +=============+=================================+===============+===============+========+=======+========+=======+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |        |       |        | -1.8  |
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |        |       | 227    |       |
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+  
    
    The output is a dictionary {device_id:table} of tables like that:
    +--------------+--------------+---------------+
    | ts           | Temperature  | WindDirection |
    +--------------+--------------+---------------+
    |1583010011665 | -1.8         |  230          |
    +--------------+--------------+---------------+
    |1583010000692 |   -2.5       | 227           |
    +--------------+--------------+---------------+
    """
    
    

    def get_ts(row):
        return int(row[3])

    def get_key(row):
        return row[2]

    def get_device_id(row):
        return row[1]

    i = 0
    n = 3
    device_tables = dict([(d_id, []) for d_id in devices])
    
    #TODO
    #https://petl.readthedocs.io/en/stable/transform.html#selecting-rows
    #https://petl.readthedocs.io/en/stable/transform.html#reshaping-tables

    #for d_id in device_tables:
    #    header = ['ts'] + find_keys(d_id, ts_kv_table)
    #    device_tables[d_id].append(header)

    #lookup (id, ts)

    #

    for row in ts_kv_table[1:]:
        #TODO
        #get device short name
        #create device table
        #add the key to the table
        #device id in TB is java uuid
        #id in postgres is postgres uuid
        #I should extract device table from postgres directly
        device_id = get_device_id(row)
        key = get_key(row)
        ts = get_ts(row)
        label = devices[device_id]['label']
        if label not in device_tables:
            device_tables[label] = [['ts', key], [ts, get_value(row)]]
        else:
            keys = set(petl.util.base.header(device_tables[label]))
            if key not in keys:
                device_tables[label] = [['ts', key], [ts, get_value(row)]]
        print(device_tables[label] )
        if i>n:
            break
        i+=1

def get_value(row):
    #IND_VAL = 4
    keys = ['bool_v', 'str_v', 'long_v', 'dbl_v']
    conv = [bool, str, int, float]
    for i,k in enumerate(keys):
        if row[k]:
            return conv[i](row[k])
    return ''

def get_ts(row):
    return int(row[3])

def transform_fields(tbl):
    ts_kv_table = petl.transform.conversions.convert(tbl, 'ts', int)
    #print(get_value(ts_kv_table[1]))
    ts_kv_table = petl.addfield(ts_kv_table, 'value', lambda row: get_value(row))
    ts_kv_table = petl.cutout(ts_kv_table, 'bool_v', 'str_v', 'long_v', 'dbl_v')
    return ts_kv_table

def lookup_and_transform(ts_kv_table):
    lkp = petl.lookup(ts_kv_table, 'entity_id')
    for id in lkp:
        #print(f"{id} : {len(lkp[id])} entries")
        lkp[id] = sorted(lkp[id], key=lambda row : get_ts(row))
        #keys = set([row[2] for row in lkp[id]])
        #header = ['ts'] + list(keys)
        tbl = [petl.header(ts_kv_table)] + lkp[id]
        tbl = petl.cutout(tbl, 'entity_type', 'entity_id')
        tbl_by_ts = petl.recast(tbl, variablefield='key', valuefield='value')
        tbl_by_ts = petl.transform.headers.sortheader(tbl_by_ts)
        tbl_by_ts = petl.transform.basics.movefield(tbl_by_ts, 'ts', 0)
        lkp[id] = tbl_by_ts
    return lkp

def load(tables_by_id, output_folder):
    for id in tables_by_id:
        tbl_device_file = path.join(output_folder, f"{id}.csv")
        if path.isfile(tbl_device_file):
            tbl_old = petl.fromcsv(tbl_device_file, delimiter = ';')
            old_header = petl.header(tbl_old)
            new_header = petl.header(tables_by_id[id])
            if old_header == new_header:
               petl.appendcsv(tables_by_id[id], source = tbl_device_file)
            else:#TODO: write to the new file
               raise ValueError(f"Incompatible headers:\n old={old_header}\n new={new_header}")
        else:       
            petl.tocsv(tables_by_id[id], tbl_device_file, delimiter = ';')


HEADER = ['entity_type', 'entity_id', 'key', 'ts', 'bool_v', 'str_v', 'long_v', 'dbl_v']

if __name__ == "__main__":
    #parse command-line arguments
    ts_file = argv[1]
    #devices_file = argv[2] #Device file is not used now
    output_folder = argv[2]
    check_dir(output_folder)
    #devices = load_devices(devices_file)
    #if len(argv)>3:
    #    devices = check_devices(output_folder, argv[3])
    #else:
    #    devices = check_devices(output_folder)
    #print(f"{len(devices)} devices is loaded")
    ts_kv_table = petl.io.csv.fromcsv(ts_file, header = HEADER, delimiter=';')
    print(f"Loaded rows data from {ts_file}")
    ts_kv_table = transform_fields(ts_kv_table)
    lkp = lookup_and_transform(ts_kv_table)
    load(lkp, output_folder)
    print(f"Data from {len(lkp)} devices saved to {output_folder}")
   