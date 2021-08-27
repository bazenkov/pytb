# Convert raw database dump to the dataset
# Usage:
# convert ts_kv.csv devices.csv output_folder
# or
# convert input_folder devices.csv output_folder
#
# The output csv files is named like device_name.csv
# The name consists of the device name where non-latin symbols and symbols not allowed for file naming are removed or replaced by _
# If the specified output folder already contains a file device_id.csv,
# the new data are appended at the end.
# No consistency check is made at this stage
#
# The dataset contains the following files:
# device_id_1.csv
# ...
# device_id_n.csv
# keys.csv
# devices.csv
#
# Each device file contains the timestamps and the values as follows:
# ts;temperature;humidity
# 1583010039249;18.9;NaN
# 1583010040249;19.0;65.0
#
# File devices.csv contains information about devices identity:
# id;
#
# Input is a csv files which contains the rows dumped from ts_kv table in the database:
# DEVICE;1ea47494dc14d40bd76a73c738b665f;25;1583010011665;;;;-1.8
# DEVICE;1ea4e683c20f6a0a4e57bde087d24ee;27;1583010039249;;;;18.9
#
# The header is usually absent in the file, but the column names are:
# entity_type ; entity_id ; key ; ts ; bool_v ; str_v ; long_v ; dbl_v
#
#
# Features to be done:
# - Add json config file
# - Automatic target folder creation if absent
# - Fetch device description from TB

import os
import sys
from os import path
import json
from unidecode import unidecode
import traceback
import argparse
import tb_rest as tb
import petl

# from memory_profiler import profile

FORBIDDEN_SYMBOLS = ",\"*/:<>?\\|+;=()[] "
REPLACE_SYMBOLS = "_" * len(FORBIDDEN_SYMBOLS)
TRANS_TABLE = FORBIDDEN_SYMBOLS.maketrans(FORBIDDEN_SYMBOLS, REPLACE_SYMBOLS)

TB_VERSIONS = {'old', '2.5.4', '3.2'}
HEADER = ('entity_type', 'entity_id', 'key', 'ts', 'bool_v', 'str_v', 'long_v', 'dbl_v')
HEADER_32 = ('entity_id', 'key', 'ts', 'bool_v', 'str_v', 'long_v', 'dbl_v', 'json_v')


#def valid_name(device_name):
#    return unidecode(device_name.translate(TRANS_TABLE)).replace("__", "_").replace("___", "_").strip("_")


def check_dir(folder):
    if not path.exists(folder):
        os.mkdir(folder)


#def devices_path(folder):
#    return folder + "/devices.json"


def load_devices(file, output_folder=None):
    """
    File header:
    id;additional_info;customer_id;type;name;label;search_text;tenant_id

    Output:
        dictionary like {'id_1': {name:'Device 1'}, ...}

    """
    tbl_devices = petl.io.csv.fromcsv(file, delimiter=';', encoding='utf-8')
    #tbl_devices = petl.cutout(tbl_devices, 'customer_id', 'search_text', 'tenant_id')
    # PETL docs:
    # https://petl.readthedocs.io/en/stable/util.html#petl.util.lookups.dictlookupone
    devices = petl.dictlookupone(tbl_devices, 'id')
    if output_folder:
        file = path.join(output_folder, "devices.csv")
        petl.io.tocsv(tbl_devices, file, delimiter=";", encoding="utf-8")
    return devices


IND_TS = 2
IND_VAL = IND_TS + 1


def get_value(row):
    keys = ['bool_v', 'str_v', 'long_v', 'dbl_v']
    conv = [bool, str, int, float]
    for i, k in enumerate(keys):
        if row[k]:
            return conv[i](row[k])
    return ''


def get_ts(row):
    return int(row[IND_TS])


def transform_fields_old(tbl):
    """The input is a dump of ts_kv table for TB version <= 2.5.4:
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
    | entity_type | entity_id                       | key           | ts            | bool_v | str_v | long_v | dbl_v |
    +=============+=================================+===============+===============+========+=======+========+=======+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |        |       |        | -1.8  |
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |        |       | 227    |       |
    +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+  
    
    The output:
    +-------------+---------------------------------+---------------+---------------+--------+
    | entity_type | entity_id                       | key           | ts            | value  |
    +=============+=================================+===============+===============+========+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |  -1.8  |
    +-------------+---------------------------------+---------------+---------------+--------+
    | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |   227  |
    +-------------+---------------------------------+---------------+---------------+--------+
    
    """
    ts_kv_table = petl.transform.conversions.convert(tbl, 'ts', int)
    ts_kv_table = petl.addfield(ts_kv_table, 'value', lambda row: get_value(row))
    ts_kv_table = petl.cutout(ts_kv_table, 'bool_v', 'str_v', 'long_v', 'dbl_v')
    return ts_kv_table


def transform_fields_254(tbl, ts_kv_dict):
    """The input is a dump of ts_kv table for TB version 2.5.4:
    +----------------------------------+---------------+---------------+--------+-------+--------+-------+
    |  entity_id                       | key           | ts            | bool_v | str_v | long_v | dbl_v |
    +==================================+===============+===============+========+=======+========+=======+
    |  1ea47494dc14d40bd76a73c738b665f | 25   | 1583010011665 |        |       |        | -1.8  |
    +----------------------------------+---------------+---------------+--------+-------+--------+-------+
    |  1ea47494dc14d40bd76a73c738b665f | 36 | 1583010000692 |        |       | 227    |       |
    +----------------------------------+---------------+---------------+--------+-------+--------+-------+  
    
    The output:
    +---------------------------------+---------------+---------------+--------+
    | entity_id                       | key           | ts            | value  |
    +=================================+===============+===============+========+
    | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |  -1.8  |
    +---------------------------------+---------------+---------------+--------+
    | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |   227  |
    +---------------------------------+---------------+---------------+--------+

    ts_kv_dict is a dict like {25:'Temperature', 36:'WindDirection'}
    """
    ts_kv_table = petl.transform.conversions.convert(tbl,
                                                     {'ts': int,
                                                      'key': lambda k: ts_kv_dict[k]})
    ts_kv_table = petl.addfield(ts_kv_table, 'value', lambda row: get_value(row))
    ts_kv_table = petl.cutout(ts_kv_table, 'bool_v', 'str_v', 'long_v', 'dbl_v', 'json_v')
    return ts_kv_table


def transform_fields_32(tbl, ts_kv_dict):
    return transform_fields_254(tbl, ts_kv_dict)


#KEYS_TO_REMOVE = {"remove": {'error', 'Breaker', 'deltaP1', 'deltaP2', 'deltaP3', 'deltaQ1', 'deltaQ2', 'deltaQ3'}}

def sort_table(tbl):
    new_tbl = petl.transform.headers.sortheader(tbl)
    new_tbl = petl.transform.basics.movefield(new_tbl, 'ts', 0)
    return petl.sort(new_tbl, 'ts')

def lookup_and_transform(ts_kv_table, keys):
    """The table has the following structure:
    +---------------------------------+---------------+---------------+--------+
    | entity_id                       | key           | ts            | value  |
    +=================================+===============+===============+========+
    | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |  -1.8  |
    +---------------------------------+---------------+---------------+--------+
    | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |   227  |
    +---------------------------------+---------------+---------------+--------+
    
    The output is a dictionary {device_id:table} of tables like that:
    +--------------+--------------+---------------+
    | ts           | Temperature  | WindDirection |
    +--------------+--------------+---------------+
    |1583010011665 | -1.8         |  230          |
    +--------------+--------------+---------------+
    |1583010000692 |   -2.5       | 227           |
    +--------------+--------------+---------------+
    """

    lkp = petl.lookup(ts_kv_table, 'entity_id', value=('key', 'ts', 'value'))
    for entity_id in lkp:
        tbl = [('key', 'ts', 'value')] + lkp[entity_id]
        tbl = petl.recast(tbl, variablefield='key', valuefield='value')
        if keys:
            cut_keys = set(keys['remove']) & set(petl.fieldnames(tbl))
            tbl = petl.cutout(tbl, *cut_keys)
    #    tbl = petl.transform.headers.sortheader(tbl)
    #    tbl = petl.transform.basics.movefield(tbl, 'ts', 0)
    #    lkp[entity_id] = petl.sort(tbl, 'ts')
        lkp[entity_id] = sort_table(tbl)
    return lkp


def load(tables_by_id, output_folder, devices):
    for device_id in tables_by_id:
        # name = valid_name(devices[device_id]['name'])
        name = devices[device_id]['data_name']
        tbl_device_file = path.join(output_folder, f"{name}.csv")
        if path.isfile(tbl_device_file):
            tbl_old = petl.fromcsv(tbl_device_file, delimiter=';')
            old_header = petl.header(tbl_old)
            new_header = petl.header(tables_by_id[device_id])
            if old_header == new_header:
                petl.appendcsv(tables_by_id[device_id], source=tbl_device_file, delimiter=';')
            else:
                tbl_new = petl.cat(tbl_old, tables_by_id[device_id])
                tbl_new = sort_table(tbl_new)
                # tbl_new = petl.sortheader(tbl_new)
                file_new = tbl_device_file+"_new.csv"
                petl.tocsv(tbl_new, file_new, delimiter=';')
                os.remove(tbl_device_file)
                os.renames(file_new, tbl_device_file)
                #raise ValueError(f"Incompatible headers:\n old={old_header}\n new={new_header}")
        else:
            petl.tocsv(tables_by_id[device_id], tbl_device_file, delimiter=';')


def dict_intersect(d_1, d_2):
    for k in set(d_1.keys()):
        if k not in d_2:
            del d_1[k]
    return d_1


def convert_file(ts_file, output_folder, fun_transform, devices, kvdict=None, keys=None, header=HEADER_32):
    ts_kv_table = petl.io.csv.fromcsv(ts_file, header=header, delimiter=';')
    print(f"Loaded {len(ts_kv_table)} rows.")
    print("Transforming fields...")
    if kvdict:
        ts_kv_table = fun_transform(ts_kv_table, kvdict)
    else:
        ts_kv_table = fun_transform(ts_kv_table)
    lkp = lookup_and_transform(ts_kv_table, keys)
    # include only devices which is present in device table
    lkp = dict_intersect(lkp, devices)
    load(lkp, output_folder, devices)
    print(f"Success. Data from {ts_file} was saved to {output_folder}")


def is_ts(name):
    return name[:5] == "ts_kv"


converter = {'old': transform_fields_old,
             '2.5.4': transform_fields_254,
             '3.2': transform_fields_32}


def convert_folder(input_folder, output_folder, fun_converter, devices, kvdict=None, keys=None):
    files = [f for f in os.scandir(input_folder)]
    files = sorted(files, key=lambda x: os.stat(x).st_mtime)
    for f in files:
        if is_ts(f.name):
            try:
                print(f"Processing {f.path} ...", end=' ')
                convert_file(f.path, output_folder, fun_converter, devices, kvdict, keys)
                print("Success")
            except Exception as e:
                print("ERROR")
                traceback.print_exc(file=sys.stdout)
                print(e)
        else:
            print(f"File {f.name} skipped")


def get_args():
    parser = argparse.ArgumentParser(
        description="Converts the raw dump of ts_kv table to the separate csv files for each device.")
    parser.add_argument('-v',
                        help="Version of Thingsboard. Possible values are 'old', '2.5.4' or '3.2'.",
                        choices=TB_VERSIONS,
                        default="3.2")
    parser.add_argument('--kvdict',
                        help="CSV file where each line is 'key;key_id'. Needed for Thingsboard version 2.5.4 and 3.2. \
                        If not specified, looking for ts_kv_dictionary.csv file in the input folder",)
    parser.add_argument('--keys', help="JSON file with keys to be removed from the dataset")
    parser.add_argument('--devices', help="CSV file copied from device table.")
    parser.add_argument('input', help="file or folder to convert")
    parser.add_argument('output_folder', help="the directory where the dataset will be converted")
    return parser.parse_args()


def load_kvdict_csv(file):
    tbl_kvdict = petl.io.csv.fromcsv(file, delimiter=';', encoding='utf-8', header=['key', 'key_id'])
    return petl.lookupone(tbl_kvdict, 'key_id', value='key')


def main(args):

    def load_kvdict():
        DEFAULT_KVDICT_FILE = "ts_kv_dictionary.csv"
        if args.v in {'2.5.4', '3.2'}:
            if not args.kvdict:
                kvdict_path = path.join(path.dirname(args.input), DEFAULT_KVDICT_FILE)
                return load_kvdict_csv(kvdict_path)
                # raise ValueError(f"Version={args.v}, but the keys dictionary file is not specified")
            else:
                return load_kvdict_csv(args.kvdict)
        else:
            return None

    def load_keys():
        if not args.keys:
            return None
        return json.load(open(args.keys))

    check_dir(args.output_folder)
    devices = load_devices(args.devices, args.output_folder)
    print(f"Loaded {len(devices)} from {args.devices}")
    kvdict = load_kvdict()
    keys = load_keys()

    if path.isdir(args.input):
        convert_folder(args.input, args.output_folder, converter[args.v], devices, kvdict, keys)
    else:
        convert_file(args.input, args.output_folder, converter[args.v], devices, kvdict, keys)


if __name__ == "__main__":
    main(get_args())

# @profile
# def convert_file_old(ts_file, devices, output_folder):
#     ts_kv_table = petl.io.csv.fromcsv(ts_file, header=HEADER, delimiter=';')
#     print(f"Loaded {len(ts_kv_table)} rows.")
#     print("Transforming fields...")
#     ts_kv_table = transform_fields_old(ts_kv_table)
#     lkp = lookup_and_transform(ts_kv_table)
#     load(lkp, output_folder, devices)
#     print(f"Success. Data from {len(lkp)} devices saved to {output_folder}")


# def convert_file_254(ts_file, devices, output_folder, keys=None):
#     ts_kv_table = petl.io.csv.fromcsv(ts_file, header=HEADER, delimiter=';')
#     print(f"Loaded {len(ts_kv_table)} rows.")
#     print("Transforming fields...")
#     ts_kv_table = transform_fields_254(ts_kv_table, keys)
#     lkp = lookup_and_transform(ts_kv_table)
#     load(lkp, output_folder, devices)
#     print(f"Success. Data from {len(lkp)} devices saved to {output_folder}")



#def check_devices(folder, tb_access_file=None):
#    device_file = devices_path(folder)
#    if path.exists(device_file):
#        devices = json.load(open(device_file, 'r'))
#    elif tb_access_file:
#        tb_params = tb.load_access_parameters(tb_access_file)
#        tb_con = tb.TbConnection(tb_params['url'], tb_params['user'], tb_params['password'])
#        devices, resp = tb.get_tenant_devices(tb_con.url, tb_con.get_token(), get_credentials=True)
#        json.dump(devices, open(device_file, 'w'))
#    else:
#        raise ValueError("File devices.json is not present and access to Thingsboard is not provided")
#    return dict([(d['id']['id'], d) for d in devices])

    # def create_device_tables(devices):
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


# def transform(ts_kv_table, devices):
#     """The table has the following structure:
#     +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
#     | entity_type | entity_id                       | key           | ts            | bool_v | str_v | long_v | dbl_v |
#     +=============+=================================+===============+===============+========+=======+========+=======+
#     | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | Temperature   | 1583010011665 |        |       |        | -1.8  |
#     +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+
#     | DEVICE      | 1ea47494dc14d40bd76a73c738b665f | WindDirection | 1583010000692 |        |       | 227    |       |
#     +-------------+---------------------------------+---------------+---------------+--------+-------+--------+-------+

#     The output is a dictionary {device_id:table} of tables like that:
#     +--------------+--------------+---------------+
#     | ts           | Temperature  | WindDirection |
#     +--------------+--------------+---------------+
#     |1583010011665 | -1.8         |  230          |
#     +--------------+--------------+---------------+
#     |1583010000692 |   -2.5       | 227           |
#     +--------------+--------------+---------------+
#     """


#     def get_ts(row):
#         return int(row[3])

#     def get_key(row):
#         return row[2]

#     def get_device_id(row):
#         return row[1]

#     i = 0
#     n = 3
#     device_tables = dict([(d_id, []) for d_id in devices])

#     #TODO
#     #https://petl.readthedocs.io/en/stable/transform.html#selecting-rows
#     #https://petl.readthedocs.io/en/stable/transform.html#reshaping-tables

#     #for d_id in device_tables:
#     #    header = ['ts'] + find_keys(d_id, ts_kv_table)
#     #    device_tables[d_id].append(header)

#     #lookup (id, ts)

#     #

#     for row in ts_kv_table[1:]:
#         #TODO
#         #get device short name
#         #create device table
#         #add the key to the table
#         #device id in TB is java uuid
#         #id in postgres is postgres uuid
#         #I should extract device table from postgres directly
#         device_id = get_device_id(row)
#         key = get_key(row)
#         ts = get_ts(row)
#         label = devices[device_id]['label']
#         if label not in device_tables:
#             device_tables[label] = [['ts', key], [ts, get_value(row)]]
#         else:
#             keys = set(petl.util.base.header(device_tables[label]))
#             if key not in keys:
#                 device_tables[label] = [['ts', key], [ts, get_value(row)]]
#         print(device_tables[label] )
#         if i>n:
#             break
#         i+=1
