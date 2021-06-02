"""Assign short name to every device from TB.
The label is [type]_[number]
All TB devices are downloaded. Then grouped according to their types.
Each group is sorted according to their full names. 
Then labels are assigned and uploaded to TB

This script is not intended to provide consistent and unique labels!

It is intended for one-shot used on a fresh database.
"""

from sys import argv
import tb_rest as tb

def group_by_types(devices):
    types = set([ d['type'] for d in devices ])
    device_by_types = dict([(t, []) for t in types])
    for d in devices:
        device_by_types[d['type']].append(d)
    return device_by_types


# def sort_types(device_by_types):
#    def none_to_blank(s):
#        if s:
#            return s
#        else:
#            return "z"
#    for group in device_by_types.values():
#        group.sort(key = lambda x: x['createdTime'])

def assign_labels(device_by_types):
    for d_type in device_by_types:
        devices = device_by_types[d_type]
        non_labeled_devices = [d for d in devices if not d['label']]
        non_labeled_devices.sort(key = lambda x: x['createdTime'])
        num_labels = len(devices) - len(non_labeled_devices)
        start_num = num_labels
        for i,d in enumerate(non_labeled_devices):
            if not d['label']:
                d['label'] = d['type'] + "_" + str(start_num + i)

def upload_tb(tb_con, devices):
    for d in devices:
        _, resp = tb.post_device(tb_con.url, tb_con.get_token(), d)
        if resp.status_code != 200:
            print("Failed to post device:")
            print(d)

"""
devices is the list of structures:
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

"""

if __name__ == "__main__":
    tb_params = tb.load_access_parameters(argv[1])
    tb_con = tb.TbConnection(tb_params['url'], tb_params['user'], tb_params['password'])
    devices, resp = tb.get_tenant_devices(tb_con.url, tb_con.get_token(), get_credentials = True)
    device_by_types = group_by_types(devices)
    assign_labels(device_by_types)
    devices = [d for group in device_by_types.values() for d in group ]
    upload_tb(tb_con, devices)
    
