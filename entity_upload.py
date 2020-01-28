#TODO
#Devices
#Assets
#Relations
#Attributes

import tb_rest as tb
import json as json


def upload_devices(tb_url, json_file, token):
    with open(json_file, 'r', encoding="utf8") as file:
        json_dict = json.load(file)
    for d in json_dict:
        resp_data, resp = tb.post_device(tb_url, token, d)
        if not resp_data:
            print("Error uploading device:")
            print(d)
            print(resp)


TB_ACCESS_FILE = "tb_lab11.access"

files = {'TENANT': "tenants.json",
         'ASSET': "assets.json",
         'DEVICE': "devices.json",
         'RELATION': "relations.json",
         'CUSTOMER': "customers.json",
         'ATTRIBUTE': "attributes.json",
         'DASHBOARD': "dashboards.json"}

folder = "lab_11"
def mkpath(entity_name):
    return folder + '/' + files[entity_name]

if __name__ == "__main__":
    
    params = tb.load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, tb_admin, tb_admin_password = params["url"], params["user"], params["password"], params["admin_user"], params["admin_password"]
    tenants_list = upload_tenants(tb_url, mkpath('TENANT'), token)
    
    for tenant_dir in os.walk(folder)[1]:
        try:
        #read tenant user
            params = tb.load_access_parameters(folder + '/' + '.access')
            tenant_user, tenant_pass = params['user'], params['password']
            print(f"Authorizing as {tenant_user}...")
            bearerToken = tb.get_token(tb_url, tenant_user, tenant_pass)[0]
            print("Access token obtained:")
            print(bearerToken)
            upload_customers(tb_url, mkpath('CUSTOMER'), bearerToken)
            upload_devices(tb_url, mkpath('DEVICE'), bearerToken)
            upload_assets(tb_url, mkpath('ASSETS'), bearerToken)
            upload_relations(tb_url, mkpath('RELATION'), bearerToken)
            upload_attributes(tb_url, mkpath('ATTRIBUTE'), bearerToken)
            upload_dashboards(tb_url, mkpath('DASHBOARD'), bearerToken)
        except Exception as e:
            print(f"Error at tenant {tenant_dir}")
            print(e)


