#TODO:
#Load tenants + 
#Load customers + 
#Load assets +
#Load relations +
#Load devices +
#Load attributes + 
#Load dashboards + 

import tb_rest as tb
import json as json
import os.path as path
import os as os
import datetime as dt

def json_format(json_str):
    json_str = json_str.replace('\'', '\"' )
    json_str = json_str.replace('None', 'null')
    json_str = json_str.replace('\": True', '\": true')
    json_str = json_str.replace('\": False', '\": false')
    return json_str

def save_entities(file, entities_list):
    with open(file, 'w', encoding='utf-8') as file:
        json_str = json_format(str(entities_list))
        file.write(json_str)

def save_tenants(tb_url, admin_user, admin_password, save_file):
    """
        Admin user is required!
    """
    bearerToken, refreshToken = tb.getToken(tb_url, admin_user, admin_password)[0:2]
    tenants_list = tb.get_tenants(tb_url, bearerToken)
    save_entities(save_file, tenants_list)
    return tenants_list

def save_customers(tb_url, save_file, tenant_admin_user=None, password=None, token=None):
    """
    Save customers that belong to this user's tenant
    """
    if not token:
        token, refreshToken = tb.getToken(tb_url, tenant_admin_user, password)[0:2]
    customers_list = tb.get_customers(tb_url, token)
    save_entities(save_file, customers_list)
    return customers_list

ASSET_LIMIT = 2000
def save_assets(tb_url, save_file, tenant_admin_user=None, password=None, token=None):
    """
    Save assets that belong to this user's tenant
    """
    if not token:
        print("Obtaining token...")
        token, refreshToken = tb.getToken(tb_url, tenant_admin_user, password)[0:2]
    print("Loading assets...")
    assets_list, resp = tb.list_tenant_assets(tb_url, token, limit = ASSET_LIMIT)
    if assets_list:
        print(f"{len(assets_list)} assets loaded.")
        save_entities(save_file, assets_list)
        print(f"Assets successfully saved to {save_file}")
    else:
        print("FAILURE: assets were not loaded")
        print(f"Response status: {resp.status_code}")
        print(resp.json())
    return assets_list
    
DEVICE_LIMIT = 2000
def save_devices(tb_url, save_file, tenant_admin_user=None, password=None, token=None):
    """
    Save devices that belong to this user's tenant
    """
    if not token:
        print("Obtaining token...")
        token, refreshToken = tb.getToken(tb_url, tenant_admin_user, password)[0:2]
    print("Loading devices...")
    devices_list, resp = tb.get_tenant_devices(tb_url, token, limit = DEVICE_LIMIT)
    if devices_list:
        print(f"{len(devices_list)} devices loaded.")
        save_entities(save_file, devices_list)
        print(f"Devices successfully saved to {save_file}")
    else:
        print("FAILURE: devices were not loaded")
        print(f"Response status: {resp.status_code}")
        print(resp.json())
    return devices_list

def save_relations(tb_url, entity_list, save_file, tenant_admin_user=None, password=None, token=None):
    if not token:
        print("Obtaining token...")
        token, refreshToken = tb.getToken(tb_url, tenant_admin_user, password)[0:2]
    print("Loading relations...")
    all_relations_list = []
    for entity in entity_list:
        relations_list, resp = tb.get_relations(tb_url, token, fromId=entity['id']['id'], fromType=entity['id']['entityType'])
        if relations_list:
            all_relations_list += relations_list
        elif resp.status_code != 200:
            print("FAILURE: relations were not loaded for entity:")
            print(f"Type {entity['id']['entityType']}, Id = {entity['id']['id']}")
            print(f"Response status: {resp.status_code}")
            print(resp.json())
    save_entities(save_file, all_relations_list)
    return all_relations_list

def save_attributes(tb_url, entity_list, save_file, token):
    print("Loading attributes...")
    all_attributes_list = []
    for entity in entity_list:
        entry = {'id': entity['id']}
        for scope in tb.ATTR_SCOPES:
            #entity_keys, resp = tb.get_attribute_keys(tb_url, token, entity['id']['id'], entity['id']['entityType'], scope = scope)
            attributes, resp = tb.get_attribute_values(tb_url, token, entity['id']['id'], entity['id']['entityType'], scope = scope)
            entry[scope] = attributes
        all_attributes_list.append(entry)
    save_entities(save_file, all_attributes_list)
    return all_attributes_list     

def save_dashboards(tb_url, save_dir, token):
    print("Loading dashboards...")
    dashboards, resp = tb.list_tenant_dashboards(tb_url, token)
    if dashboards:
        print(f"{len(dashboards)} dashboards loaded.")
    #    save_entities(save_file, dashboards)
    #    print(f"Dashboards successfully saved to {save_file}")
    else:
        print("FAILURE: dashboards were not loaded")
        print(f"Response status: {resp.status_code}")
        print(resp.json())
    db_info_list = []
    check_dir(save_dir)
    for i, db in enumerate(dashboards):
        db_info, resp = tb.get_dashboard(tb_url, token, db['id']['id'])
        if db_info:
            db_info_list.append(db_info)
        else:
            print(f"FAILURE: dashboard {db['id']['id']} was not loaded")
            print(f"Response status: {resp.status_code}")
            print(resp.json())
        save_file = save_dir + '/' + f"{str(i)}.json"
        save_entities(save_file, db_info)
    return db_info_list



files = {'TENANT': "tenants.json",
         'ASSET': "assets.json",
         'DEVICE': "devices.json",
         'RELATION': "relations.json",
         'CUSTOMER': "customers.json",
         'ATTRIBUTE': "attributes.json",
         'DASHBOARD': "dashboards"}

def check_dir(folder):
    if not path.exists(folder):
        os.mkdir(folder)

def prepare_tenant_dir(folder, tenant):
    tenant_dir = folder + "/" + tenant['name']
    check_dir(tenant_dir)
    tenant_access_file = tenant_dir + "/.access"
    tenant_params = tb.load_access_parameters(tenant_access_file)
    tenant_user, tenant_pass = tenant_params['user'], tenant_params['password']
    return tenant_dir, tenant_user, tenant_pass

def mkpath(folder, entity_name):
        return folder + '/' + files[entity_name]

TB_ACCESS_FILE = "tb.access"
#TB_ACCESS_FILE = "tb_lab11.access"
#folder = "lab_11"
folder = "main_tb"

if __name__ == "__main__":
    
    check_dir(folder)
    params = tb.load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, tb_admin, tb_admin_password = params["url"], params["user"], params["password"], params["admin_user"], params["admin_password"]
    #tenants_list = save_tenants(tb_url, tb_admin, tb_admin_password, mkpath(folder, 'TENANT'))
    tenants_list = [{'name': "ИПУ РАН"}, {'name': "Школа 29"}]
    for tenant in tenants_list:
        try:
            tenant_dir, tenant_user, tenant_pass = prepare_tenant_dir(folder, tenant)
            print(f"Authorizing as {tenant_user}...")
            bearerToken = tb.get_token(tb_url, tenant_user, tenant_pass)[0]
            print("Access token obtained:")
            print(bearerToken)
            save_customers(tb_url, mkpath(tenant_dir, 'CUSTOMER'), token = bearerToken)
            assets = save_assets(tb_url,  mkpath(tenant_dir,'ASSET'), token = bearerToken)
            devices = save_devices(tb_url,  mkpath(tenant_dir, 'DEVICE'), token = bearerToken)
            save_relations(tb_url, assets, mkpath(tenant_dir, 'RELATION'), token = bearerToken)
            entity_list = assets + devices
            save_attributes(tb_url, entity_list, mkpath(tenant_dir, 'ATTRIBUTE'), token = bearerToken)
            save_dashboards(tb_url, mkpath(tenant_dir, 'DASHBOARD'), token = bearerToken)
        except Exception as e:
            print(f"Error at tenant {tenant['name']}")
            print(e)
            
    
    
    
    
    
   