from tb_rest import *
import datetime

#TB_ACCESS_FILE = "tb_test_params.access"
TB_ACCESS_FILE = "tb.access"

def test_get_attributes():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password = [params["url"], params["user"], params["password"]]
    bearerToken, _, _ = getToken(tb_url, tb_user, tb_password)
    assetType = 'ROOM'
    assets, _ = list_tenant_assets(tb_url, bearerToken, assetType)
    entityType = 'ASSET'
    entityId = assets[0]['id']['id']
    scope = 'SERVER_SCOPE'
    keys = ['P', 'Q']
    data, resp = get_attribute_values(tb_url, bearerToken, entityType, entityId, scope, keys)
    print(resp)
    print(data)

def test_upload():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url = params["url"]
    DEVICE_TOKEN = 'SMARTMETER000048' #params["test_device_token"]
    ts = toJsTimestamp(datetime.datetime.now().timestamp())
    time.sleep(5)
    ts_2 = toJsTimestamp(datetime.datetime.now().timestamp())
    data = [{'ts': ts, 'values': {'A': 'Test On'}},
            {'ts': ts_2, 'values': {'B': 'Test Off'}}]
    resp = upload_telemetry(tb_url, DEVICE_TOKEN, json_data=data)
    print(resp)

def test_create_asset():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, ipu_customer_id, ipu_tenant_id = [params["url"], 
        params["user"], params["password"], params["ipu_customer_id"], params["ipu_tenant_id"] ]
    bearerToken, _, _ = getToken(tb_url, tb_user, tb_password)
    print(bearerToken)
    randName = "Test asset " + str(datetime.datetime.now().timestamp())
    entityJson, resp = create_asset(tb_url, bearerToken, randName, 
      ipu_tenant_id , ipu_customer_id , "Test type", info="Info")
    print(resp)
    print(entityJson)
    attributes = {"testNum":10, "testString":"ABC"}
    resp = upload_attributes(tb_url, bearerToken, entityJson["id"]["id"], entityJson["id"]["entityType"], "SERVER_SCOPE", attributes)
    print(resp)

def test_list_assets():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password = [params["url"], params["user"], params["password"]]
    bearerToken, _, _ = getToken(tb_url, tb_user, tb_password)
    assetType = 'ROOM'
    assets, resp = list_tenant_assets(tb_url, bearerToken, assetType)
    print(assets)
    print(resp)

def test_create_device():
    #params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, ipu_customer_id, ipu_tenant_id = load_default_params()
    bearerToken, _, _ = getToken(tb_url, tb_user, tb_password)
    randName = "Test device " + str(datetime.datetime.now().microsecond)
    entityJson, resp = create_device(tb_url, bearerToken, randName, 
      ipu_tenant_id , ipu_customer_id , "TEST DEVICE", info="Info")
    print(resp)
    print(entityJson)
    attributes = {"testNum":10, "testString":"ABC"}
    resp = upload_attributes(tb_url, bearerToken, entityJson["id"]["id"], entityJson["id"]["entityType"], "SERVER_SCOPE", attributes)
    print(resp)

def test_device_query():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, _, _ = load_default_params()
    bearerToken, _, _ = getToken(tb_url, tb_user, tb_password)
    parameters = {'rootId': params["test_device_query_root_id"], 
        'rootType':'DEVICE', 
        'direction':'FROM',
        'relationTypeGroup':'COMMON',
        'maxLevel':1}
    resp = device_query(tb_url, bearerToken, deviceTypes = ['TEST DEVICE'],
        parameters = parameters )
    print(resp)

def load_default_params():
    params = load_access_parameters(TB_ACCESS_FILE)
    return params["url"], params["user"], params["password"], params["ipu_customer_id"], params["ipu_tenant_id"] 

def test_delete():
    params = load_access_parameters(TB_ACCESS_FILE)
    tb_url, tb_user, tb_password, _, _ = load_default_params()

if __name__ == "__main__":
    test_upload()
    test_create_asset()
    test_list_assets()
    test_get_attributes()    
    test_create_device()
    test_device_query()