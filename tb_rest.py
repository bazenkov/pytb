# Import smtplib for the actual sending function
import urllib.request
import requests
import datetime
import time


# authorization
LOGIN_URL = '/api/auth/login'
def get_auth_url(tb_url):
    return tb_url + LOGIN_URL

def load_access_parameters(key_value_file):
    params = {}
    with open(key_value_file) as file:
        for line in file:
            name, var = line.partition("=")[::2]
            params[name.strip()] = var.strip()
    return params

def getToken(tb_url, user, passwd, print_token = False):
    url = get_auth_url(tb_url)
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}
    loginJSON = {'username': user, 'password': passwd}
    tokenAuthResp = requests.post(url, headers=headers, json=loginJSON).json()
    if 'token' in tokenAuthResp:
        bearerToken = 'Bearer: ' + tokenAuthResp['token']
        refreshToken = tokenAuthResp['refreshToken']
    else:
        bearerToken = ''
        refreshToken = ''
        print(f"ERROR {tokenAuthResp.status}: {tokenAuthResp.error}")
        print(tokenAuthResp.message)
    if print_token:
        print("Bearer token:")
        print(bearerToken)
        print("Refresh token:")
        print(refreshToken)
    return bearerToken, refreshToken, tokenAuthResp

def refresh_token(tb_url, bearerToken, refreshToken):
    REFRESH_URL = '/api/auth/token'
    url = tb_url + REFRESH_URL
    headers = {'Content-Type': 'application/json',
               'X-Authorization': bearerToken}
    json = {'refreshToken': refreshToken}
    tokenAuthResp = requests.post(url, headers=headers, json=json).json()
    bearerToken = 'Bearer: ' + tokenAuthResp['token']
    refreshToken = tokenAuthResp['refreshToken']
    return bearerToken, refreshToken, tokenAuthResp

def getKeys(tb_url, deviceId, bearerToken):
    url = tb_url + '/api/plugins/telemetry/DEVICE/' + deviceId + '/keys/timeseries'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers)
    return resp

def toJsTimestamp(pyTimestamp):
    return int(pyTimestamp * 1e3)

def fromJsTimestamp(jsTimestamp):
    return jsTimestamp / 1e3

def getTimeseries(tb_url, deviceId, bearerToken, keys, startTs, endTs, limit=100, interval=None, agg='NONE'):
    '''
    keys - an iterable like ['temperature', 'humidity']
    startTs, endTs - JavaScript integer timestamp
    '''
    params = { 'keys': ','.join(keys), 'startTs': startTs, 'endTs': endTs,
               'limit': limit, 'interval': interval, 'agg': agg }
    url = tb_url + '/api/plugins/telemetry/DEVICE/' + deviceId + '/values/timeseries'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers, params=params)
    return resp

def load_telemetry(tb_url, bearerToken, device_list, startTs, endTs):
    for device in device_list:
        resp = getTimeseries(tb_url,
            device['id'], bearerToken, device['keys'], startTs, endTs, limit=10000)
        if resp.status_code == 200:
            device['data'] = resp.json()
    return device_list, resp

def upload_telemetry(tb_url, deviceToken, json_data):
    # http(s)://host:port/api/v1/$ACCESS_TOKEN/telemetry
    url = tb_url + '/api/v1/' + deviceToken + '/telemetry'
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(url, headers=headers, json=json_data)
    return resp

def create_asset(tb_url, bearerToken, name, 
tenantId, customerId, assetType, info="" , createdTime = toJsTimestamp(datetime.datetime.now().timestamp())):
    ''' Example of json for NEW asset:
      {
      "additionalInfo": "Info",
      "createdTime": 1560345499564,
      "customerId": {
          "entityType": "CUSTOMER",
          "id": "abc123"
      },
      "name": "Test POST asset",
      "tenantId": {
          "entityType": "TENANT",
          "id": "abcdef1234"
      },
      "type": "Потребитель"
    }
    '''
    url = tb_url + '/api/asset'
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    asset_json = {
      "additionalInfo": info,
      "createdTime": createdTime,
      "customerId": {
          "entityType": "CUSTOMER",
          "id": customerId
      },
      "name": name,
      "tenantId": {
          "entityType": "TENANT",
          "id": tenantId
      },
      "type": assetType
    }
    resp = requests.post(url, headers=headers, json=asset_json)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        return [], resp

def upload_attributes(tb_url, bearerToken, entityId, entityType, scope, attributes):
    '''
    attributes - dictionary {key:value}
    scope = SERVER_SCOPE, CLIENT_SCOPE, SHARED_SCOPE
    '''
    url = tb_url + f'/api/plugins/telemetry/{entityType}/{entityId}/attributes/{scope}'
    headers = headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.post(url, headers=headers, json=attributes)
    return resp

def get_attribute_keys(tb_url, bearerToken, entity_id, entity_type, scope = None):
    url = tb_url + f'/api/plugins/telemetry/{entity_type}/{entity_id}/keys/attributes'
    if scope:
        url = url + f'/{scope}'        
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        return [], resp

ATTR_SCOPES = {'SERVER_SCOPE', 'CLIENT_SCOPE', 'SHARED_SCOPE'}

def get_attribute_values(tb_url, bearerToken, entity_id, entity_type, scope=None, keys=None):
    if scope:
        url = f'{tb_url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/attributes/{scope}'
    else:
        url = f'{tb_url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/attributes'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    if keys:
        params = {'keys':keys}
        resp = requests.get(url, headers=headers, params=params)
    else:
        resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
    else:
        data = []
    return data, resp

def list_tenant_assets(tb_url, bearerToken, assetType=None, limit = 100, textSearch = None):
    url = f'{tb_url}/api/tenant/assets'
    params = {'limit':limit}
    if assetType:
        params['type']=assetType
    if textSearch:
        params['textSearch'] = textSearch
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}  
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data'], resp 
    else:
        return [], resp

def get_tenant_devices(tb_url, bearerToken, deviceType=None, limit = 100, textSearch = None):
    url = f'{tb_url}/api/tenant/devices'
    params = {'limit':limit}
    if deviceType:
        params['type']=deviceType
    if textSearch:
        params['textSearch'] = textSearch
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}  
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data'], resp 
    else:
        return [], resp

def device_query(tb_url, bearerToken, deviceTypes=None, parameters=None, relationType=None):
    '''
    Description:
   tb_url/swagger-ui.html#!/device-controller/findByQueryUsingPOST_1

    DeviceSearchQuery {
        deviceTypes (Array[string], optional),
        parameters (RelationsSearchParameters, optional),
        relationType (string, optional)
        }
        RelationsSearchParameters {
            rootId (string),
            rootType (string) = ['TENANT', 'CUSTOMER', 'USER', 'DASHBOARD', 'ASSET', 'DEVICE', 'ALARM', 'RULE_CHAIN', 'RULE_NODE', 'ENTITY_VIEW', 'WIDGETS_BUNDLE', 'WIDGET_TYPE']stringEnum:"TENANT", "CUSTOMER", "USER", "DASHBOARD", "ASSET", "DEVICE", "ALARM", "RULE_CHAIN", "RULE_NODE", "ENTITY_VIEW", "WIDGETS_BUNDLE", "WIDGET_TYPE",
            direction (string) = ['FROM', 'TO']stringEnum:"FROM", "TO",
            relationTypeGroup (string) = ['COMMON', 'ALARM', 'DASHBOARD', 'RULE_CHAIN', 'RULE_NODE']stringEnum:"COMMON", "ALARM", "DASHBOARD", "RULE_CHAIN", "RULE_NODE",
        maxLevel (integer)
    }
    '''
    url = f'{tb_url}/api/devices'
    query = {}
    if deviceTypes:
        query['deviceTypes'] = deviceTypes
    if parameters:
        query['parameters'] = parameters
    if relationType:
        query['relationType'] = relationType
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken} 
    resp = requests.post(url, headers = headers, json = {'query':query} )
    return resp.json()

def get_devices(tb_url, jwtToken, customerId, device_type='', limit=200):
    params = {'customerId': customerId, 'limit': limit, 'type': device_type}
    url = tb_url + '/api/customer/' + customerId + '/devices'
    headers = {'Accept': 'application/json', 'X-Authorization': jwtToken}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data']
    else:
        print('Error: ' + str(resp.status_code))
        return []

def get_device_credentials(tb_url, jwtToken, device_id):
    url = tb_url + '/api/device/' + device_id + '/credentials'
    headers = {'Accept': 'application/json', 'X-Authorization': jwtToken}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    else:
        print('Error: ' + str(resp.status_code))
        return []

def create_device(tb_url, bearerToken, name, 
tenantId, customerId, deviceType, info="" , createdTime = toJsTimestamp(datetime.datetime.now().timestamp())):
    ''' Example of json for NEW device:
      {
      "additionalInfo": "Info",
      "createdTime": 1560345499564,
      "customerId": {
          "entityType": "CUSTOMER",
          "id": "abc123"
      },
      "name": "Test POST asset",
      "tenantId": {
          "entityType": "TENANT",
          "id": "abcdef12345"
      },
      "type": "Device type"
    }
    '''
    url = tb_url + '/api/device'
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    asset_json = {
      "additionalInfo": info,
      "createdTime": createdTime,
      "customerId": {
          "entityType": "CUSTOMER",
          "id": customerId
      },
      "name": name,
      "tenantId": {
          "entityType": "TENANT",
          "id": tenantId
      },
      "type": deviceType
    }
    resp = requests.post(url, headers=headers, json=asset_json)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        return [], resp
    pass

def get_tenants(tb_url, bearerToken):
    url = f"{tb_url}/api/tenants"
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    params = {'limit': 100}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data']
    else:
        print('Error: ' + str(resp.status_code))
        return []

def get_customers(tb_url, bearerToken):
    url = f"{tb_url}/api/customers"
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    params = {'limit': 100}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data']
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp

def get_relations(tb_url, bearerToken, fromId, fromType, relationType = None):
    url = f"{tb_url}/api/relations"
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    params = {'fromId': fromId, 'fromType': fromType}
    if relationType:
        params['relationType'] = relationType
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp

def list_tenant_dashboards(tb_url, bearerToken, limit = 100):
    url = f"{tb_url}/api/tenant/dashboards"
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    params = {'limit': limit}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data'], resp
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp

def get_dashboard(tb_url, bearerToken, dashboard_id):
    url = f"{tb_url}/api/dashboard/{dashboard_id}"
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp



