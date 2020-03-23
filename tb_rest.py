# Import smtplib for the actual sending function
import urllib.request
import requests
import datetime as dt
import time
import os.path as path
import os as os


# authorization
LOGIN_URL = '/api/auth/login'
def get_auth_url(tb_url):
    return tb_url + LOGIN_URL

def request_success(resp):
    return resp.status_code == 200

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
    resp = requests.post(url, headers=headers, json=loginJSON)
    tokenAuthResp = resp.json()
    if 'token' in tokenAuthResp:
        bearerToken = 'Bearer: ' + tokenAuthResp['token']
        refreshToken = tokenAuthResp['refreshToken']
    else:
        bearerToken = ''
        refreshToken = ''
        print(f"ERROR {tokenAuthResp['status']}: {tokenAuthResp['error']}")
        print(tokenAuthResp.message)
    if print_token:
        print("Bearer token:")
        print(bearerToken)
        print("Refresh token:")
        print(refreshToken)
    return bearerToken, refreshToken, resp

DEFAULT_TOKEN_FILE = "token.access"
TOKEN_EXPIRES = dt.timedelta(minutes=10)

def save_params(params_file, params):
    with open(params_file, 'w') as file:
        for k in params.keys():
            file.write(f"{k}={params[k]}\n")

def _get_token(tb_url, tb_user, tb_password, token_file = DEFAULT_TOKEN_FILE):
    '''
    The token file contains key-value pairs:
    token = TOKEN STRING
    expires = DATETIME STRING

    If the token file is specified then the token is loaded from it.
    If the expiration time is reached, the function gets the new token from thingsboard server and saves it to the token file.
    '''
    #load from token file, if it exists
    #if path.exists(token_file):
    #    params = load_access_parameters(token_file)
    #    token = params['token']
    #    expire_time = dt.datetime.fromisoformat(params['expires'])
    #    if is_expired(expire_time):
    #        token, expire_time = new_token(tb_url, tb_user, tb_password)
    #else:
    token, expire_time = new_token(tb_url, tb_user, tb_password)
    return token, expire_time

def new_token(tb_url, tb_user, tb_password, token_file = DEFAULT_TOKEN_FILE):
    token = getToken(tb_url, tb_user, tb_password)[0]
    expire_time = dt.datetime.now() + TOKEN_EXPIRES 
    params = {'token': token, 'expires':expire_time }
    save_params(token_file, params)
    return token, expire_time

def is_expired(expire_time):
    return expire_time < dt.datetime.now()

def refresh_token(tb_url, bearerToken, refreshToken):
    REFRESH_URL = '/api/auth/token'
    url = tb_url + REFRESH_URL
    headers = {'Content-Type': 'application/json',
               'X-Authorization': bearerToken}
    json = {'refreshToken': refreshToken}
    tokenAuthResp = requests.post(url, headers=headers, json=json)
    bearerToken = 'Bearer: ' + tokenAuthResp.json()['token']
    refreshToken = tokenAuthResp.json()['refreshToken']
    return bearerToken, refreshToken, tokenAuthResp

def getKeys(tb_url, deviceId, bearerToken):
    url = tb_url + '/api/plugins/telemetry/DEVICE/' + deviceId + '/keys/timeseries'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers)
    return resp

def toJsTimestamp(pyTimestamp):
    return int(pyTimestamp * 1e3)

def fromJsTimestamp(jsTimestamp):
    return round( int(jsTimestamp) / 1e3)

SEC_IN_DAY = 60*60*24
SEC_IN_WEEK = SEC_IN_DAY*7
SEC_IN_MONTH = SEC_IN_DAY*30

def get_timeseries(tb_url, deviceId, bearerToken, keys, startTs, endTs, limit=SEC_IN_DAY, interval=None, agg='NONE'):
    '''
    keys - an iterable like ['temperature', 'humidity']
    startTs, endTs - JavaScript integer timestamp
                    JavaScript_Ts = Python_Ts * 1000
    '''
    params = { 'keys': ','.join(keys), 'startTs': startTs, 'endTs': endTs,
               'limit': limit, 'interval': interval, 'agg': agg }
    url = tb_url + '/api/plugins/telemetry/DEVICE/' + deviceId + '/values/timeseries'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers, params=params)
    return resp

def get_telemetry(tb_url, deviceId, bearerToken, keys, start_time, end_time, limit=100, interval=None, agg='NONE'):
    '''
    keys - an iterable like ['temperature', 'humidity']
    start_time, end_time - Python datetime objects
    '''
    start_ts = toJsTimestamp(start_time.timestamp())
    end_ts = toJsTimestamp(end_time.timestamp())
    params = { 'keys': ','.join(keys), 'startTs': start_ts, 'endTs': end_ts,
               'limit': limit, 'interval': interval, 'agg': agg }
    url = tb_url + '/api/plugins/telemetry/DEVICE/' + deviceId + '/values/timeseries'
    headers = {'Accept': 'application/json', 'X-Authorization': bearerToken}
    resp = requests.get(url, headers=headers, params=params)
    return resp



def load_telemetry(tb_url, bearerToken, device_list, startTs, endTs):
    for device in device_list:
        resp = get_timeseries(tb_url,
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
tenantId, customerId, assetType, info="" , createdTime = toJsTimestamp(dt.datetime.now().timestamp())):
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

def get_tenant_devices(tb_url, bearerToken, deviceType=None, limit = 2000, textSearch = None):
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
tenantId, customerId, deviceType, info="" , createdTime = toJsTimestamp(dt.datetime.now().timestamp())):
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

def post_device(tb_url, bearerToken, device_json, create_new = False):
    url = tb_url + '/api/device'
    headers = {'Content-Type': 'application/json', 'X-Authorization': bearerToken}
    if create_new:
        created_json = create_device(tb_url, bearerToken,
            device_json['name'], 
            device_json['tenantId']['id'],
            device_json['customerId']['id'],
            device_json['type'],
            device_json['additionalInfo'])
        device_json['id'] = created_json['id']
    resp = requests.post(url, headers = headers, json = device_json)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        return [], resp



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
    headers = _x_auth_headers(bearerToken)
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp

def list_tenant_rulechains(tb_url, bearerToken, limit = 100):
    url = f"{tb_url}/api/ruleChains"
    headers = _x_auth_headers(bearerToken)
    params = {'limit': limit}
    return _get_list(url, headers, params)

def get_rulechain(tb_url, bearerToken, chain_id):
    #url = f"{tb_url}/api/rulechain/{chain_id}"
    url = f"{tb_url}/api/rulechain/"
    headers = _x_auth_headers(bearerToken)
    params = {'ruleChainId': chain_id}
    return _get_entity(url, headers, params)


''' Hidden functions'''
def _get_list(url, headers, params):
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json()['data'], resp
    else:
        print('Error: ' + str(resp.status_code))
        return [], resp

def _get_entity(url, headers, params = None):
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json(), resp
    else:
        print(f'Error at {url}: ' + str(resp.status_code))
        return [], resp

def _x_auth_headers(token):
    return {'Content-Type': 'application/json', 'X-Authorization': token}
