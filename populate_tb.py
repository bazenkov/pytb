from sys import argv
import time
import random
import tb_rest as tb


WAIT_SEC = 30

if __name__ == "__main__":
    tb_params = tb.load_access_parameters(argv[1])
    tb_con = tb.TbConnection(tb_params['url'], tb_params['user'], tb_params['password'])
    print(tb_con.get_token())
    #list devices
    devices, resp = tb.get_tenant_devices(tb_con.url, tb_con.get_token())
    #generate data
    for d in devices:
        d['token'] = tb.get_device_credentials(tb_con.url, tb_con.get_token(), d['id']['id'])['credentialsId']
    num = int(argv[2])
    for i in range(num):
        for d in devices:
            json_data = {'A': random.randint(0, 10), 'B':random.random()*10}
            tb.upload_telemetry(tb_con.url, d['token'], json_data)
    time.sleep(WAIT_SEC)