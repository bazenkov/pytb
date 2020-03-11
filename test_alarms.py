import sys
import datetime as dt
import random as rand
import time
import tb_rest as tb


if __name__ == "__main__":
    access_file = sys.argv[1]
    delay_sec = int(sys.argv[2])/1000
    if len(sys.argv) > 3:
        num = int(sys.argv[3])
    else:
        num = float("Infinity")
    
    params = tb.load_access_parameters(access_file)
    tb_url, tb_user, tb_password, tb_admin, tb_admin_password = params["url"], params["user"],\
                params["password"], params["admin_user"], params["admin_password"]
    device_token = "DHT11_DEMO_TOKEN"
    
    i = 0
    while i < num:
        ts = tb.toJsTimestamp(dt.datetime.now().timestamp())
        temp_val = 20.0 + 2*rand.random() 
        data = [{'ts': ts, 'values': {'temp': temp_val}}]
        resp = tb.upload_telemetry(tb_url, device_token, json_data=data)
        time.sleep(delay_sec)
        i += 1
