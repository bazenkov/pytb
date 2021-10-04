from os import error
from sys import stderr
import json
from datetime import datetime
import time
import argparse
import requests
import gzip as gz

DELAY_MS = 500


def log_error(entry, resp):
    print(f"Response code {resp.status_code} for entry: {entry}", file=stderr)


def upload_telemetry(tb_url, deviceToken, json_data):
    # http(s)://host:port/api/v1/$ACCESS_TOKEN/telemetry
    url = tb_url + '/api/v1/' + deviceToken + '/telemetry'
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(url, headers=headers, json=json_data)
    return resp


def from_js_timestamp(js_timestamp):
    return round(int(js_timestamp) / 1e3)


def upload(tb_url, data, start_ts, end_ts, delay=DELAY_MS):
    """data is json array:
    [{"ts": 1616965289148, "devEui": "MOXAKON1-MR234-017", "values": {"PT": 0,...} }, {...}, ... ]
    """

    for entry in data:
        if start_ts <= from_js_timestamp(entry['ts']) < end_ts:
            device_token = entry['devEui']
            message_json = entry
            del message_json['devEui']
            resp = upload_telemetry(tb_url, device_token, message_json)
            if resp.status_code != 200:
                log_error(entry, resp)
            time.sleep(delay / DELAY_MS)


# TEST_DEVICES = {'MERCURY-0':'aceb69f0-a599-11ea-9c42-6b68c14fd0ab',
# 'MERCURY-2':'cdeb3840-a59b-11ea-9c42-6b68c14fd0ab'} def test_upload(tb_url, data, devices=TEST_DEVICES): '''Test
# that every entry in the data is present in TB devices - dict {'device_token':'device_id'} ''' for d in devices:
# ts_data = tb.ge


def get_args():
    parser = argparse.ArgumentParser(
        description="Uploads collector arxiv to Thingsboard. \
                     The data flow is being balanced such that TB is not overloaded.")
    parser.add_argument('--format', help="expand or pack", default="pack")
    parser.add_argument('--delay', help="Time in milliseconds between messages.", default=DELAY_MS)
    parser.add_argument('--start', help="The time of the beginning of the target period (including). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='1970-01-02 12:00:00')
    parser.add_argument('--end', help="The time of the end of the target period (excluding). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='2999-01-01 00:00:00')
    parser.add_argument('url', help="TB host url")
    parser.add_argument('input', help="File with json data to upload.")
    return parser.parse_args()


def get_ts(timestr):
    return datetime.fromisoformat(timestr).timestamp()


def is_gzip(path):
    return path[-2:] == "gz"


def open_gz(path):
    return gz.open(path, 'rt') if is_gzip(path) else open(path)


def make_json(path):
    json_str = ""
    bracket_found = False
    with open_gz(path) as f:
        for line in f.readlines():
            if line.rstrip('\n') == "],":
                json_str += ","
                bracket_found = True
            else:
                if bracket_found:
                    # skip current "[\n" line and proceed to the next
                    bracket_found = False
                else:
                    json_str += line.rstrip('\n').lstrip('\t')
    if bracket_found:
        # if the last line of the file was like "]," then json str is like "[{...},...,{},"
        json_str = json_str[:-1] + "]"
    return json_str


def main(args):
    def upload_expand():
        data = json.loads(make_json(args.input))
        print(f"Uploading data from {args.start} to {args.end}...")
        upload(args.url, data, get_ts(args.start), get_ts(args.end), args.delay)

    def upload_pack():
        with open_gz(args.input) as f:
            print(f"Uploading data from {args.start} to {args.end}...")
            for line in f.readlines():
                if line[-1] == ",":
                    data = json.loads(line[:-2])
                else:
                    data = json.loads(line)
                upload(args.url, data, get_ts(args.start), get_ts(args.end), args.delay)
    # data = open(args.input).readlines()
    # data = json.load(open(args.input))
    # data = map(lambda x: json.loads(x)[0], data)
    print(f"Reading {args.input}...")
    if args.format == "expand":
        upload_expand()
    else:
        upload_pack()
    print(f"Completed")


if __name__ == "__main__":
    main(get_args())
