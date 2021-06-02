from os import error
from sys import stderr
import tb_rest as tb
import json
from datetime import datetime
import time
import argparse

DELAY_MS = 500


def log_error(entry, resp):
    print(f"Response code {resp.status_code} for entry: {entry}", file=stderr)


def upload(tb_url, data, start_ts, end_ts, delay=DELAY_MS):
    """data is array of json:
    [{"ts": 1616965289148, "devEui": "MOXAKON1-MR234-017", "values": {"PT": 0,...} }, {...}, ... ]
    """

    for entry in data:
        if start_ts <= tb.fromJsTimestamp(entry['ts']) < end_ts:
            device_token = entry['devEui']
            message_json = entry
            del message_json['devEui']
            resp = tb.upload_telemetry(tb_url, device_token, message_json)
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
    parser.add_argument('--delay', help="Time in milliseconds between messages.", default=DELAY_MS)
    parser.add_argument('--start', help="The time of the beginning of the target period (including). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='1970-01-01 00:00:00')
    parser.add_argument('--end', help="The time of the end of the target period (excluding). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='2999-01-01 00:00:00')
    parser.add_argument('url', help="TB host url")
    parser.add_argument('input', help="File with json data to upload.")
    return parser.parse_args()


def get_ts(timestr):
    return datetime.fromisoformat(timestr).timestamp()


def main(args):
    data = open(args.input).readlines()
    data = map(lambda x: json.loads(x)[0], data)
    upload(args.url, data, get_ts(args.start), get_ts(args.end), args.delay)


if __name__ == "__main__":
    main(get_args())
