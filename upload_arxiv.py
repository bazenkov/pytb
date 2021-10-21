from os import error
import os
from sys import stderr
from os import path
import json
import datetime as dt
from datetime import datetime
import time
import argparse
import requests
import gzip as gz

DELAY_MS = 1000


def log_error(entry, resp):
    print(f"ERROR at {datetime.now()}. Response code {resp.status_code}: {resp.text}", file=stderr)


def upload_telemetry(tb_url, deviceToken, json_data):
    # http(s)://host:port/api/v1/$ACCESS_TOKEN/telemetry
    url = tb_url + '/api/v1/' + deviceToken + '/telemetry'
    headers = {'Content-Type': 'application/json'}
    resp = requests.post(url, headers=headers, json=json_data)
    return resp


def from_js_timestamp(js_timestamp):
    return round(int(js_timestamp) / 1e3)


MAX_TRY = 5


def upload(tb_url, data, starttime, endtime, delay):
    """data is json array:
    [{"ts": 1616965289148, "devEui": "MOXAKON1-MR234-017", "values": {"PT": 0,...} }, {...}, ... ]
    """

    for entry in data:
        if starttime.timestamp() <= from_js_timestamp(entry['ts']) < endtime.timestamp():
            device_token = entry['devEui']
            message_json = entry
            del message_json['devEui']
            stop = False
            try_count = 0
            while not stop and try_count < MAX_TRY:
                resp = upload_telemetry(tb_url, device_token, message_json)
                if resp.status_code == 200:
                    stop = True
                else:
                    log_error(entry, resp)
                    try_count += 1
                    time.sleep(delay / DELAY_MS)

            time.sleep(delay / DELAY_MS)


def get_args():
    parser = argparse.ArgumentParser(
        description="Uploads collector arxiv to Thingsboard. \
                     The data flow is being balanced such that TB is not overloaded.")
    parser.add_argument('--format', help="expand or pack", default="pack")
    parser.add_argument('--delay', help="Time in milliseconds between messages.", default=DELAY_MS, type=int)
    parser.add_argument('--start', help="The time of the beginning of the target period (including). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='1970-01-02 12:00:00',
                        type=datetime.fromisoformat)
    parser.add_argument('--end', help="The time of the end of the target period (excluding). \
                        String in ISO format as \"2021-01-25 09:01:00\" ", default='2999-01-01 00:00:00',
                        type=datetime.fromisoformat)
    parser.add_argument("--matchdate", help="Only files that match YYYYmmdd pattern with the date belonging to  \
                        the range given by --start and --end arguments. Used only input is a directory.",
                        action="store_true")
    parser.add_argument('url', help="TB host url")
    parser.add_argument('input', help="File with json data to upload.")
    return parser.parse_args()


def get_ts(dt_obj):
    return dt_obj.timestamp()


def is_gzip(filename):
    return filename[-2:] == "gz"


def open_gz(filename):
    return gz.open(filename, 'rt') if is_gzip(filename) else open(filename)


def make_json(filename):
    json_str = ""
    bracket_found = False
    with open_gz(filename) as f:
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


def upload_file(filename, url, fileformat, starttime, endtime, delay):
    def upload_expand():
        data = json.loads(make_json(filename))
        print(f"Uploading data from {starttime} to {endtime}...")
        upload(url, data, starttime, endtime, delay)

    def upload_pack():
        with open_gz(filename) as f:
            print(f"Uploading data from {starttime} to {endtime}...")
            for line in f.readlines():
                if line[-2] == ",":
                    data = json.loads(line[:-2])
                else:
                    data = json.loads(line)
                upload(url, data, starttime, endtime, delay)

    print(f"Reading {filename}...")
    if fileformat == "expand":
        upload_expand()
    else:
        upload_pack()
    print(f"Completed")


def is_log(filename):
    return filename[-3:] == "log" or filename[-6:] == "log.gz"


TZ_MOSCOW = dt.timezone(dt.timedelta(hours=3), "UTC+03:00")


def match_date(filename, starttime, endtime):
    """ Match the filename to the pattern YYYYmmdd*
    Return True if the date YYYYmmdd in the filename is between starrtime and endtime, ends included.

    >>> match_date("20210927-anytext.extension", datetime(2021,9,25), datetime(2021,9,27))
    True
    >>> match_date("Anytext.extension", datetime(2021,9,25), datetime(2021,9,27))
    False
    >>> match_date("20210927-anytext.extension", datetime(2021,9,24), datetime(2021,9,26))
    False
    """
    def get_date(dtobj):
        return datetime(dtobj.year, dtobj.month, dtobj.day, tzinfo=dtobj.tzinfo)

    if len(filename) < 8:
        return False
    else:
        try:
            fdate = datetime.strptime(filename[0:8], "%Y%m%d")
            fdate = fdate.astimezone(TZ_MOSCOW)
        except ValueError as e:
            return False
        return get_date(starttime) <= fdate <= get_date(endtime)+dt.timedelta(days=1)


def main(args):
    def list_files():
        f_list = [f.path for f in os.scandir(args.input) if is_log(f.name)]
        if args.matchdate:
            f_list = [f for f in f_list if match_date(path.basename(f), args.start, args.end)]
        return sorted(f_list, key=lambda x: os.stat(x).st_mtime)

    if path.isdir(args.input):
        print(f"The input directory is {args.input}")
        files = list_files()
        nl = '\n'
        print(f"Files selected for upload:\n {nl.join(files)}")
        for fname in files:
            upload_file(fname, args.url, args.format, args.start, args.end, args.delay)
    else:
        upload_file(args.input,
                    args.url,
                    args.format,
                    args.start,
                    args.end,
                    args.delay)


if __name__ == "__main__":
    main(get_args())
