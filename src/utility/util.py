import re
from datetime import datetime, timedelta


def print_rdd(x):
    print (x)


def extract_time(str_val):
    time_lst = re.findall(r"\[(.*?)\]", str_val)
    time_str = time_lst[0].replace('-0400', '').strip()
    return datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')


def get_start_time(logfile):
    with open(logfile, 'r') as f:
        first_line = f.readline()
        start_time = extract_time(first_line)
    return start_time


def to_time(res, start_time):
    time_res = []
    for seconds, val in res:
        curtime = (start_time + timedelta(seconds=seconds))
        time_str = datetime.strftime(curtime, '%d/%b/%Y:%H:%M:%S') + ' -0400'
        time_res.append((time_str, val))

    return time_res


def parse_request(str_val):
    resource = ''
    reply_code = ''
    reply_bytes = 0

    str_splits = str_val.split(']')

    if len(str_splits) == 2:
        second_str = str_splits[1]
        # e.g. "GET /history/apollo/ HTTP/1.0" 200 6245
        second_str = second_str.replace(u'\u201c', '"').replace(u'\u201d', '"')
        req_lst = re.findall('"([^"]*)"', second_str)
        # e.g. GET /history/apollo/ HTTP/1.0

        if len(req_lst) == 1:
            req_str = req_lst[0]
            resource = __extract_resource__(req_str)

            if resource:
                reply_code, reply_bytes = __extract_code_bytes__(second_str.replace('"' + req_str + '"', '').strip())
    return resource, reply_code, reply_bytes


def __extract_resource__(req):
    resource = ''

    if req.startswith('GET'):
        resource = req[3:]
    elif req.startswith(('POST', 'HEAD')):
        resource = req[4:]
    resource = resource.replace('HTTP/1.0', '').strip()
    return resource


def __extract_code_bytes__(reply_str):
    reply_code = ''
    reply_bytes = 0

    reply_lst = reply_str.split()

    if (len(reply_lst) == 2) and (reply_lst[1].isdigit()):
        reply_code = reply_lst[0]
        reply_bytes = int(reply_lst[1])

    return reply_code, reply_bytes
