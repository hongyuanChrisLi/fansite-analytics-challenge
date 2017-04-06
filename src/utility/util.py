import re
from datetime import datetime, timedelta


def print_rdd(x):
    """
    used by spark action foreach
    """
    print (x)


def get_host(str_val):
    """
    Extract host/IP from string
    :param str_val: one line of the log
    :return: string
    """
    host = ''
    str_lst = str_val.split('- -')
    if len(str_lst) == 2:
        host = str_lst[0].rstrip()
    return host


def get_offset_seconds(str_val, start_time):
    """
    Calculate offset seconds based on start_time
    :param str_val: one line of the log
    :param start_time: datetime, extracted from the first line of the log file
    :return: integer
    """
    curtime = __extract_time__(str_val)
    return int((curtime - start_time).total_seconds())


def get_start_time(logfile):
    """
    get datetime from the first line of the log file
    :param logfile: filename
    :return: datetime
    """
    with open(logfile, 'r') as f:
        first_line = f.readline()
        start_time = __extract_time__(first_line)
    return start_time


def __extract_time__(str_val):
    """
    private method
    
    extract time string and covert it to datetime
    :param str_val: one line of the log
    :return: datetime
    """
    time_lst = re.findall(r"\[(.*?)\]", str_val)
    time_str = time_lst[0].replace('-0400', '').strip()
    return datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')


def to_time(res, start_time):
    """
    map offset seconds back to timestamp string for each tuple of a list
    :param res: a list of tuples
    :param start_time: datetime, extracted from the first line of the log file
    :return: 
    """
    time_res = []
    for seconds, val in res:
        curtime = (start_time + timedelta(seconds=seconds))
        time_str = datetime.strftime(curtime, '%d/%b/%Y:%H:%M:%S') + ' -0400'
        time_res.append((time_str, val))

    return time_res


def parse_request(str_val):
    """
    parse one line of the log 
    extract resource name, reply code and reply bytes
    :param str_val: one line of the log
    :return: a tuple (resource, reply_code, reply_bytes)
    """
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
    """
    private method
    
    extract resource name from request string

    :param req: "request" part of one line in log file
    :return: string
    """
    resource = ''

    if req.startswith('GET'):
        resource = req[3:]
    elif req.startswith(('POST', 'HEAD')):
        resource = req[4:]
    resource = resource.replace('HTTP/1.0', '').strip()
    return resource


def __extract_code_bytes__(reply_str):
    """
    private method
    
    extract reply code and reply bytes from reply string string
    
    :param reply_str: "reply" part of one line in log file
    :return: 
    """
    reply_code = ''
    reply_bytes = 0

    reply_lst = reply_str.split()

    if (len(reply_lst) == 2) and (reply_lst[1].isdigit()):
        reply_code = str(reply_lst[0])
        reply_bytes = int(reply_lst[1])

    return reply_code, reply_bytes
