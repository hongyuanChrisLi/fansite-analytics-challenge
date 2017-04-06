import util
import file_writer

"""
Feature 4: Blocked IPs Implementation
"""


def output_blocked_hosts(input_rdd, start_time, filename):
    """
    Input file rdd and write to output file blocked.txt
    1. map: extracts resource, hosts/IPs, offset names and reply codes from input [resource is the key]
    2. filter: keeps values with resource name == '/login' 
    3. map: for each key-value pair, drop key (resource), 
        and generate a new key-value pair 
        where host/IP is the key 
            and the value is a list with one tuple (offset second, reply code, the original input string)
    4. reduce: combines all lists by key (host/IPs)
    5. map: for each host/IP, finds the blocked request attempts as a blocked list
    6. reduce: combines all blocked lists from all hosts/IPs
    
    :param input_rdd: rdd from sc.textFile [Each value is one line of log.txt]
    :param start_time: datetime, extracted from the first line of the log file
    :param filename: blocked.txt 
    :return: 
    """
    res = input_rdd \
        .map(lambda x: __req_map__(x, start_time)) \
        .filter(lambda x: x[0] == '/login') \
        .map(lambda x: (x[1][0], [(x[1][1], x[1][2], x[1][3])])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: __find__blocked__(x[1]))\
        .reduce(lambda x, y: x + y)
    # print(res)
    file_writer.write_list(res, filename)


def __req_map__(str_val, start_time):
    """
    This is a private method.
    This method takes one value from input rdd and calls util util.get_offset_seconds
        to convert a str of timestamp to an integer, which represents offset seconds from the start time
    It also calls util.parse_request to get resource name, host/IP, and reply code
    
    :param str_val: one value from input rdd [ one line from log.txt]
    :param start_time: datetime, extracted from the first line of the log file
    :return: a tuple (resource, (host, offset_sec, reply_code, str_val))
    """
    host = util.get_host(str_val)
    offset_sec = util.get_offset_seconds(str_val, start_time)
    (resource, reply_code, reply_bytes) = util.parse_request(str_val)
    return resource, (host, offset_sec, reply_code, str_val)


def __find__blocked__(lst):
    """
    This is a private method.
    This method loops through a list of request and find blocked requests
    
    :param lst: a list of request. [((offset_sec, reply_code, str_val), (..., ..., ...), ...]
    :return: a list of blocked requests with the form of their original log entries
    """
    lst.sort()
    status = {'failed_logins': 0, 'block_start': 0}
    blocked = []

    def __success_reset__():
        status['failed_logins'] = 0
        status['block_start'] = 0

    def __fail_reset__(mysec):
        status['failed_logins'] = 1
        status['block_start'] = mysec

    for sec, code, str_val in lst:
        if code == '200':
            # If reply code is 200. Login sucess. Reset counters
            __success_reset__()
        else:
            if status['failed_logins'] == 0:
                # This is the first login fail, reset failure counter
                __fail_reset__(sec)
            elif status['failed_logins'] < 3:
                # This is NOT first fail, but within 3 attempts
                if sec - status['block_start'] < 20:
                    # This fail happens within 20s since first fail. Counter add 1
                    status['failed_logins'] += 1
                else:
                    # This fail happens 20s after first fail, reset failure counter
                    __fail_reset__(sec)
            else:
                # This is NOT first fail and exceeds 3 attempt limit
                if sec - status['block_start'] < 300:
                    # This happens within the 5min blocking period. Record it as blocked
                    blocked.append(str_val)
                else:
                    # This happens after 5min blocking period. Reset failure counter
                    __fail_reset__(sec)
    return blocked
