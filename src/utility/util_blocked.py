import util
import file_writer


def output_blocked_hosts(input_rdd, start_time, filename):
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
    host = util.get_host(str_val)
    offset_sec = util.get_offset_seconds(str_val, start_time)
    (resource, reply_code, reply_bytes) = util.parse_request(str_val)
    return resource, (host, offset_sec, reply_code, str_val)


def __find__blocked__(lst):
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
            __success_reset__()
        else:
            if status['failed_logins'] == 0:
                __fail_reset__(sec)
            elif status['failed_logins'] < 3:
                if sec - status['block_start'] < 20:
                    status['failed_logins'] += 1
                else:
                    __fail_reset__(sec)
            else:
                if sec - status['block_start'] < 300:
                    blocked.append(str_val)
                else:
                    __fail_reset__(sec)
    return blocked
