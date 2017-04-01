import re
import file_writer
import util


def output_top_resource(input_rdd, filename):
    pair_rdd = input_rdd.map(__resource_map__)
    # valid_rdd = pair_rdd.filter(lambda x: x[0])
    res = pair_rdd.reduceByKey(lambda x, y: x + y).top(10, key=lambda x: x[1])
    file_writer.write_keys(res, filename)
    print(res)


def __resource_map__(x):
    resource = ''
    reply_bytes = 0

    str_splits = x.split(']')

    if len(str_splits) == 2:
        second_str = str_splits[1]
        # e.g. "GET /history/apollo/ HTTP/1.0" 200 6245

        req_lst = re.findall('"([^"]*)"', second_str)
        # e.g. GET /history/apollo/ HTTP/1.0

        if len(req_lst) == 1:
            req_str = req_lst[0]
            # req_splits = req_str.split()

            resource = __get_resource__(req_str)

            if resource:
                reply_bytes = __get_reply_bytes__(second_str.replace('"' + req_str + '"', '').strip())

    return resource, reply_bytes


def __get_resource__(req):
    resource = ''

    if req.startswith('GET'):
        resource = req[3:]
    elif req.startswith(('POST', 'HEAD')):
        resource = req[4:]
    resource = resource.replace('HTTP/1.0', '').strip()
    return resource


def __get_reply_bytes__(num_str):
    reply_bytes = 0

    num_lst = num_str.split()

    if (len(num_lst) == 2) and (num_lst[1].isdigit()):
        reply_bytes = int(num_lst[1])

    return reply_bytes
