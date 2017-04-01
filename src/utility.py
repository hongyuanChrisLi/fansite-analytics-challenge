import re
from file_writer import FileWriter


class Utility:

    def __init__(self):
        return

    @staticmethod
    def output_top_hosts(input_rdd, filename):
        pair_rdd = input_rdd.map(Utility.__host_map__)
        # err_rdd = pair_rdd.filter(lambda x: x[1] == 0)
        res = pair_rdd.reduceByKey(lambda x, y: x + y) \
            .top(10, key=lambda x: x[1])
        # print(err_rdd.count())
        FileWriter.write_list(res, filename)

    @staticmethod
    def output_top_resource(input_rdd, filename):
        pair_rdd = input_rdd.map(Utility.resource_map)
        # valid_rdd = pair_rdd.filter(lambda x: x[0])
        res = pair_rdd.reduceByKey(lambda x, y: x + y) \
            .top(10, key=lambda x: x[1])
        print (res)

    @staticmethod
    def __print_rdd__(x):
        print (x)

    @staticmethod
    def __host_map__(x):
        count = 0
        items = x.split('- -')
        if len(items) == 2:
            # check if the record is valid
            count = 1

        host = items[0].rstrip()
        return host, count

    @staticmethod
    def resource_map(x):
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

                resource = Utility.__get_resource__(req_str)

                if resource:
                    reply_bytes = Utility.__get_reply_bytes__(second_str.replace('"' + req_str + '"', '').strip())

        return resource, reply_bytes

    @staticmethod
    def __get_resource__(req):

        resource = ''

        if req.startswith('GET'):
            resource = req[3:]
        elif req.startswith(('POST', 'HEAD')):
            resource = req[4:]
        resource = resource.replace('HTTP/1.0', '').strip()
        return resource

    @staticmethod
    def __get_reply_bytes__(num_str):

        reply_bytes = 0

        num_lst = num_str.split()

        if (len(num_lst) == 2) and (num_lst[1].isdigit()):
            reply_bytes = int(num_lst[1])

        return reply_bytes
