import util


def output_blocked_hosts(input_rdd, start_time):
    pair_rdd = input_rdd\
        .map(lambda x: __req_map__(x, start_time))\
        .filter(lambda x: x[0] == '/login')
    pair_rdd.foreach(util.print_rdd)


def __req_map__(str_val, start_time):
    offset_sec = util.get_offset_seconds(str_val, start_time)
    (resource, reply_code, reply_bytes) = util.parse_request(str_val)
    return resource, (offset_sec, reply_code, str_val)
