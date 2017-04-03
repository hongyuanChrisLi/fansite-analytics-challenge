import file_writer
import util


def output_top_resource(input_rdd, filename):
    pair_rdd = input_rdd.map(__resource_map__)
    # pair_rdd.foreach(util.print_rdd)
    res = pair_rdd.reduceByKey(lambda x, y: x + y).top(10, key=lambda x: x[1])
    file_writer.write_keys(res, filename)


def __resource_map__(x):
    (resource, reply_code, reply_bytes) = util.parse_request(x)
    return resource, reply_bytes

