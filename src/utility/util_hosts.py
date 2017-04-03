import file_writer
import util


def output_top_hosts(input_rdd, filename):
    pair_rdd = input_rdd.map(__host_map__)
    # err_rdd = pair_rdd.filter(lambda x: x[1] == 0)
    res = pair_rdd.reduceByKey(lambda x, y: x + y) \
        .top(10, key=lambda x: x[1])
    # print(err_rdd.count())
    file_writer.write_pair_list(res, filename)


def __host_map__(x):
    count = 0
    host = util.get_host(x)
    if host:
        # check if the record is valid
        count = 1
    return host, count
