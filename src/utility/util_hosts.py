import file_writer


def output_top_hosts(input_rdd, filename):
    pair_rdd = input_rdd.map(__host_map__)
    # err_rdd = pair_rdd.filter(lambda x: x[1] == 0)
    res = pair_rdd.reduceByKey(lambda x, y: x + y) \
        .top(10, key=lambda x: x[1])
    # print(err_rdd.count())
    file_writer.write_list(res, filename)


def __host_map__(x):
    count = 0
    items = x.split('- -')
    if len(items) == 2:
        # check if the record is valid
        count = 1

    host = items[0].rstrip()
    return host, count