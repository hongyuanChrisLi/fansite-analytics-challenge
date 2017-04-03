import util
import file_writer


WINDOW_SIZE = 3600


def output_top_hours(input_rdd, start_time, filename, partitions):
    pair_rdd = input_rdd.map(lambda x: (util.get_offset_seconds(x, start_time), 1))
    # pair_rdd.foreach(util.print_rdd)
    # print("\n")
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y).partitionBy(partitions)
    # reduce_rdd.foreach(util.print_rdd)
    # print("\n")
    period_rdd = reduce_rdd\
        .flatMap(lambda x: __gen_window__(x))\
        .reduceByKey(lambda x, y: x+y)
    join_rdd = (period_rdd.join(reduce_rdd)).mapValues(lambda val: val[0])
    # join_rdd.foreach(util.print_rdd)
    res = join_rdd.top(10, key=lambda x: x[1])
    file_writer.write_list(util.to_time(res, start_time), filename)


def __gen_window__(x):
    return [(x[0] - offset, x[1]) for offset in range(WINDOW_SIZE)]
