import util
import file_writer


WINDOW_SIZE = 3600


def output_top_hours(input_rdd, start_time, filename, partitions):
    pair_rdd = input_rdd.map(lambda x: __time_map_sec__(x, start_time))
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y).partitionBy(partitions)
    # reduce_rdd.foreach(util.print_rdd)
    period_rdd = reduce_rdd\
        .flatMap(lambda x: __gen_window__(x))\
        .reduceByKey(lambda x, y: x+y)
    join_rdd = (period_rdd.join(reduce_rdd)).mapValues(lambda val: val[0])
    # join_rdd.foreach(util.print_rdd)
    res = join_rdd.top(10, key=lambda x: x[1])
    file_writer.write_list(util.to_time(res, start_time), filename)


def __gen_window__(x):
    return [(x[0] - offset, x[1]) for offset in range(WINDOW_SIZE)]


def __time_map_sec__(str_val, start_time):
    curtime = util.extract_time(str_val)
    return int((curtime - start_time).total_seconds()), 1