import re
import util
import file_writer
from datetime import datetime, timedelta

window_size = 3600


def output_top_hours(input_rdd, start_time, filename):
    pair_rdd = input_rdd.map(lambda x: __time_map_sec__(x, start_time))
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)
    # reduce_rdd.foreach(util.print_rdd)
    period_rdd = reduce_rdd.flatMap(lambda x: __gen_window__(x, window_size))\
        .groupByKey().mapValues(lambda vals: __sum__(vals))
    join_rdd = period_rdd.join(reduce_rdd).mapValues(lambda val: val[0])
    # join_rdd.foreach(util.print_rdd)
    res = join_rdd.top(10, key=lambda x: x[1])
    file_writer.write_list(__to_time__(res, start_time), filename)


def __sum__(vals):
    res = 0
    for x in vals:
        res += x
    return res


def __gen_window__(x, n):
    return [(x[0] - offset, x[1]) for offset in range(n)]


def __time_map_sec__(str_val, start_time):
    curtime = util.extract_time(str_val)
    return int((curtime - start_time).total_seconds()), 1


def __to_time__(res, start_time):
    time_res = []
    for seconds, val in res:
        curtime = (start_time + timedelta(seconds=seconds))
        time_str = datetime.strftime(curtime, '%d/%b/%Y:%H:%M:%S') + ' -0400'
        time_res.append((time_str, val))

    return time_res
