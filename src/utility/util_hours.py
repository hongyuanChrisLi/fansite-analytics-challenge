import util
import file_writer


WINDOW_SIZE = 3600
BUCKET_SIZE = WINDOW_SIZE * 2
TOP_BUCKETS = 20
PARTITIONS = 10


def output_top_hours_tuned(input_rdd, start_time, filename):
    pair_rdd = input_rdd.map(lambda x: (util.get_offset_seconds(x, start_time), 1))
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)

    bucket1_rdd = reduce_rdd\
        .map(lambda x: (int(x[0]) / BUCKET_SIZE, x[1])) \
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0] * BUCKET_SIZE, x[1]))

    bucket2_rdd = reduce_rdd\
        .map(lambda x: ((int(x[0]) - WINDOW_SIZE) / BUCKET_SIZE, x[1]))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0] * BUCKET_SIZE + WINDOW_SIZE, x[1]))\
        .filter(lambda x: x[0] >= 0)

    top_buckets = (bucket1_rdd.union(bucket2_rdd)).top(TOP_BUCKETS, key=lambda x: x[1])

    top10_in_bucket = __get_top10_hours__(reduce_rdd, top_buckets[0][0], top_buckets[0][0] + BUCKET_SIZE)
    tenth_count = top10_in_bucket[9][1]
    buckets = __get_selected_buckets__(top_buckets[1:], tenth_count)
    res = top10_in_bucket

    if buckets:
        for bucket in buckets:
            print("processing bucket " + str(bucket))
            res += __get_top10_hours__(reduce_rdd, bucket[0], bucket[1])

    res.sort(key=lambda tup: tup[1], reverse=True)
    file_writer.write_pair_list(util.to_time(res[:10], start_time), filename)


def output_top_hours(input_rdd, start_time, filename):
    pair_rdd = input_rdd.map(lambda x: (util.get_offset_seconds(x, start_time), 1))
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y).partitionBy(PARTITIONS)
    period_rdd = reduce_rdd\
        .flatMap(lambda x: __gen_window__(x))\
        .reduceByKey(lambda x, y: x+y)\
        .filter(lambda x: x[0] >= 0)
    res = period_rdd.top(10, key=lambda x: x[1])
    print(res)
    file_writer.write_pair_list(util.to_time(res, start_time), filename)


def __gen_window__(x):
    return [(x[0] - offset, x[1]) for offset in range(WINDOW_SIZE)]


def __get_top10_hours__(rdd, start, end):
    parts = int(end - start) / (BUCKET_SIZE * 5)

    if not parts:
        parts = 1

    filter_rdd = rdd\
        .filter(lambda x: (x[0] >= start) and (x[0] < end))\
        .partitionBy(parts)
    # filter_rdd.foreach(util.print_rdd)

    period_rdd = filter_rdd\
        .flatMap(lambda x: __get_bucket_window__(x, start, end)) \
        .reduceByKey(lambda x, y: x + y)
    # period_rdd.foreach(util.print_rdd)

    return period_rdd.top(10, key=lambda x: x[1])


def __get_bucket_window__(x, start, end):
    range_start = WINDOW_SIZE - (end - x[0])
    if range_start < 0:
        range_start = 0

    range_end = x[0] - start + 1
    if range_end > WINDOW_SIZE:
        range_end = WINDOW_SIZE

    return [(x[0] - offset, x[1]) for offset in range(range_start, range_end)]


def __get_selected_buckets__(buckets, min_count):
    selected_buckets = []
    combined_buckets = []

    if len(buckets) > 1:
        for bucket_start, count in buckets:
            if count > min_count:
                selected_buckets.append((bucket_start, bucket_start + BUCKET_SIZE))

        if selected_buckets:

            selected_buckets.sort()
            # print(selected_buckets)
            cur_bucket = selected_buckets[0]

            if len(selected_buckets) > 1:
                for bucket in selected_buckets[1:]:
                    if cur_bucket[1] > bucket[0]:
                        cur_bucket = (cur_bucket[0], bucket[1])
                    else:
                        combined_buckets.append(cur_bucket)
                        cur_bucket = bucket

            combined_buckets.append(cur_bucket)

    return combined_buckets
