import util
import file_writer

"""
Feature 3: Top 10 60-minute periods

Static variables: 
    WINDOW_SIZE: The 60-minute period
    BUCKET_SIZE: 2 times WINDOWS_SIZE, 
        used for pre-processing to narrow down ranges of calculations
    TOP_BUCKETS: The number of top buckets are considered for finer search
"""

WINDOW_SIZE = 3600
BUCKET_SIZE = WINDOW_SIZE * 2
TOP_BUCKETS = 20


def output_top_hours_tuned(input_rdd, start_time, filename):
    """
    Input file rdd and write to output file hours.txt
    
    === Input processing ===
    1. map: extracts offset seconds from input as keys. The value part "count" is initialized with 1
    2. reduce: sum of counts by key
    
    === Pre-Processing [Purpose: Narrow down the calculation ranges]===
    3. create buckets (bucket1_rdd) with intervals [0, 7200), [7200, 14400), [14400, 21600), ...
        a.map: calculate bucket keys by integer division of BUCKET_SIZE
        b.reduce: sum of counts
        c.map: map bucket keys back to offset seconds
    4. create buckets (bucket2_rdd) with intervals [3600, 10800), [10800, 18000), [18000, 25200), ...
        a.map: calculate bucket keys by deduction of WINDOW_SIZE and integer division of BUCKET_SIZE
        b.reduce: sum of counts
        c.map: map bucket keys back to offset seconds
        d.filter: remove negative seconds
    5. union: combine bucket1_rdd and bucket2_rdd
    6. top: find the top 20 buckets by counts 
    7. calculate the top 10 60-minute periods in the top 1 bucket (see function: __get_top10_hours__)
    8. get the 10th 60-minute period of the top 10 and its count "tenth_count"
    9. use "tenth_count" as the criterium to filter out buckets with less count from top 20 
        (see function: __get_selected_buckets__)
    
    === top 10 periods processing ===
    10. calculate top 10 60-minute periods in all the remaining buckets and append them to the result list
    11. sort the result list and get the top 10 60-mionute period
    
    :param input_rdd: rdd from sc.textFile [Each value is one line of log.txt] 
    :param start_time: datetime, extracted from the first line of the log file
    :param filename: hours.txt
    :return: 
    """
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


def __get_top10_hours__(rdd, start, end):
    """
    This is a private method.
    
    1. filter: keeps key-value pairs within the bucket. Maybe partitions depends of bucket size. 
        (Note: the actual bucket size may not always be BUCKET_SIZE, see function: __get_selected_buckets__)
    2. flatMap: for one single offset seconds generates key-value pairs for all previous seconds within a WINDOW_SIZE
        (see function: __get_bucket_window__ )
    3. reduce: sum of counts by key (offset_sec). Here the key offset_sec is treated as the start of a 60-minute period
    4. top: top 10 60-minute periods by count
    
    :param rdd: rdd of key-value pairs (offset_sec, count)
    :param start: start offset second of a bucket
    :param end: end offset second of a bucket
    :return: a list of tuples [(offset_sec, count), (..., ...), ...]
    """
    parts = int(end - start) / (BUCKET_SIZE * 5)

    if not parts:
        parts = 1

    filter_rdd = rdd\
        .filter(lambda x: (x[0] >= start) and (x[0] < end))\
        .partitionBy(parts)

    period_rdd = filter_rdd\
        .flatMap(lambda x: __get_bucket_window__(x, start, end)) \
        .reduceByKey(lambda x, y: x + y)

    return period_rdd.top(10, key=lambda x: x[1])


def __get_bucket_window__(x, start, end):
    """
    This is a private method.
    
    For a single offset second (x[0]), this methods generates all previous offset seconds within a WINDOW_SIZE.
    For example, if x[0] == 7200, this method returns [3601, 3602, ...., 7200], 
        because all offset seconds in this list count 7200 as part of the 60-minute periods starting with them
        
    range_start and range_end are boundary controls that make sure the 60-min periods are within the bucket. 
    
    :param x: a rdd key-value pair
    :param start: start offset second of a bucket
    :param end: end offset second of a bucket
    :return: a list of offset seconds [offset_sec, ..., ... ]
    """
    range_start = WINDOW_SIZE - (end - x[0])
    if range_start < 0:
        range_start = 0

    range_end = x[0] - start + 1
    if range_end > WINDOW_SIZE:
        range_end = WINDOW_SIZE

    return [(x[0] - offset, x[1]) for offset in range(range_start, range_end)]


def __get_selected_buckets__(buckets, min_count):
    """
    This is a private method.
    
    1. select buckets that their counts are larger than min_count
    2. combine adjacent buckets into bigger bucket
    
    :param buckets: a list of key value pair with bucket_start and count [(bucket_start, count), (..., ...), ...].
        These buckets are fixed-sized BUCKET_SIZE
    :param min_count: the count of 10th 60-min period from top 1 bucket
    :return: a list of selected, combined buckets with bucket_start and bucket_end [(bucket_start, bucket_end), ...]. 
        These buckets may not be fixed-sized. Instead, several times of BUCKET_SIZE. 
    """
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


"""
!! DEPRECATED !!

This section includes a previously developed functions with poor performance.
No pre-processing is used to narrow down the calculation ranges. 

"""


def output_top_hours(input_rdd, start_time, filename):
    """
    !! DEPRECATED !!

    1. map: extracts offset seconds from input as keys. The value part "count" is initialized with 1
    2. reduce: sum of counts by key
    3. flatMap: for one single offset seconds generates key-value pairs for all previous seconds within a WINDOW_SIZE
    
    :param input_rdd: rdd from sc.textFile [Each value is one line of log.txt] 
    :param start_time: datetime, extracted from the first line of the log file
    :param filename: hours.txt
    :return: 
    """
    partitions = 10
    pair_rdd = input_rdd.map(lambda x: (util.get_offset_seconds(x, start_time), 1))
    reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y).partitionBy(partitions)
    period_rdd = reduce_rdd \
        .flatMap(lambda x: __gen_window__(x)) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: x[0] >= 0)
    res = period_rdd.top(10, key=lambda x: x[1])
    print(res)
    file_writer.write_pair_list(util.to_time(res, start_time), filename)


def __gen_window__(x):
    """
    !! DEPRECATED !!
    
    This is a private method.
    
    :param x: a rdd key-value pair
    :return: a list of offset seconds [offset_sec, ..., ... ]
    """
    return [(x[0] - offset, x[1]) for offset in range(WINDOW_SIZE)]
