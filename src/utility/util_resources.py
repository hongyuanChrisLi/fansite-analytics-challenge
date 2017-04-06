import file_writer
import util

"""
Feature 2: Top 10 Resources Implementation
"""


def output_top_resource(input_rdd, filename):
    """
    Input file rdd and write to output file resources.txt
    1. map: extracts resource names and bytes from input as a key value pair
    2. reduce: adds up bytes by keys
    
    :param input_rdd: rdd from sc.textFile [Each value is one line of log.txt]
    :param filename: resources.txt [top 10 resources ]
    :return: 
    """
    pair_rdd = input_rdd.map(__resource_map__)
    res = pair_rdd.reduceByKey(lambda x, y: x + y).top(10, key=lambda x: x[1])
    file_writer.write_keys(res, filename)


def __resource_map__(x):
    """
    This is a private method.
    This method takes one value from input rdd and calls util.parse_request to get resource name and reply bytes
    
    :param x: one value from input rdd [ one line from log.txt]
    :return: a tuple (resource, reply_bytes)
    """
    (resource, reply_code, reply_bytes) = util.parse_request(x)
    return resource, reply_bytes

