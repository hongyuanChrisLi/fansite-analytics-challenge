import file_writer
import util

"""
Feature 1: Top 10 hosts/IPs Implementation
"""


def output_top_hosts(input_rdd, filename):
    """
    Input file rdd and write to output file hosts.txt
    1. map: extracts host names or IP addresses from input as the keys
    2. reduce: count by keys
    
    :param input_rdd: rdd from sc.textFile [Each value is one line of log.txt]
    :param filename: hosts.txt [top 10 hosts / IPs that visit this site]
    :return: 
    """
    pair_rdd = input_rdd.map(__host_map__)
    res = pair_rdd\
        .reduceByKey(lambda x, y: x + y) \
        .top(10, key=lambda x: x[1])
    file_writer.write_pair_list(res, filename)


def __host_map__(x):
    """
    This is a private method.
    This method takes one value from input rdd and extracts a host name or IP address
    If host name or IP address is extracted, 1 is assigned to count, otherwise it stays 0
    
    :param x: one value from input rdd [ one line from log.txt]
    :return: a tuple (host, count)
    """
    count = 0
    host = util.get_host(x)
    if host:
        # check if the record is valid
        count = 1
    return host, count
