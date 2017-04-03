import os
import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext
from utility import util_resources
from utility import util_hosts
from utility import util_hours
from utility import util


log_input = sys.argv[1]
hosts_output = sys.argv[2]
hours_output = sys.argv[3]
resources_output = sys.argv[4]
blocked_output = sys.argv[5]
partitions = int(sys.argv[6])
print(partitions)

conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
input_rdd = sc.textFile(log_input).persist()
util_hosts.output_top_hosts(input_rdd, hosts_output)
util_resources.output_top_resource(input_rdd, resources_output)

# start = datetime.now()
# print(start)
start_time = util.get_start_time(log_input)
util_hours.output_top_hours(input_rdd, start_time, hours_output, partitions)
# end = datetime.now()
# print(end)
# print(end - start)



