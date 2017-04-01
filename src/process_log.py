import os
import sys

from pyspark import SparkConf, SparkContext
from utility import util_resources
from utility import util_hosts

log_input = sys.argv[1]
hosts_output = sys.argv[2]
hours_output = sys.argv[3]
resources_output = sys.argv[4]
blocked_output = sys.argv[5]

print (log_input)
print (hosts_output)
print (resources_output)
print (os.getcwd())


conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)
input_rdd = sc.textFile(log_input)
util_hosts.output_top_hosts(input_rdd, hosts_output)
util_resources.output_top_resource(input_rdd, resources_output)




