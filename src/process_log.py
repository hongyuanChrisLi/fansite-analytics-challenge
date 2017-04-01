from pyspark import SparkConf, SparkContext
from utility import Utility

import sys


log_input = sys.argv[1]
hosts_output = sys.argv[2]
hours_output = sys.argv[3]
resources_output = sys.argv[4]
blocked_output = sys.argv[5]

print (log_input)
print (hosts_output)
print (resources_output)

conf = SparkConf().setMaster('local').setAppName('Insight')
sc = SparkContext(conf=conf)
input_rdd = sc.textFile(log_input)
Utility.output_top_hosts(input_rdd, hosts_output)
# Utility.output_top_resource(input_rdd, resources_output)




