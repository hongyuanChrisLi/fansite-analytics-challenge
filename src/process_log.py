from pyspark import SparkConf, SparkContext


class ProcessLog:
    def __init__(self):
        conf = SparkConf().setMaster('local').setAppName('Insight')
        self.sc = SparkContext(conf=conf)
        self.input_rdd = self.sc.textFile('../log_input/log.txt')

    def find_top_hosts(self):
        pair_rdd = self.input_rdd.map(ProcessLog.__host_map__)
        err_rdd = pair_rdd.filter(lambda x: x[1] == 0)
        res = pair_rdd.reduceByKey(lambda x, y: x + y) \
            .top(10, key=lambda x: x[1])
        print(err_rdd.count())
        print (res)

    @staticmethod
    def __print_rdd__(x):
        print (x)

    @staticmethod
    def __host_map__(x):
        num = 0
        items = x.split('- -')
        if len(items) == 2:
            # check if the record is valid
            num = 1

        host = items[0].rstrip()
        return host, num



pl = ProcessLog()
pl.find_top_hosts()



# print (input_rdd.getNumPartitions())
# split_rdd = input_rdd.map(lambda line: line.split('- -'))
# pair_rdd = split_rdd.map(lambda x: (x[0].rstrip(), 1))
# res = pair_rdd.reduceByKey(lambda x, y: x + y)\
#     .top(10, key=lambda x: x[1])
# print (res)
# .foreach(print_rdd)

# pair_rdd =
