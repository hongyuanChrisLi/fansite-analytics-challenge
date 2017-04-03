

def output_blocked_hosts(input_rdd, start_time):
    pair_rdd = input_rdd.map(lambda x: __time_map_sec__(x, start_time))