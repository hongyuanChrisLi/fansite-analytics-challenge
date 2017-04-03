import re
from datetime import datetime


def print_rdd(x):
    print (x)


def extract_time(str_val):
    time_lst = re.findall(r"\[(.*?)\]", str_val)
    time_str = time_lst[0].replace('-0400', '').strip()
    return datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S')


def get_start_time(logfile):
    with open(logfile, 'r') as f:
        first_line = f.readline()
        start_time = extract_time(first_line)
    return start_time
