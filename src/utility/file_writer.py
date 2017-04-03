

def write_pair_list(res, filename):
    f = open (filename, 'w')
    for(key, value) in res:
        f.write(key + ',' + str(value) + '\n')
    f.close()


def write_keys(res, filename):
    f = open(filename, 'w')
    for(key, value) in res:
        f.write(key + '\n')
    f.close()


def write_list(res, filename):
    f = open(filename, 'w')
    for item in res:
        f.write(item.encode('utf8') + '\n')
    f.close()
