

def write_pair_list(res, filename):
    """
    write a list of key-value pairs
    :param res: a list of key-value pairs
    :param filename: output file
    :return: 
    """
    f = open (filename, 'w')
    for(key, value) in res:
        f.write(key + ',' + str(value) + '\n')
    f.close()


def write_keys(res, filename):
    """
    write keys of a list of key-value pairs
    :param res: a list of key-value pairs
    :param filename: output file
    :return: 
    """
    f = open(filename, 'w')
    for(key, value) in res:
        f.write(key + '\n')
    f.close()


def write_list(res, filename):
    """
    write a list of values
    :param res: a list of values
    :param filename: output file
    :return: 
    """
    f = open(filename, 'w')
    for item in res:
        f.write(item.encode('utf8') + '\n')
    f.close()
