

class FileWriter:

    def __init__(self):
        return

    @staticmethod
    def write_list(res, filename):
        f = open (filename, 'w')
        for(host, count) in res:
            f.write(host + ',' + str(count) + '\n')
        f.close()

    @staticmethod
    def write_keys(res, filename):
        return
