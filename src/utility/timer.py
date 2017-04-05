from datetime import datetime


class Timer:

    def __init__(self):
        self.start_stamp = datetime.now()
        self.last_stamp = datetime.now()

    def start(self):
        self.start_stamp = datetime.now()
        print("Timer start: " + self.start_stamp.strftime('%m-%d %H:%M:%S'))
        self.last_stamp = self.start_stamp

    def stamp(self, msg):
        cur_time = datetime.now()
        section_time = cur_time - self.last_stamp
        total_time = cur_time - self.start_stamp
        print(msg)
        print("Time elased in this Section: " + str(section_time))
        print("Total elapsed time: " + str(total_time) + "\n")
        self.last_stamp = cur_time
