from src.utility import util
from src.utility.timer import Timer


class SerialHours:
    LOG_FILE = '../../log_input/log.txt'
    WINDOW_SIZE = 3600

    def __init__(self):
        self.window_queue = []
        self.window_sum = 0
        self.top10 = []
        self.last_record = 0
        self.rear_sec = 0

    def run(self):

        log_start_time = util.get_start_time(SerialHours.LOG_FILE)
        sec_count = 0

        with open(SerialHours.LOG_FILE, 'r') as f:
            for line in f:
                off_sec = util.get_offset_seconds(line, log_start_time)
                if off_sec == self.rear_sec:
                    sec_count += 1
                else:
                    self.__upd_sum__(self.rear_sec, sec_count)
                    self.rear_sec = off_sec
                    sec_count = 1

        self.__upd_sum__(self.rear_sec, sec_count)
        while self.window_queue:
            end = self.window_queue[0][0] + SerialHours.WINDOW_SIZE
            self.__pop_queue__(end)

        print(self.top10)
        for item in util.to_time(self.top10, log_start_time):
            print(item)

    def __upd_sum__(self, cur_sec, sec_count):
        # print(cur_sec, sec_count)
        self.window_queue.append((cur_sec, sec_count))
        while cur_sec - self.last_record >= SerialHours.WINDOW_SIZE:
            end = cur_sec - SerialHours.WINDOW_SIZE + 1
            self.__pop_queue__(end)
        self.window_sum += sec_count
        # print('D: ' + str(self.window_sum))

    def __pop_queue__(self, end):

        for i in range(self.last_record, end):
            front_item = self.window_queue[0]
            self.__add_to_top__((i, self.window_sum))
            if i == front_item[0]:
                self.window_queue.pop(0)
                self.window_sum -= front_item[1]

            if i == self.rear_sec:
                self.window_queue = []
                break

        # print('C: ' + str(self.window_sum))
        self.last_record = end

    def __add_to_top__(self, item):

        if len(self.top10) == 10 and self.top10[9][1] > item[1]:
            return

        self.top10.append(item)
        self.top10.sort(key=lambda tup: tup[1], reverse=True)

        if len(self.top10) > 10:
            self.top10.pop()

timer = Timer()
timer.start()
serial_hours = SerialHours()
serial_hours.run()
timer.stamp('Finished processing')
