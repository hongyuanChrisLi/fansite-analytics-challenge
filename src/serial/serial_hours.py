from src.utility import util
from src.utility.timer import Timer

"""
Feature 3: Top 10 60-minute periods (A serial implementation)

"""

class SerialHours:

    LOG_FILE = '../../log_input/log.txt'
    WINDOW_SIZE = 3600

    def __init__(self):
        """
        window_queue: a sliding window over offset seconds. 
            Latest element is appended to the end of the queue. 
            Earliest element is at the beginning of the queue
        window_sum: the sum of counts of offset seconds within current window queue
        top10: top 10 windows with the start "offset second" and window_sum
        last_record: the start of last window, whose window_sum has been calculated
        rear_sec: the last element of the queue
        """
        self.window_queue = []
        self.window_sum = 0
        self.top10 = []
        self.last_record = 0
        self.rear_sec = 0

    def run(self):
        """
        call this public method to start the process. 
        
        It reads the log file line by line, extract timestamp string to offset seconds
        In each loop: 
            1. calculated the sum of the current offset second
            2. call __upd_sum__
            3. update self.rear_sec
        
        After the loop, it prints the top 10 60-minute periods
        
        :return: 
        """

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
        """
        This private method updates self.window_queue and self.window_sum
        When cur_sec - self.last_record >= SerialHours.WINDOW_SIZE, 
            which means it's time to slide the window, __pop_queue__ is called
        
        :param cur_sec: current offset second that needs to be added to self.window_queue
        :param sec_count: the count of this offset second
        :return: 
        """
        # print(cur_sec, sec_count)
        self.window_queue.append((cur_sec, sec_count))
        while cur_sec - self.last_record >= SerialHours.WINDOW_SIZE:
            end = cur_sec - SerialHours.WINDOW_SIZE + 1
            self.__pop_queue__(end)
        self.window_sum += sec_count
        # print('D: ' + str(self.window_sum))

    def __pop_queue__(self, end):
        """
        pop all offset seconds in self.window_queue that are less than the "end"
        generate key-value pairs for all offset seconds 
            that are larger than self.last_record and less than "end"
        call __add_to_top__ and let this method decides whether these new key-value pairs made to top 10
        
        :param end: the end (upper boundary) offset seconds that needs to be popped
        :return: 
        """

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
        """
        maintaining top 10 
        
        :param item: key-valu pair (offset_sec, count)
        :return: 
        """

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
