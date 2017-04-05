import unittest
import sys
sys.path.insert(0, '../src/utility')
sys.path.insert(0, 'src/utility')
import util

from datetime import datetime


class TestUtil(unittest.TestCase):
    LOG_FILE = 'tests/test_features/log_input/log.txt'
    STR_VAL = '199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179'
    START_TIME = datetime.strptime('1995-07-01 00:00:01', '%Y-%m-%d %H:%M:%S')

    def test_get_host(self):
        host = util.get_host(TestUtil.STR_VAL)
        try:
            self.assertEqual(host, '199.120.110.21')
            TestUtil.success(util.get_host.__name__)
        except AssertionError:
            TestUtil.failure(util.get_host.__name__)

    def test_get_start_time(self):
        try:
            start_time = util.get_start_time(TestUtil.LOG_FILE)
            self.assertEqual(start_time, TestUtil.START_TIME)
            TestUtil.success(util.get_start_time.__name__)
        except AssertionError:
            TestUtil.failure(util.get_start_time.__name__)

    def test_get_offset_seconds(self):
        try:
            secs = util.get_offset_seconds(TestUtil.STR_VAL, TestUtil.START_TIME)
            self.assertEqual(secs, 10)
            TestUtil.success(util.get_offset_seconds.__name__)
        except AssertionError:
            TestUtil.failure(util.get_offset_seconds.__name__)

    def test_to_time(self):
        res = [(1, 23), (7, 56)]
        expected_time_res = [('01/Jul/1995:00:00:02 -0400', 23), ('01/Jul/1995:00:00:08 -0400', 56)]
        try:
            time_res = util.to_time(res, TestUtil.START_TIME)
            self.assertEqual(time_res, expected_time_res)
            TestUtil.success(util.to_time.__name__)
        except AssertionError:
            TestUtil.failure(util.to_time.__name__)

    def test_parse_request(self):
        try:
            (resource, reply_code, reply_bytes) = util.parse_request(TestUtil.STR_VAL)
            self.assertEqual(resource, '/shuttle/missions/sts-73/sts-73-patch-small.gif')
            self.assertEqual(reply_code, '200')
            self.assertEqual(reply_bytes, 4179)

            TestUtil.success(util.parse_request.__name__)
        except AssertionError:
            TestUtil.failure(util.parse_request.__name__)

    @staticmethod
    def success(func_name):
        print "[PASS] Test: " + func_name

    @staticmethod
    def failure(func_name):
        print "[FAIL] Test: " + func_name

# run_all_unit_tests
if __name__ == '__main__':
    unittest.main()
