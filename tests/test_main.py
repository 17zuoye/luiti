# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest


class TestLuiti(unittest.TestCase):

    def test_check_date_range(self):
        from luiti import luigi, TaskWeek, arrow

        @luigi.check_date_range()
        class CheckDateRangeExampleWeek(TaskWeek):
            root_dir = "/foobar"

            def run(self):
                return "data"

        next_week = arrow.now().replace(weeks=1)

        # 这周得数据得下周跑
        self.assertEqual(CheckDateRangeExampleWeek(next_week).run(), 'data')
        self.assertEqual(CheckDateRangeExampleWeek(arrow.now()).run(), False)

    def test_check_runtime_range(self):
        from luiti import luigi, TaskWeek, arrow

        @luigi.check_runtime_range(hour_num=[5, 6, 7, 8], weekday_num=[1], )
        class CheckRuntimeRangeExampleWeek(TaskWeek):
            root_dir = "/foobar"

            def run(self):
                return "data"

        day_1 = arrow.get("2014-09-01 06:28")  # valid
        self.assertTrue(day_1)

        def func(d1):
            # overwrite arrow's method directly.
            arrow.now = lambda: arrow.get(d1)
            return CheckRuntimeRangeExampleWeek(d1).run()

        self.assertEqual(func("2014-09-01 09:00"), False)
        self.assertEqual(func("2014-09-02 06:28"), False)
        self.assertEqual(func("2014-09-01 04:28"), False)
        self.assertEqual(func("2014-09-01 05:00"), "data")
        self.assertEqual(func("2014-09-01 06:28"), "data")
        self.assertEqual(func("2014-09-01 08:59"), "data")


if __name__ == '__main__':
    unittest.main()
