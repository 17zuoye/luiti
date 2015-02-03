# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest


class TestLuitiUtils(unittest.TestCase):

    def test_main(self):
        from luiti import TaskWeek, ArrowParameter
        import arrow

        class HelloWorldWeek(TaskWeek):
            root_dir = "/foobar"

        task1 = HelloWorldWeek("2014-09-02")                            # Tuesday
        self.assertEqual(task1.date_value, ArrowParameter.get("2014-09-01"))     # Monday

        self.assertEqual(task1.data_dir, "/foobar/2014-09-01")
        self.assertEqual(task1.data_file, "/foobar/2014-09-01/hello_world_week.json")
        self.assertEqual(task1.date_str, "2014-09-01")
        self.assertEqual(task1.date_type, "week")
        self.assertEqual(task1.date_value_by_type_in_last, ArrowParameter.get("2014-08-25"))
        self.assertEqual(task1.task_class, HelloWorldWeek)



if __name__ == '__main__': unittest.main()
