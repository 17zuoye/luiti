# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest


class TestDaemon(unittest.TestCase):

    def test_main(self):
        from luiti.daemon import run
        run

    def test_utils(self):
        from luiti.daemon.utils import TaskStorageSet, TaskStorageDict
        from luiti.task_templates import TaskDay
        from luiti.luigi_extensions import ArrowParameter
        from luiti import luigi

        class FoobarDay(TaskDay):
            root_dir = "/tmp"
            foobar = luigi.Parameter()

        current_time = ArrowParameter.now()
        t1 = FoobarDay(date_value=current_time, foobar="1")
        t2 = FoobarDay(date_value=current_time, foobar="1")

        s1 = TaskStorageSet()
        s1.add(t1)
        self.assertEqual(len(s1), 1)
        s1.add(t2)
        self.assertEqual(len(s1), 1)
        self.assertTrue(t1 in s1)
        self.assertTrue(t2 in s1)

        d1 = TaskStorageDict()
        d1[t1].add(t1)
        d1[t2].add(t2)
        self.assertEqual(len(d1), 1)
        self.assertEqual(len(d1[t1]), 1)


if __name__ == '__main__':
    unittest.main()
