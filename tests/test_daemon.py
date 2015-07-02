# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.task_templates import TaskDay
from luiti import luigi
from luiti.luigi_extensions import ArrowParameter
current_time = ArrowParameter.now()


class FoobarDay(TaskDay):
    root_dir = "/tmp"
    foobar = luigi.Parameter()


class TestDaemon(unittest.TestCase):

    def test_main(self):
        from luiti.daemon import run
        run

    def test_utils(self):
        from luiti.daemon.utils import TaskStorageSet, TaskStorageDict

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

    def test_create_task(self):
        """ test tasks are the same one completely. """
        from luiti.daemon.query_engine.create_task import CreateTask

        t1 = CreateTask.new(FoobarDay, {"date_value": current_time, "foobar": 1})
        t2 = CreateTask.new(FoobarDay, {"date_value": current_time, "foobar": 1})

        self.assertEqual(t1, t2)
        self.assertEqual(t1.task_id, t2.task_id)
        self.assertEqual(hash(t1), hash(t2))

    def test_VisualiserEnvTemplate(self):
        from luiti.daemon import VisualiserEnvTemplate

        env = VisualiserEnvTemplate({
            "file_web_url_prefix": "http://HUE:8888/filebrowser/#/",
            "date_begin": lambda: "2014-09-01",
            "additional_task_parameters": {
                "subject": {
                    "values": ["english", "math"],
                    "default": "english",
                }
            },
            "package_config": {
                "defaults": ["foo", "bar", ],
            }
        }).data

        self.assertTrue(isinstance(env, dict), "raw dict")
        self.assertEqual(env["date_begin"], "2014-09-01", "eval lambda")


if __name__ == '__main__':
    unittest.main()
