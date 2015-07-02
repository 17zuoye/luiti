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

    def test_ptm(self):
        from luiti import config
        from luiti.daemon.ptm import PTM

        # setup luiti package path
        sys.path.insert(0, os.path.join(root_dir, "tests"))
        sys.path.insert(0, os.path.join(root_dir, "tests/zip_package_by_luiti"))

        # setup env
        config.curr_project_dir = os.path.join(root_dir, "tests/project_A")

        self.assertEqual(PTM.current_package_name, "project_A")
        self.assertEqual(PTM.current_package_path, config.curr_project_dir)
        self.assertTrue(isinstance(PTM.current_luiti_visualiser_env, dict))

        self.assertEqual(len(PTM.task_classes), 8)
        self.assertEqual(PTM.task_class_names, ['ADay', 'BDay', 'CDay', 'DDay', 'FoobarDay', 'HDay', 'ImportPackagesDay', 'MultipleDependentDay'])

        self.assertEqual(len(PTM.task_clsname_to_package), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package["ADay"].__name__, "project_A")

        self.assertEqual(len(PTM.task_clsname_to_source_file), len(PTM.task_classes))
        self.assertTrue("project_A/luiti_tasks/a_day.py" in PTM.task_clsname_to_source_file["ADay"])

        self.assertEqual(len(PTM.task_clsname_to_package_name), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package_name["ADay"], "project_A")

        self.assertEqual(PTM.task_package_names, ["project_A", "project_B"])

        self.assertEqual(PTM.package_to_task_clsnames, {
            'project_B': ['HDay'],
            'project_A': ['ADay', 'BDay', 'CDay', 'DDay', 'FoobarDay', 'ImportPackagesDay', 'MultipleDependentDay'],
        })


if __name__ == '__main__':
    unittest.main()
