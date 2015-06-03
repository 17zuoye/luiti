# -*- coding: utf-8 -*-

import os
import sys
RootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RootDir)
os.environ['LUIGI_CONFIG_PATH'] = RootDir + '/tests/client.cfg'

import unittest
from luiti import manager

sys.path.insert(0, os.path.join(
    RootDir, "tests/zip_package_by_luiti"))


class TestManager(unittest.TestCase):

    def test_Loader(self):
        # change work dir
        os.chdir(os.path.join(RootDir, "tests/project_A"))

        self.assertEqual(
            manager.load_a_task_by_name("ADay"),
            manager.load_a_task_by_name("a_day"),
        )

        self.assertRaises(
            AssertionError,
            lambda: manager.load_a_task_by_name("not_exists_day"),
        )

    def test_get_all_date_file_to_task_instances(self):
        ADay = manager.load_a_task_by_name("ADay")
        BDay = manager.load_a_task_by_name("BDay")
        files = manager.get_all_date_file_to_task_instances("20140901-20140903", [ADay, BDay])
        self.assertEqual(['/foobar/2014-09-01/a_day.json',
                          '/foobar/2014-09-01/b_day.json',
                          '/foobar/2014-09-02/a_day.json',
                          '/foobar/2014-09-02/b_day.json',
                          '/foobar/2014-09-03/a_day.json',
                          '/foobar/2014-09-03/b_day.json'],
                         sorted(files.keys()))

    def test_load_all_tasks(self):
        all_tasks = manager.load_all_tasks()
        self.assertEqual(manager.ld.result, all_tasks)  # cause they'are linked.

        HDay = manager.load_a_task_by_name("HDay")
        self.assertTrue(HDay in manager.ld.all_task_classes, "project B is also loaded.")

    def test_find_dep_on_tasks(self):
        # simple case
        # ADay is dep on BDay, ADay is inputed into BDay.
        BDay = manager.load_a_task_by_name("BDay")
        dep_tasks_by_BDay = manager.find_dep_on_tasks(BDay, manager.ld.all_task_classes)
        self.assertEqual(len(dep_tasks_by_BDay), 1)
        self.assertEqual(dep_tasks_by_BDay[0].__name__, "ADay")

        # complex case
        #   MultipleDependentDay => HDay => DDay
        #   delete MultipleDependentDay, and delete HDay and DDay.
        MultipleDependentDay = manager.load_a_task_by_name("MultipleDependentDay")
        dep_tasks_by_MultipleDependentDay = manager.find_dep_on_tasks(MultipleDependentDay, manager.ld.all_task_classes)
        self.assertEqual(len(dep_tasks_by_MultipleDependentDay), 2)
        self.assertEqual(sorted(map(lambda i1: i1.__name__, dep_tasks_by_MultipleDependentDay)), ["DDay", "HDay"])

    def test_generate_a_task(self):
        dir1 = "/tmp/test_generate_a_task/"
        os.system("rm -rf %s" % dir1)  # clean prev error
        os.system("mkdir -p %s/luiti_tasks" % dir1)
        os.chdir(dir1)

        content_a = manager.generate_a_task("ADay")
        self.assertTrue("ADay" in content_a)
        self.assertTrue("TaskDay" in content_a)

        content_b = manager.generate_a_task("b_week")
        self.assertTrue("BWeek" in content_b)
        self.assertTrue("TaskWeek" in content_b)

        os.system("rm -rf %s" % dir1)

    def test_new_a_project(self):
        os.chdir("/")  # fix chdir err
        dir1 = "/tmp/test_new_a_project/"
        os.system("rm -rf %s" % dir1)  # clean prev error
        os.system("mkdir -p %s" % dir1)
        os.chdir(dir1)

        files = manager.new_a_project("project_c")

        self.assertTrue("Project C" in file(files[0]).read())
        self.assertTrue("zip_safe" in file(files[1]).read())
        self.assertTrue("luigi.plug_packages" in file(files[2]).read())
        self.assertTrue("@MrTestCase" in file(files[3]).read())

        os.chdir("project_c")
        os.system("python tests/test_main.py")
        os.chdir("..")

        os.system("rm -rf %s" % dir1)


if __name__ == '__main__':
    unittest.main()
