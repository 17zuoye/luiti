# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest
from luiti import *


class TestManager(unittest.TestCase):

    def test_Loader(self):
        # change work dir
        os.chdir(os.path.join(root_dir, "tests/project_A"))

        self.assertEqual(
                manager.load_a_task_by_name("ADay"),
                manager.load_a_task_by_name("a_day"),
             )

        self.assertRaises(
                AssertionError,
                lambda : manager.load_a_task_by_name("i_day"),
              )


    def test_generate_a_task(self):
        dir1 = "/tmp/test_generate_a_task/"
        os.system("rm -rf %s" % dir1) # clean prev error
        os.system("mkdir -p %s/luiti_tasks" % dir1)
        os.chdir(dir1)

        content_a = manager.generate_a_task("ADay")
        self.assertTrue("ADay"    in content_a)
        self.assertTrue("TaskDay" in content_a)

        content_b = manager.generate_a_task("b_week")
        self.assertTrue("BWeek"    in content_b)
        self.assertTrue("TaskWeek" in content_b)

        os.system("rm -rf %s" % dir1)


    def test_new_a_project(self):
        os.chdir("/") # fix chdir err
        dir1 = "/tmp/test_new_a_project/"
        os.system("rm -rf %s" % dir1) # clean prev error
        os.system("mkdir -p %s" % dir1)
        os.chdir(dir1)

        files = manager.new_a_project("project_C")

        self.assertTrue("Project C"           in file(files[0]).read())
        self.assertTrue("zip_safe"            in file(files[1]).read())
        self.assertTrue("luigi.plug_packages" in file(files[2]).read())

        os.system("rm -rf %s" % dir1)



if __name__ == '__main__': unittest.main()
