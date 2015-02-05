# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest


# 1. change to work dir
project_dir = root_dir + "/tests/project_A"
os.chdir(project_dir)

# 2. init luiti env
sys.path.insert(0, root_dir + "/tests") # project sys.path
import project_A.luiti_tasks.__init_luiti

# 3. setup tests variables
from luiti import manager, luigi
day_str                    = "2014-09-01T00:00:00+08:00"

class TestLuiti(unittest.TestCase):

    def test_ref_tasks(self):
        ADay = manager.load_a_task_by_name("ADay")
        BDay = manager.load_a_task_by_name("BDay")
        CDay = manager.load_a_task_by_name("CDay")

        ADay_task = ADay(day_str)

        self.assertEqual(ADay_task.BDay, BDay)
        self.assertEqual(ADay_task.CDay, CDay)

        self.assertEqual(ADay_task.count, 1)
        self.assertEqual(ADay_task.BDay_task.count, 2)
        self.assertEqual(ADay_task.CDay_task.count, 3)
        self.assertEqual(ADay_task.total_count, 6)

        self.assertEqual(ADay_task.date_value, ADay_task.BDay_task.date_value)
        self.assertEqual(ADay_task.date_value, ADay_task.CDay_task.date_value)

    def test_multiple_luiti_tasks(self):
        DDay = manager.load_a_task_by_name("DDay")
        HDay = manager.load_a_task_by_name("HDay")

        DDay_task = DDay(day_str)
        self.assertEqual(DDay_task.HDay, HDay)
        self.assertEqual(DDay_task.total_count, 12)

        # hash is luigi's test task unique method
        self.assertEqual(hash(DDay_task.HDay_task), hash(HDay(day_str)))

    def test_serialize_and_unserialize(self):
        """
        Fix multiple projects use the same `luiti_tasks` namespace.

        But if there are same directory(e.g. ./templetes`  under the `luiti_tasks`, and luiti can't figure out the right choice.
        """
        import pickle
        import cPickle

        def serialize_and_unserialize_a_task_instance(cls_name, serialize):
            task_cls               = manager.load_a_task_by_name(cls_name)
            task_instance          = task_cls(day_str)

            task_instance_2        = serialize.loads(serialize.dumps(task_instance))

            self.assertEqual(hash(task_instance), hash(task_instance_2))

            for ref_task_name_3 in task_cls._ref_tasks:
                self.assertEqual(getattr(task_instance, ref_task_name_3), getattr(task_instance_2, ref_task_name_3))
                self.assertEqual(hash(getattr(task_instance, ref_task_name_3+"_task")), hash(getattr(task_instance_2, ref_task_name_3+"_task")))


        serialize_and_unserialize_a_task_instance('ADay', pickle)
        serialize_and_unserialize_a_task_instance('ADay', cPickle)
        serialize_and_unserialize_a_task_instance('DDay', pickle)
        serialize_and_unserialize_a_task_instance('DDay', cPickle)

    def test_plug_packages(self):
        package_names = [i1.__name__ for i1 in luigi.luiti_config.luiti_tasks_packages]
        self.assertTrue("project_A" in package_names)
        self.assertTrue("project_B" in package_names)

        self.assertTrue("ADay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("BDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("CDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("DDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("HDay" in manager.PackageMap.task_clsname_to_package)


if __name__ == '__main__': unittest.main()
