# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest


from luiti import manager
manager.Path.enable_ignore = False
manager.Path.ProjectDir    = os.path.join(root_dir, "tests")
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

if __name__ == '__main__': unittest.main()