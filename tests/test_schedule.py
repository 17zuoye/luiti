# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.tests import SetupLuitiPackages
config = SetupLuitiPackages.config

from luiti.schedule import SensorSchedule
from luiti import luigi, TaskDay, manager


class TestSensorSchedule(unittest.TestCase):

    def test_read_all_required_tasks(self):
        BetaReportDay = manager.load_a_task_by_name("BetaReportDay")
        ss = SensorSchedule(BetaReportDay, "2014-09-01")

        result = map(lambda i1: i1.task_clsname, ss.ordered_task_instances_list)
        self.assertEqual(result, ['DumpBrowserMapDay', 'DumpWebLogDay', 'CleanWebLogDay', 'CounterVisitorByBrowserDay', 'CounterVisitorByRegionDay', 'CounterVisitorDay', 'BetaReportDay'])

    def test_is_external(self):
        class ExampleExternalTask(luigi.ExternalTask):
            pass
        self.assertTrue(SensorSchedule.is_external(ExampleExternalTask()))

        class LuitiTaskDay(TaskDay):
            is_external = True
            root_dir = "/foobar"
        self.assertTrue(SensorSchedule.is_external(LuitiTaskDay(date_value="2014-09-01")))


if __name__ == '__main__':
    unittest.main()
