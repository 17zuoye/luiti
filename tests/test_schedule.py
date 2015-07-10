# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.tests import SetupLuitiPackages
config = SetupLuitiPackages.config

from luiti.schedule import SensorSchedule
from luiti import luigi, TaskDay


class TestSensorSchedule(unittest.TestCase):

    def test_read_all_required_tasks(self):
        SensorSchedule

        from luiti import manager
        BetaReportDay = manager.load_a_task_by_name("BetaReportDay")
        ordered_task_instances = SensorSchedule.read_all_required_tasks(BetaReportDay(date_value="2014-09-01"))
        result = map(str, ordered_task_instances)
        self.assertEqual(result, ['DumpBrowserMapDay(date_value=2014-09-01T00:00:00+08:00)', 'DumpWebLogDay(date_value=2014-09-01T00:00:00+08:00)', 'CleanWebLogDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorByBrowserDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorByRegionDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorDay(date_value=2014-09-01T00:00:00+08:00)', 'BetaReportDay(date_value=2014-09-01T00:00:00+08:00)'])

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
