# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.tests import SetupLuitiPackages
config = SetupLuitiPackages.config


class TestSensorSchedule(unittest.TestCase):

    def test_read_all_required_tasks(self):
        from luiti.schedule import SensorSchedule
        SensorSchedule

        from luiti import manager
        BetaReportDay = manager.load_a_task_by_name("BetaReportDay")
        ordered_task_instances = SensorSchedule.read_all_required_tasks(BetaReportDay(date_value="2014-09-01"))
        result = map(str, ordered_task_instances)
        self.assertEqual(result, ['DumpBrowserMapDay(date_value=2014-09-01T00:00:00+08:00)', 'DumpWebLogDay(date_value=2014-09-01T00:00:00+08:00)', 'CleanWebLogDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorByBrowserDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorByRegionDay(date_value=2014-09-01T00:00:00+08:00)', 'CounterVisitorDay(date_value=2014-09-01T00:00:00+08:00)', 'BetaReportDay(date_value=2014-09-01T00:00:00+08:00)'])


if __name__ == '__main__':
    unittest.main()
