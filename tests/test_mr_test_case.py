# -*- coding: utf-8 -*-

import os
import sys
RootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RootDir)
os.environ['LUIGI_CONFIG_PATH'] = RootDir + '/tests/client.cfg'

from luiti.tests import SetupLuitiPackages
config = SetupLuitiPackages.config

import unittest
from luiti import MrTestCase


@MrTestCase
class TestMrTestCase(unittest.TestCase):

    mr_task_names = [
        'FoobarDay',
    ]

if __name__ == '__main__':
    unittest.main()
