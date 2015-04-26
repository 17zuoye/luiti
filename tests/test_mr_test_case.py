# -*- coding: utf-8 -*-

import os
import sys
RootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RootDir)
os.environ['LUIGI_CONFIG_PATH'] = RootDir + '/tests/client.cfg'

import unittest
from luiti import MrTestCase

sys.path.insert(0, os.path.join(
    RootDir, "tests/zip_package_by_luiti"))


# change work dir
os.chdir(os.path.join(RootDir, "tests/project_A"))


@MrTestCase
class TestMrTestCase(unittest.TestCase):

    mr_task_names = [
        'FoobarDay',
    ]

if __name__ == '__main__':
    unittest.main()
