# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest
from luiti import *


# change work dir
os.chdir(os.path.join(root_dir, "tests/project_A"))


@MrTestCase
class TestMrTestCase(unittest.TestCase):

    mr_task_names = [
            'FoobarDay',
            ]

if __name__ == '__main__': unittest.main()
