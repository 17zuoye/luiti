# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest
from luiti import MrTestCase


@MrTestCase
class TestMapReduce(unittest.TestCase):
    mr_task_names = [
            ]

if __name__ == '__main__':
    unittest.main()