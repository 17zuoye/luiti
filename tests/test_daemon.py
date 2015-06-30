# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest


class TestDaemon(unittest.TestCase):

    def test_main(self):
        from luiti.daemon import run
        run


if __name__ == '__main__':
    unittest.main()
