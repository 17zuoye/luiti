# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

from luiti import *
luigi.plug_packages(
        "project_B",        # dep project
        "etl_utils==0.1.8",  # just for test import
      )
