# -*- coding: utf-8 -*-

__all__ = ["luigi", "TaskDay", "cached_property", "TaskDayHadoop",
           "json", "MRUtils", ]

import os
import sys
root_dir = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'


from luiti import luigi, TaskDay, cached_property, TaskDayHadoop, json, MRUtils
luigi.plug_packages(
    "project_B",        # dep project
    "etl_utils==0.1.10",  # just for test import
    "zip_package_by_luiti",  # zip file package
)
