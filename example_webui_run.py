#!/usr/bin/env python
# -*-coding:utf-8-*-

import os
import sys
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)

import logging
logger = logging.getLogger("luiti.server")

# link webui_packages path
from luiti.tests import SetupLuitiPackages
config = SetupLuitiPackages.config
from luiti.daemon import run


task_list_url = "http://localhost:8082/luiti/dag_visualiser?date_value=2015-07-09T00%3A00%3A00%2B08%3A00&language=English&luiti_package=luiti_summary&luiti_package=luiti_clean&luiti_package=luiti_dump&luiti_package=luiti_middle&luiti_package=project_A&luiti_package=project_B"
task_show_url = "http://localhost:8082/luiti/dag_visualiser?date_value=2015-07-09T00%3A00%3A00%2B08%3A00&language=English&luiti_package=luiti_summary&luiti_package=luiti_clean&luiti_package=luiti_dump&luiti_package=luiti_middle&luiti_package=project_A&luiti_package=project_B&task_cls=BetaReportDay"

# generated from http://www.network-science.de/ascii/
print """
( \      |\     /|\__   __/\__   __/\__   __/
| (      | )   ( |   ) (      ) (      ) (
| |      | |   | |   | |      | |      | |
| |      | |   | |   | |      | |      | |
| |      | |   | |   | |      | |      | |
| (____/\| (___) |___) (___   | |   ___) (___
(_______/(_______)\_______/   )_(   \_______/

"""
print "Welcome to luiti's test webui example!"
print
print "  Open below two urls in your favourite browser."
print
print "  task_list_url: ", task_list_url
print "  task_show_url: ", task_show_url
print

run("0.0.0.0", 8082)
