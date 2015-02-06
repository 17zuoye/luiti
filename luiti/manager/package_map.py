#-*-coding:utf-8-*-

import os, sys
import glob
from inflector import Inflector
from etl_utils import singleton, cached_property

from .config import luiti_config as lc
from .setup_packages import setup_packages


@singleton()
class PackageMapClass(object):

    @cached_property
    @setup_packages
    def task_clsname_to_package(self):

        assert lc.luiti_tasks_packages, "At least have one project!"

        result = dict()
        for project1 in lc.luiti_tasks_packages:
            for f2 in glob.glob(os.path.join(project1.__path__[0], "luiti_tasks/[a-z]*.py")):
                task_filename3        = os.path.basename(f2).rsplit(".", 1)[0]
                task_clsname4         = Inflector().classify(task_filename3)
                result[task_clsname4] = project1
        return result
PackageMap = PackageMapClass()
