#-*-coding:utf-8-*-

import os, sys
import glob
from inflector import Inflector
from etl_utils import singleton, cached_property

from .config import luiti_config as lc
from .active_packages import active_packages


@singleton()
class PackageMapClass(object):

    @cached_property
    @active_packages
    def task_clsname_to_package(self):

        assert lc.luiti_tasks_packages, "At least have one project!"

        result = dict()
        for project1 in lc.luiti_tasks_packages:
            project_dir2 = project1.__path__[0]

            # if it's not a zip file, but a normal package directory
            is_zip_file = os.path.exists(os.path.join(project_dir2, "__init__.py"))
            if not is_zip_file:
                raise Exception("""[setup.py format error] make sure project "%s" zip_safe=False option exists!""" % project1.__name__)

            task_path_pattern = os.path.join(project_dir2, "luiti_tasks/[a-z]*.py")

            for f2 in glob.glob(task_path_pattern):
                task_filename3        = os.path.basename(f2).rsplit(".", 1)[0]
                task_clsname4         = Inflector().classify(task_filename3)
                result[task_clsname4] = project1
        return result
PackageMap = PackageMapClass()
