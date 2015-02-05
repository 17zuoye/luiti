#-*-coding:utf-8-*-

import os
import sys
import glob
import string
import traceback
import importlib
from inflector import Inflector
from etl_utils import singleton, cached_property
from ..luigi_ext import luigi



@singleton()
class PackageMapClass(object):

    @cached_property
    def task_clsname_to_package(self):
        loaded_name = luigi.luiti_config.curr_project_name + ".luiti_tasks.__init_luiti"
        if loaded_name not in sys.modules: Loader.import2(loaded_name)

        assert luigi.luiti_config.luiti_tasks_packages, "At least have one project!"

        result = dict()
        for project1 in luigi.luiti_config.luiti_tasks_packages:
            for f2 in glob.glob(os.path.join(project1.__path__[0], "luiti_tasks/[a-z]*.py")):
                task_filename3        = os.path.basename(f2).rsplit(".", 1)[0]
                task_clsname4         = Inflector().classify(task_filename3)
                result[task_clsname4] = project1
        return result


class Loader(object):

    PackageMap = PackageMapClass()

    @staticmethod
    def load_all_tasks():
        result     = {"success": list(), "failure": list()}

        for task_clsname_1 in Loader.PackageMap.task_clsname_to_package.keys():
            is_success = False
            task_cls   = None
            err        = None

            try:
                task_cls    = Loader.load_a_task_by_name(task_clsname_1)
                is_success  = True
            except Exception as e:
                err = list(sys.exc_info())
                err[2] = "".join(traceback.format_tb(err[2]))
                err = str(err[0]) + ": " + str(err[1]) + "\n" + err[2]

            if is_success:
                result['success'].append({"task_cls": task_cls, "task_file": file1})
            else:
                result['failure'].append({"err": err,           "task_file": file1})

        return result

    @staticmethod
    def load_a_task_by_name(task_clsname_1):
        assert task_clsname_1[0] in string.uppercase, \
                                "Task name should begin with UpperCase !"

        package_path = Loader.PackageMap.task_clsname_to_package[task_clsname_1].__name__ + \
                       ".luiti_tasks." + \
                       Inflector().underscore(task_clsname_1)
        task_lib     = Loader.import2(package_path)
        return getattr(task_lib, task_clsname_1)


    @staticmethod
    def import2(a_package):
        return __import__(a_package, None, None, 'non_empty')


