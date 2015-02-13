#-*-coding:utf-8-*-

import os
import sys
import string
import traceback
import importlib
from inflector import Inflector

from .config            import luiti_config as lc
from .active_packages   import active_packages
from .package_map       import PackageMap



class Loader(object):

    @staticmethod
    @active_packages
    def load_all_tasks():
        result     = {"success": list(), "failure": list()}

        for task_clsname_1 in sorted(PackageMap.task_clsname_to_package.keys()):
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
                result['success'].append({"task_cls": task_cls})
            else:
                result['failure'].append({"err": err,           "task_clsname": task_clsname_1})

        return result

    @staticmethod
    @active_packages
    def load_a_task_by_name(s1):
        task_clsname_1  = Inflector().classify(s1)    # force convert
        task_filename_1 = Inflector().underscore(s1)  # force convert

        assert task_clsname_1 in PackageMap.task_clsname_to_package, u"""
        "%s" cannt be found. Auto converted class name is "%s", file name is "luiti_tasks/%s.py", please check it carefully.
        """ % (s1, task_clsname_1, task_filename_1)

        package_path = PackageMap.task_clsname_to_package[task_clsname_1].__name__ + \
                       ".luiti_tasks." + task_filename_1
        task_lib     = lc.import2(package_path)
        return getattr(task_lib, task_clsname_1)
