#-*-coding:utf-8-*-

import os
import sys
import glob
import string
import importlib
from inflector import Inflector
from .path import Path

class Loader(object):

    @staticmethod
    def load_all_tasks(*luiti_tasks_dirs):
        result     = {"success": list(), "failure": list()}
        task_files = []

        for dir1 in luiti_tasks_dirs:
            task_files.extend(
                    glob.glob(os.path.join(dir1, Path.TasksDir, "[a-z]*.py"))
                )

        def fix_load(file1, result):
            is_success = False
            task_cls   = None
            err        = None

            try:
                basename1   = os.path.basename(file1).split(".")[0]
                classname1  = Inflector().classify(basename1)
                task_cls    = Loader.load_a_task_by_name(classname1)
                is_success  = True
            except Exception as e:
                err = e

            if is_success:
                result['success'].append({"task_cls": task_cls, "task_file": file1})
            else:
                result['failure'].append({"err": err,           "task_file": file1})

        for task_file1 in task_files: fix_load(task_file1, result)

        return result

    @staticmethod
    def load_a_task_by_name(task_name_1):
        assert len(Path.all_luiti_tasks_parent_dirs), "at least has a luiti_tasks dir!"

        # clean sys.path
        for idx1, dir1 in enumerate(sys.path):
            sys.path[idx1] = sys.path[idx1].strip("/")
        old_sys_path = sys.path

        # ensure no luiti_tasks dir conflicts
        for dir1 in Path.all_luiti_tasks_parent_dirs:
            if dir1 in sys.path: sys.path.remove(dir1)

        # 1. check `task_name_1` format.
        assert task_name_1[0] in string.uppercase, "Task name should begin with UpperCase !"

        # 2. import it!
        task1      = None

        for dir1 in Path.all_luiti_tasks_parent_dirs:
            sys.path.insert(0, dir1)

            module_name = Path.TasksDir + "." + Inflector().underscore(task_name_1)
            file_path   = os.path.join(dir1, module_name.replace(".", "/") + ".py")

            if os.path.exists(file_path):
                lib1  = importlib.import_module(Path.TasksDir + "." + Inflector().underscore(task_name_1))
                task1 = getattr(lib1, task_name_1)

            sys.path.remove(dir1)
            # clean parent import info
            if Path.TasksDir in sys.modules: del sys.modules[Path.TasksDir]

            if task1 is not None: break

        # reset to orig
        sys.path = old_sys_path

        if task1 is None:
            raise Exception("[luiti] cannot find %s's path" % task_name_1)

        return task1
