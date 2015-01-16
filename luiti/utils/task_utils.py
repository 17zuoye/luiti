#-*-coding:utf-8-*-

import os
import sys
import string
import importlib
from inflector import Inflector
from ..manager import Path

class TaskUtils:

    @staticmethod
    def load_task(task_name_1):
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

        if task1 is None:
            raise Exception("[luiti] cannot find %s's path" % task_name_1)

        # reset to orig
        sys.path = old_sys_path

        return task1
