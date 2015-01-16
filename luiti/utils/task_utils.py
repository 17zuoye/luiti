#-*-coding:utf-8-*-

import importlib
from inflector import Inflector

class TaskUtils:

    @staticmethod
    def load_task(task_name_1):
        # 1. check `task_name_1` format.
        import string
        assert task_name_1[0] in string.uppercase, "Task name should begin with UpperCase !"

        # 2. import it!
        lib1  = importlib.import_module(Inflector().underscore(task_name_1))
        task1 = getattr(lib1, task_name_1)
        return task1
