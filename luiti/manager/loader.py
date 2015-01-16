#-*-coding:utf-8-*-

import os
import glob
from inflector import Inflector
from .path import Path

class Loader(object):

    @staticmethod
    def load_all_tasks(*luiti_tasks_dirs):
        from ..utils import TaskUtils

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
                task_cls    = TaskUtils.load_task(classname1)
                is_success  = True
            except Exception as e:
                err = e

            if is_success:
                result['success'].append({"task_cls": task_cls, "task_file": file1})
            else:
                result['failure'].append({"err": err,           "task_file": file1})

        for task_file1 in task_files: fix_load(task_file1, result)

        return result
