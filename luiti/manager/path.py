#-*-coding:utf-8-*-

import os
import sys

class Path(object):

    TasksDir = "luiti_tasks"

    all_luiti_tasks_parent_dirs = []

    @staticmethod
    def find_all_luiti_tasks_parent_dirs(project_dir):
        """ return all luiti tasks directories. """

        if not os.path.exists(project_dir):
            raise ValueError("%s doesnt exists!" % project_dir)

        luiti_tasks_dir = os.path.join(project_dir, Path.TasksDir)
        if not os.path.exists(luiti_tasks_dir):
            raise ValueError("%s has no subdir %s !" % (project_dir, Path.TasksDir))

        for root, dirs, files in os.walk(project_dir):
            for dir1 in dirs:
                if dir1 == Path.TasksDir:
                    Path.all_luiti_tasks_parent_dirs.append(root)

        return Path.all_luiti_tasks_parent_dirs
