#-*-coding:utf-8-*-

import os
import sys

class Path(object):

    TasksDir = "luiti_tasks"

    @staticmethod
    def find_all_luiti_tasks_dirs(project_dir):
        """ return all luiti tasks parent directories. """

        if not os.path.exists(project_dir):
            raise ValueError("%s doesnt exists!" % project_dir)

        if not os.path.exists(os.path.join(project_dir, Path.TasksDir)):
            raise ValueError("%s has no subdir %s) % !" % (project_dir, TasksDir))

        sys.path.insert(0, project_dir)

        return [project_dir]

# TODO find sub modules
