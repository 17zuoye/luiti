#-*-coding:utf-8-*-

import os
import sys
from etl_utils import singleton, cached_property

@singleton()
class PathClass(object):

    TasksDir = "luiti_tasks"
    ProjectDir = os.getcwd()
    enable_ignore = True


    @cached_property
    def all_luiti_tasks_parent_dirs(self):
        """ 自动发现 可以是任何 sys.path, 因为 luigi_tasks 肯定有 __init__.py 文件。  """
        return self.find_all_luiti_tasks_parent_dirs(self.ProjectDir)

    def find_all_luiti_tasks_parent_dirs(self, project_dir):
        """ return all luiti tasks directories. """
        result = []

        if not os.path.exists(project_dir):
            raise ValueError("%s doesnt exists!" % project_dir)

        luiti_tasks_dir = os.path.join(project_dir, Path.TasksDir)
        if not os.path.exists(luiti_tasks_dir):
            raise ValueError("%s has no subdir %s !" % (project_dir, Path.TasksDir))

        # Filter `luiti_tasks` dir when .luitiignore under the same root dir.
        for root, dirs, files in os.walk(project_dir):
            for dir1 in dirs:
                if dir1 == Path.TasksDir:
                    yes = False
                    yes = yes or (root not in result)
                    if Path.enable_ignore:
                        if not os.path.exists(os.path.join(root, ".luitiignore")):
                            yes = True
                    if yes: result.append(root)
        return result

Path = PathClass()
