# -*-coding:utf-8-*-

import os
import sys
from inflector import Inflector
from etl_utils import singleton, cached_property
import arrow


@singleton()
class LuitiConfigClass(object):

    """ Make sure init variables only once. """
    DateTypes = ["range", "week"] + arrow.Arrow._ATTRS
    #           ['year', 'month', ...]

    curr_project_name = None
    curr_project_dir = None

    linked_luigi = None

    @cached_property
    def attached_package_names(self):
        return set(['luiti'])

    @cached_property
    def luiti_tasks_packages(self):
        return set([])

    @staticmethod
    def import2(a_package):
        return __import__(a_package, None, None, 'non_empty')

    @staticmethod
    def get_date_type(name1):
        """ Inherit class must be in TaskBase{Day,Week,Month,Range} style.  """
        assert isinstance(name1, (str, unicode))
        str1 = Inflector().underscore(name1).split("_")[-1].lower()
        assert str1 in luiti_config.DateTypes
        return str1

    @staticmethod
    def get_time_task(name1):
        """ return e.g. TaskDay """
        type2 = luiti_config.get_date_type(name1)
        return "Task" + Inflector().camelize(type2)

    @staticmethod
    def link_packages():
        """
        called by `active_packages`
        """
        is_in_luigi_distributed = False

        # 1. unmornal task class
        if luiti_config.curr_project_name == "__main__":
            return False

        # 2. setup current project as root
        if luiti_config.curr_project_dir is None:
            luiti_config.curr_project_dir = os.getcwd()  # auto from current class
        luiti_config.fix_project_dir()

        def exists(filename1):
            return os.path.exists(os.path.join(luiti_config.curr_project_dir, filename1))

        # These files are created by luigi.
        if exists("job-instance.pickle") and exists("job.jar") and \
                exists("packages.tar") and exists("luigi"):
            is_in_luigi_distributed = True

        # compact with no-luiti project
        is_a_luiti_project = exists("luiti_tasks")

        if luiti_config.curr_project_name is None:
            if is_in_luigi_distributed:
                for item1 in os.listdir(luiti_config.curr_project_dir):
                    # is a valid python package
                    if exists(item1 + "/__init__.py") and \
                            exists(item1 + "/luiti_tasks"):
                        luiti_config.luiti_tasks_packages.add(luiti_config.import2(item1))
            else:
                # "project_A"
                curr_project_name = luiti_config.get_curr_project_name()
                luiti_config.curr_project_name = curr_project_name

                # project_A/
                curr_project_syspath = os.path.dirname(luiti_config.curr_project_dir)
                if curr_project_syspath not in sys.path:
                    sys.path.insert(0, curr_project_syspath)

                luiti_config.luiti_tasks_packages.add(luiti_config.import2(luiti_config.curr_project_name))

                # 3. ensure other luiti tasks packages can be loaded.
                if is_a_luiti_project:
                    luiti_config.import2(
                        luiti_config.curr_project_name + ".luiti_tasks.__init_luiti")

    def get_curr_project_path(self):
        curr_package_name = self.get_curr_project_name()
        curr_path = luiti_config.curr_project_dir
        dir1 = curr_path.rstrip("/")
        if dir1.split("/").count(curr_package_name) == 2:
            dir1 = os.path.dirname(dir1)
        return dir1

    def get_curr_project_name(self):
        """ a valid Python package path. """
        assert isinstance(luiti_config.curr_project_dir, str), luiti_config.curr_project_dir
        return os.path.basename(luiti_config.curr_project_dir)

    def fix_project_dir(self):
        """ Fix project_A/project_A/luiti_tasks dir """
        _try_dir = os.path.join(
            luiti_config.curr_project_dir,
            os.path.basename(luiti_config.curr_project_dir))
        if os.path.exists(_try_dir):  # cause of the same name
            luiti_config.curr_project_dir = _try_dir


luiti_config = LuitiConfigClass()
