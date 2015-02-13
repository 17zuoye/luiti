#-*-coding:utf-8-*-

import os, sys
from inflector    import Inflector
from etl_utils import singleton, cached_property
import arrow

@singleton()
class LuitiConfigClass(object):
    """ Make sure init variables only once. """
    DateTypes          = ["range", "week"] + arrow.Arrow._ATTRS # ['year', 'month', ...]

    curr_project_name = None
    curr_project_dir  = None

    linked_luigi      = None

    @cached_property
    def attached_package_names(self): return set(['luiti'])

    @cached_property
    def luiti_tasks_packages(self): return set([])

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
        lc = luiti_config
        is_in_luigi_distributed = False

        # 1. unmornal task class
        if lc.curr_project_name == "__main__": return False

        # 2. setup current project as root
        if lc.curr_project_dir is None: lc.curr_project_dir = os.getcwd() # auto from current class
        def exists(filename1): return os.path.exists(os.path.join(lc.curr_project_dir, filename1))

        # These files are created by luigi.
        if exists("job-instance.pickle") and exists("job.jar") and exists("packages.tar") and exists("luigi"):
            is_in_luigi_distributed = True

        if lc.curr_project_name is None:
            # Check current work dir is under a valid a luiti_tasks project
            if (not exists("luiti_tasks")) and (not is_in_luigi_distributed):
                raise ValueError(u"""[error] current work dir [%s] has no luiti_tasks dir! It has these files/dirs %s""" % (lc.curr_project_dir,
                           os.listdir(lc.curr_project_dir),))

            if is_in_luigi_distributed:
                for item1 in os.listdir(lc.curr_project_dir):
                    if exists(item1 + "/__init__.py") and exists(item1 + "/luiti_tasks"): # is a valid python package
                        lc.luiti_tasks_packages.add(lc.import2(item1))
            else:
                curr_project_name     = os.path.basename(lc.curr_project_dir) # "project_A"
                lc.curr_project_name  = curr_project_name

                curr_project_syspath  = os.path.dirname(lc.curr_project_dir)  # project_A/
                if curr_project_syspath not in sys.path: sys.path.insert(0, curr_project_syspath)

                lc.luiti_tasks_packages.add(lc.import2(lc.curr_project_name))

                # 3. ensure other luiti tasks packages can be loaded.
                lc.import2(lc.curr_project_name + ".luiti_tasks.__init_luiti")


luiti_config = LuitiConfigClass()
