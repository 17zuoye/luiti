#-*-coding:utf-8-*-

import os, sys
import glob
from inflector import Inflector
from .config import luiti_config as lc

def ensure_setup_packages():
    processed_package_names = set([])

    def wrap(orig_func):
        def new_func(*args, **kwargs):
            # 1. Setup env
            global luigi # Fix UnboundLocalError: local variable `luigi` referenced before assignment
            if lc.curr_project_name is None:
                if lc.curr_project_dir is None: lc.curr_project_dir = os.getcwd()
                # Check current work dir is under a valid a luiti_tasks project
                if not os.path.exists(os.path.join( lc.curr_project_dir, "luiti_tasks")):
                    raise ValueError("[error] current work dir [%s] has no luiti_tasks dir!" %  lc.curr_project_dir)

                curr_project_name     = os.path.basename(lc.curr_project_dir) # "project_A"
                curr_project_syspath  = os.path.dirname(lc.curr_project_dir)  # project_A/

                lc.curr_project_name = curr_project_name

                # 1.1. be importable
                if curr_project_syspath not in sys.path: sys.path.insert(0, curr_project_syspath)
                # 1.2. it's the root luiti tasks package
                lc.luiti_tasks_packages.add(PackageMap.Loader.import2(lc.curr_project_name))
                # 1.3. ensure other luiti tasks packages can be loaded.
                # manager.import2(lc.curr_project_name + ".luiti_tasks.__init_luiti")

            loaded_name = lc.curr_project_name + ".luiti_tasks.__init_luiti"
            if loaded_name not in sys.modules:
                PackageMap.Loader.import2(loaded_name) # force load luiti_config.attached_package_names

            # 2. Load related packages.
            import pkg_resources
            import luigi.hadoop
            import re

            for p1 in list(lc.attached_package_names): # fix Set changed size during iteration
                package2, version2 =  re.compile("(^[a-z0-9\_]+)(.*)", re.IGNORECASE).match(p1).groups()
                if package2 in processed_package_names:
                    continue
                else:
                    # Notice Python to import special version package.
                    if version2: pkg_resources.require(p1)

                    # Let luigi know it.
                    luigi.hadoop.attach(package2)

                    # Add valid package which has .luiti_tasks
                    try:
                        if PackageMap.Loader.import2(package2 + ".luiti_tasks"):
                            lc.luiti_tasks_packages.add(PackageMap.Loader.import2(package2)) # .__init_luiti Maybe not exists, so execute this first
                            PackageMap.Loader.import2(package2 + ".luiti_tasks.__init_luiti")
                    except ImportError as ie:
                        pass
                processed_package_names.add(p1)
            #import pdb; pdb.set_trace()
            return orig_func(*args, **kwargs) # call it at last.
        new_func.func_name = orig_func.func_name
        return new_func
    return wrap





from etl_utils import singleton, cached_property



@singleton()
class PackageMapClass(object):

    @cached_property
    @ensure_setup_packages()
    def task_clsname_to_package(self):

        assert lc.luiti_tasks_packages, "At least have one project!"

        result = dict()
        for project1 in lc.luiti_tasks_packages:
            for f2 in glob.glob(os.path.join(project1.__path__[0], "luiti_tasks/[a-z]*.py")):
                task_filename3        = os.path.basename(f2).rsplit(".", 1)[0]
                task_clsname4         = Inflector().classify(task_filename3)
                result[task_clsname4] = project1
        return result

PackageMap = PackageMapClass()



PackageMap.ensure_setup_packages = ensure_setup_packages
