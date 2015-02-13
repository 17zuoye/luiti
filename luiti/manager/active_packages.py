#-*-coding:utf-8-*-

import os, sys
from .config import luiti_config as lc

processed_package_names = set([])

def active_packages(orig_func):
    def new_func(*args, **kwargs):
        # 1. Setup env
        lc.link_packages()

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
                package2_lib = lc.import2(package2)
                luigi.hadoop.attach(package2_lib)

                # Add valid package which has .luiti_tasks
                if os.path.exists(package2_lib.__path__[0] + "/luiti_tasks"):
                    # .__init_luiti Maybe not exists, so execute this first
                    lc.luiti_tasks_packages.add(package2_lib)
            processed_package_names.add(p1)
        return orig_func(*args, **kwargs) # call it at last.
    new_func.func_name = orig_func.func_name
    return new_func
