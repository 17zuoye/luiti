#-*-coding:utf-8-*-

import os, sys
from .config import luiti_config as lc

processed_package_names = set([])

def active_packages(orig_func):
    """
    called by `PackageMap.task_clsname_to_package`
    """
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
                # Pip cant manage versions packages, only exist one version at one time.
                try:
                    if version2: pkg_resources.require(p1)
                except:
                    pkg_resources.require(package2)

                # TODO luiti 拷之前需要版本，之后不需要，分布式时判断目录packages即可。
                # Notice Python to import special version package.
                # if version2: pkg_resources.require(p1)

                # Let luigi know it.
                package2_lib = lc.import2(package2)
                luigi.hadoop.attach(package2_lib)

                # Add valid package which has .luiti_tasks
                #   compact with package with a plain python file.
                try:
                    path = (package2_lib.__path__ + [""])[0]
                except:
                    print "[package2_lib load error]", package2_lib
                    path = "/package/load/error"
                # TODO 兼容 egg zip 格式，看看里面有没有 luiti_tasks 文件，然后提示加 zip_safe=False
                if os.path.exists(path + "/luiti_tasks"):
                    # .__init_luiti Maybe not exists, so execute this first
                    lc.luiti_tasks_packages.add(package2_lib)
            processed_package_names.add(p1)
        return orig_func(*args, **kwargs) # call it at last.
    new_func.func_name = orig_func.func_name
    return new_func
