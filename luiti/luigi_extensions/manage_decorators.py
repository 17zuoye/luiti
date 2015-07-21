# -*-coding:utf-8-*-

import os
import glob


class ManageDecorators(object):

    @staticmethod
    def bind_to(luigi):
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        decorator_dir = os.path.join(root_dir, "luigi_decorators")
        files = glob.glob(os.path.join(decorator_dir, "*.py"))

        # The decorator name Must as the same as the filename.
        decorator_names = map(lambda i1: i1.split("/")[-1].split(".")[0], files)
        decorator_names = filter(lambda i1: not i1.startswith("__"), decorator_names)
        assert len(decorator_names) > 0, decorator_names

        for name in decorator_names:
            try:
                mod = __import__("luiti.luigi_decorators." + name, fromlist=[name])
            except ImportError:
                print "[Import error decorator name]", name
                exit()
            func = getattr(mod, name)
            setattr(luigi, name, func)

        return luigi
