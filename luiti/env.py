#-*-coding:utf-8-*-

__all__ = ['env']

import os, sys
from etl_utils import singleton, cached_property

@singleton()
class EnvClass(object):
    """ Setup luigi global env. """

    is_setuped = False

    sys_path = []

    def setup(self):
        if self.is_setuped is True:
            return False
        else:
            self.setup_sys_path
            self.is_setuped = True
            return True

    @cached_property
    def setup_sys_path(self):
        assert len(env.sys_path), "[error] Must setup Python sys path at least one path! e.g. `luiti.env.sys_path = ['some_python_library_dir', ...]` "

        # init only once
        if "path_old_by_luiti" in dir(sys):
            raise ValueError("[error] Already setup!")
        else:
            sys.path_old_by_luiti = sys.path
            sys.path     = []
            for path1 in (env.sys_path + sys.path_old_by_luiti):
                if path1 not in sys.path:
                    sys.path.append(path1)

        return sys.path

env = EnvClass()
# TODO 也许挪到 .manager
