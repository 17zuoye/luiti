#-*-coding:utf-8-*-

import os
from .command_utils import CommandUtils
from .target_utils  import TargetUtils


class HDFSUtils:

    hdfs_cli = NotImplemented

    @staticmethod
    def exists(path1):
        return TargetUtils.exists(path1)

    @staticmethod
    def copy(path1, path2):
        command1 = HDFSUtils.hdfs_cli + " -cp %s %s" % (path1, path2)
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def copyFromLocal(path1, path2):
        command1 = HDFSUtils.hdfs_cli + " -copyFromLocal %s %s" % (path1, path2)
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def copyToLocal(path1, path2):
        command1 = HDFSUtils.hdfs_cli + " -copyToLocal %s %s" % (path1, path2)
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def chown(path1):
        command1 = HDFSUtils.hdfs_cli + " -chown -R primary_user " + path1
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def mkdir_p(dir1):
        command1 = HDFSUtils.hdfs_cli + " -mkdir -p " + dir1
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def mkdir(dir1):
        command1 = HDFSUtils.hdfs_cli + " -mkdir " + dir1
        print "[command]", command1
        CommandUtils.execute(command1)

    @staticmethod
    def mv(src, dst):
        command1 = HDFSUtils.hdfs_cli + (" -mv %s %s " % (src, dst))
        print "[command]", command1
        CommandUtils.execute(command1)


# TODO 用装饰器来包装 print, CommandUtils.execute等
