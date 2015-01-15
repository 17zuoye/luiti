#-*-coding:utf-8-*-

import os
from .command_utils import CommandUtils


class HDFSUtils:

    @staticmethod
    def exists(path1):
        """ e.g. /server/hadoop-2.0.0-cdh4.5.0/bin/hdfs dfs  -Dfs.defaultFS=hdfs://10.0.1.145:8020/  -cp /user/hadoop/questions_results/english_exam/20141113.json /primary/question_result/en_exam/en_exam_20141113.json """
        return CommandUtils.execute(HDFSUtils.hdfs_cli + " -test -e " + path1)[0] == 0 # 比较返回状态码

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

HDFSUtils.hdfs_cli      = "/server/hadoop-2.0.0-cdh4.5.0/bin/hdfs dfs  -Dfs.defaultFS=hdfs://10.0.1.145:8020/ "

# TODO 用装饰器来包装 print, CommandUtils.execute等
