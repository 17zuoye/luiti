#-*-coding:utf-8-*-

import os, glob
from .command_utils import CommandUtils
from .hdfs_utils    import HDFSUtils

class CompressUtils:

    @staticmethod
    def unzip_with_upload(orig_filepath, hdfs_filepath, tmp_dir=None, tmp_name=None):
        """ """
        # 1. check
        if not HDFSUtils.exists(orig_filepath):
            raise ValueError("[hdfs] %s not exists!" % orig_filepath)

        # 2. pull file from hdfs
        tmp_local_target = tmp_dir + "/" + tmp_name
        HDFSUtils.copyToLocal(orig_filepath, tmp_local_target)

        # 3. unzip
        unzip_dir = tmp_dir + "/unzip"
        CommandUtils.execute("mkdir -p %s" % unzip_dir)
        CommandUtils.execute("tar xzvf %s -C %s" % (tmp_local_target, unzip_dir))

        unzip_file = unzip_dir
        # 兼容 zip 文件是多层级目录
        while ( os.path.isdir(unzip_file) ):
            next_dirs = glob.glob(unzip_file + "/*")
            if len(next_dirs) >  1: raise ValueError("%s should only one dir in a zip file!" % unzip_file)
            if len(next_dirs) == 0: raise ValueError("%s must always exists one file or one dir in a zip file, but there are %s ." % (unzip_file, str(next_dirs)))
            unzip_file = next_dirs[0]

        # 4. push file to hdfs
        HDFSUtils.copyFromLocal(unzip_file, hdfs_filepath)
        CommandUtils.execute("rm -rf %s" % unzip_dir)
        CommandUtils.execute("rm -rf %s" % tmp_local_target)
