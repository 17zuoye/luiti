#-*-coding:utf-8-*-

import json
import luigi
from etl_utils import singleton, cached_property


class TargetUtils:

    @staticmethod
    def line_read(hdfs1):
        with hdfs1.open('r') as data1:
            for line1 in data1:
                yield line1.decode("UTF-8").strip()

    @staticmethod
    def json_read(hdfs1):
        for line1 in TargetUtils.line_read(hdfs1):
            yield json.loads(line1) # as item1

    @staticmethod
    def hdfs(data_file1):
        # [兼容] 可以判断出 data_file1 是否包含 part-00000 的目录。

        # 兼容 snakebite 对 不存在目录的 test 有bug，或者是因为从hadoop用户切换到primary_user导致。
        f1 = luigi.hdfs.HdfsTarget(data_file1)
        if f1.exists():
            is_curr_dir = len(list(f1.fs.listdir(data_file1))) > 1 # isdir 在 luigi/hdfs.py 没有实现哦

            if is_curr_dir:
                if luigi.hdfs.HdfsTarget(data_file1 + "/part-00000").exists() and \
                    luigi.hdfs.HdfsTarget(data_file1 + "/_SUCCESS").exists():
                    return luigi.hdfs.HdfsTarget(data_file1, format=luigi.hdfs.PlainDir)

        return luigi.hdfs.HdfsTarget(data_file1)

    @staticmethod
    def mr_read(hdfs1):
        from .mr_utils import MRUtils
        for line1 in TargetUtils.line_read(hdfs1):
            k_1, v_1 = MRUtils.split_mr_kv(line1)
            yield k_1, v_1

    @staticmethod
    def isdir(path1): return HdfsClient.client.get_bite().test(path1, directory=True)

    @staticmethod
    def exists(path1): return HdfsClient.client.exists(path1)



@singleton()
class HdfsClientClass(object):
    # TODO use delegate
    @cached_property
    def client(self):
        import luigi.hdfs
        return luigi.hdfs.client
HdfsClient = HdfsClientClass()
TargetUtils.HdfsClient = HdfsClient
