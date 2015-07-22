# -*-coding:utf-8-*-

import json
import luigi
from etl_utils import singleton, cached_property


@singleton()
class TargetUtilsClass(object):

    def line_read(self, hdfs1):
        with hdfs1.open('r') as data1:
            for line1 in data1:
                line1 = line1.decode("UTF-8").strip()
                # filter blank line
                if len(line1) == 0:
                    continue
                yield line1

    def json_read(self, hdfs1):
        for line1 in TargetUtils.line_read(hdfs1):
            yield json.loads(line1)  # as item1

    def hdfs(self, data_file1):
        # [兼容] 可以判断出 data_file1 是否包含 part-00000 的目录。

        # 兼容 snakebite 对 不存在目录的 test 有bug，或者是因为从hadoop用户切换到primary_user导致。
        f1 = luigi.hdfs.HdfsTarget(data_file1)

        # isdir 在 luigi/hdfs.py 没有实现哦
        is_curr_dir = lambda: len(list(f1.fs.listdir(data_file1))) > 1

        if f1.exists() and is_curr_dir():
            # There's no part-000 when use multiple text output in streaming
            def _exists(name):
                return luigi.hdfs.HdfsTarget(data_file1 + name).exists()
            is_mr_output_root = _exists("/_SUCCESS")
            has_part_000000 = _exists("/part-00000")
            if is_mr_output_root or has_part_000000:
                return luigi.hdfs.HdfsTarget(data_file1,
                                             format=luigi.hdfs.PlainDir)

        return f1

    def hdfs_dir(self, path1):
        """
        Compact with someone use 000000_0 file naming style, but not the default MR part-00000。
        """
        return luigi.hdfs.HdfsTarget(path1, format=luigi.hdfs.PlainDir)

    def isdir(self, path1):
        return self.client.get_bite().test(path1, directory=True)

    def exists(self, path1):
        return self.client.exists(path1)

    @cached_property
    def client(self):
        return HdfsClient.client

TargetUtils = TargetUtilsClass()


@singleton()
class HdfsClientClass(object):
    # TODO use delegate

    @cached_property
    def client(self):
        import luigi.hdfs
        return luigi.hdfs.client
HdfsClient = HdfsClientClass()
TargetUtils.HdfsClient = HdfsClient
