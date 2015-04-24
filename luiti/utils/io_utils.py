# -*-coding:utf-8-*-

import json
import luigi
import luigi.hdfs
from luigi import LocalTarget
from etl_utils import JsonUtils
from .target_utils import TargetUtils


class IOUtils:

    SQL_RANGE_LIMIT = 1000

    @staticmethod
    def write_json_to_output(json1, output1):
        with output1.open('w') as output_hdfs:
            m1 = lambda item1: json.dumps(list(item1))
            m2 = lambda item1: JsonUtils.unicode_dump(item1).encode("UTF-8")
            if isinstance(json1, (list, set,)):
                # 兼容 JsonUtils.unicode_dump 不支持list
                method = m1
            else:
                method = m2
            output_hdfs.write(method(json1) + "\n")

    @staticmethod
    def read_json_from_output(output1):
        # only one line
        item1 = None
        read_line_count = 0
        for json1 in TargetUtils.json_read(output1):
            read_line_count += 1
            item1 = json1
            if read_line_count >= 2:
                raise ValueError("[multiple line error]"
                                 " %s should contain only one line!" % output1)
        return item1

    @staticmethod
    def remove_files(*files):  # 兼容 写入中途失败
        for file1 in files:
            if luigi.hdfs.exists(file1):
                luigi.hdfs.remove(file1)

    @staticmethod
    def local_target(path1):
        return LocalTarget(path1)
