#-*-coding:utf-8-*-

import json
import luigi, luigi.hdfs
from luigi import LocalTarget
from etl_utils import JsonUtils

class IOUtils:

    SQL_RANGE_LIMIT = 1000

    @staticmethod
    def write_json_to_output(json1, output1):
        with output1.open('w') as output_hdfs:
            if isinstance(json1, (list,set,)):
                method = lambda item1: json.dumps(list(item1)) # 兼容 JsonUtils.unicode_dump 不支持list
            else:
                method = lambda item1: JsonUtils.unicode_dump(item1).encode("UTF-8")
            output_hdfs.write(method(json1) + "\n")

    @staticmethod
    def read_json_from_output(output1):
        # only one line
        item1 = None
        with output1.open('r') as output_hdfs:
            for line1 in output_hdfs:
                item1 = json.loads(line1)
        return item1

    @staticmethod
    def remove_files(*files): # 兼容 写入中途失败
        for file1 in files:
            if luigi.hdfs.exists(file1):
                luigi.hdfs.remove(file1)

    @staticmethod
    def local_target(path1): return LocalTarget(path1)
