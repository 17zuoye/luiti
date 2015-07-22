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
    def json_dump(o1):
        m1 = lambda item1: json.dumps(list(item1))
        m2 = lambda item1: JsonUtils.unicode_dump(item1).encode("UTF-8")
        if isinstance(o1, (list, set,)):
            # Comptible with JsonUtils.unicode_dump dont support list
            method = m1
        else:
            method = m2
        return method(o1)

    @staticmethod
    def write_json_to_output(result, output1):
        """
        Support multiple lines.
        """
        if isinstance(result, dict):
            result = [result]
        if isinstance(result, set):
            result = list(result)
        assert isinstance(result, list), result
        assert len(result) > 0, result
        assert isinstance(result[0], dict), result

        with output1.open('w') as output_hdfs:
            for o1 in result:
                output_hdfs.write(IOUtils.json_dump(o1) + "\n")
        return 0
    write_jsons_to_output = write_json_to_output  # make a alias

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
        return True

    @staticmethod
    def local_target(path1):
        return LocalTarget(path1)
