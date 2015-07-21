# -*-coding:utf-8-*-

__all__ = ["luigi"]

"""
Bind all things to `luigi` root namespace.
"""


import luigi.hdfs
luigi.hdfs = luigi.hdfs  # just make a link

import luigi.hadoop
luigi.hadoop = luigi.hadoop  # just make a ref

from .hadoop_ext import HadoopExt
luigi.hadoop.HadoopExt = HadoopExt  # write back
# NOTE 对 luigi.hadoop 兼容 "track the job: "

luigi.debug = False

luigi.tmp_dir = "/tmp"  # default one

# TODO lazily
from ..utils import TargetUtils
luigi.HDFS = TargetUtils.hdfs  # 本来就是需要读取全局配置，所以索性就绑定在 luigi 命名空间了吧。


from ..manager import luiti_config, active_packages
luigi.ensure_active_packages = lambda: active_packages  # make a wrap
luigi.luiti_config = luiti_config
luiti_config.linked_luigi = luigi
