#-*-coding:utf-8-*-


from ...luigi_ext import luigi
from ...utils import TargetUtils


class StaticFile(luigi.Task):
    filepath = None

    # Mimic default luigi.ExternalTask
    def run(self):
        pass

    def complete(self):
        return True

    def output(self):
        assert self.filepath, u"Please assign `filepath` !"
        return TargetUtils.hdfs(self.filepath)
