#-*-coding:utf-8-*-

__all__ = ['HadoopExt']

import luigi.hadoop
from .utils import ExtUtils, TargetUtils
from .parameter import ArrowParameter


class LuitiHadoopJobRunner(luigi.hadoop.HadoopJobRunner):
    """ overwrite DefaultHadoopJobRunner.class """

    # params are copied from HadoopJobRunner
    def __init__(self, libjars=None, output_format=None):
        config = luigi.hadoop.configuration.get_config()
        opts = {
                "streaming_jar"         : config.get('hadoop', 'streaming-jar'),
                "output_format"         : output_format,
                "libjars"               : libjars,
            }
        super(LuitiHadoopJobRunner, self).__init__(**opts)


class HadoopExt(luigi.hadoop.JobTask, ExtUtils.ExtendClass):

    run_mode           = "mr_distribute"

    n_reduce_tasks    = 1 # 体现在 输出的part-00000数量为reduce数量

    physicalmemory    = 2
    reduce_memory_GB  = 2
    (  map_memory_GB) = 0.6
    iosort_memory_GB  = 0.5

    output_format     = [
                            #"org.apache.hadoop.mapreduce.lib.output.TextOutputFormat",         # 单路输出。这个版本有问题。
                            "org.apache.hadoop.mapred.TextOutputFormat",                        # 单路输出
                            "org.apache.hadoop.mapred.lib.MultipleTextOutputFormat",            # 多路输出
                        ][0]                                                                    # 默认是 单路输出
    libjars           = []


    def __init__(self, *args, **kwargs):
        """ 参考 TaskBase, 确保在 继承时还可以有TaskBase的覆写日期功能。 """
        super(HadoopExt, self).__init__(*args, **kwargs)
        self.orig_date_value = self.orig_date_value or ArrowParameter.get(self.date_value)
        self.reset_date()

    def output(self):
        return luigi.HDFS(self.data_file)

    # overwrite
    def job_runner(self):
        """ will be wraped in `run` function. """
        return LuitiHadoopJobRunner(output_format=self.output_format, libjars=self.libjars)

    def output(self):
        return TargetUtils.hdfs(self.data_file)

    def jobconfs_opts(self):
        return [
                "mapreduce.framework.name=yarn",
                'mapred.reduce.tasks=%s' % self.n_reduce_tasks,
               ]

    def jobconfs(self):
        jcs = super(luigi.hadoop.JobTask, self).jobconfs()
        for conf_opt_1 in self.jobconfs_opts():
            jcs.append(conf_opt_1)
        return jcs



    # TestCase related attrs
    def mrtest_input(self):
        raise NotImplementedError

    def mrtest_output(self):
        raise NotImplementedError

    def mrtest_attrs(self):
        return dict()
