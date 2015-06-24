# -*-coding:utf-8-*-

from __future__ import print_function

__all__ = ['HadoopExt']

import sys
import luigi.hadoop
from luigi.hadoop import flatten
from itertools import groupby

from .utils import ExtUtils, TargetUtils
from etl_utils import cached_property
from .luigi_extensions import TaskInit

# See benchmark at https://gist.github.com/mvj3/02dca2bcc8b0ef1bbfb5
# force to use faster ujson, or it's meaningless to use JSON format with no performance gained.
import ujson as json
import jsonpickle


class LuitiHadoopJobRunner(luigi.hadoop.HadoopJobRunner):
    """ overwrite DefaultHadoopJobRunner.class """

    # params are copied from HadoopJobRunner
    def __init__(self, libjars=None, output_format=None):
        config = luigi.hadoop.configuration.get_config()
        opts = {
            "streaming_jar": config.get('hadoop', 'streaming-jar'),
            "output_format": output_format,
            "libjars": libjars,
        }
        super(LuitiHadoopJobRunner, self).__init__(**opts)


DataInterchange = {
    "python": {"serialize": str,
               "internal_serialize": repr,
               "deserialize": eval},
    "json": {"serialize": json.dumps,
             "internal_serialize": json.dumps,
             "deserialize": json.loads},
    "jsonpickle": {"serialize": jsonpickle.dumps,
                   "internal_serialize": jsonpickle.dumps,
                   "deserialize": jsonpickle.loads}
}


class HadoopExt(luigi.hadoop.JobTask, ExtUtils.ExtendClass):

    # available formats are "python" and "json".
    data_interchange_format = "python"

    @cached_property
    def serialize(self):
        return DataInterchange[self.data_interchange_format]['serialize']

    @cached_property
    def internal_serialize(self):
        return DataInterchange[self.data_interchange_format]['internal_serialize']

    @cached_property
    def deserialize(self):
        return DataInterchange[self.data_interchange_format]['deserialize']

    def writer(self, outputs, stdout, stderr=sys.stderr):
        """
        Writer format is a method which iterates over the output records
        from the reducer and formats them for output.

        The default implementation outputs tab separated items.
        """
        for output in outputs:
            try:
                output = flatten(output)
                if self.data_interchange_format == "json":
                    # Only dump one json string, and skip another one, maybe key or value.
                    output = filter(lambda x: x not in ["", None], output)
                else:
                    # JSON is already serialized, so we put `self.serialize` in a else statement.
                    output = map(self.serialize, output)
                print("\t".join(map(str, output)), file=stdout)
            except:
                print(output, file=stderr)
                raise

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        """
        Iterate over input, collect values with the same key, and call the reducer for each unique key.
        """
        for key, values in groupby(inputs, key=lambda x: self.internal_serialize(x[0])):
            for output in reducer(self.deserialize(key), (v[1] for v in values)):
                yield output
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def internal_reader(self, input_stream):
        """
        Reader which uses python eval on each part of a tab separated string.
        Yields a tuple of python objects.
        """
        for input_line in input_stream:
            yield list(map(self.deserialize, input_line.split("\t")))

    def internal_writer(self, outputs, stdout):
        """
        Writer which outputs the python repr for each item.
        """
        for output in outputs:
            print("\t".join(map(self.internal_serialize, output)), file=stdout)

    run_mode = "mr_distribute"
    n_reduce_tasks = 1  # 体现在 输出的part-00000数量为reduce数量

    output_format = [
        # 单路输出。这个版本有问题。
        # "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat",
        "org.apache.hadoop.mapred.TextOutputFormat",  # 单路输出
        "org.apache.hadoop.mapred.lib.MultipleTextOutputFormat",  # 多路输出
    ][0]  # 默认是 单路输出
    output_format_default = output_format[:]
    libjars = []

    def __init__(self, *args, **kwargs):
        """ 参考 TaskBase, 确保在 继承时还可以有TaskBase的覆写日期功能。 """
        super(HadoopExt, self).__init__(*args, **kwargs)
        TaskInit.setup(self)

    # overwrite
    def job_runner(self):
        """ will be wraped in `run` function. """
        # Auto compile java code
        if self.output_format != self.output_format_default:
            self.compile_java_code()

        return LuitiHadoopJobRunner(
            output_format=self.output_format, libjars=self.libjars)

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

    def reader(self, input_stream):
        """
        Overwrite luigi, skip blank line
        """
        for line in input_stream:
            line = line.strip()
            if line:
                yield line,
