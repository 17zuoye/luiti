#-*-coding:utf-8-*-

__all__ = ['HadoopExt']

import luigi.hadoop

class HadoopExt(luigi.hadoop.JobTask):

    n_reduce_tasks    = 1 # 体现在 输出的part-00000数量为reduce数量

    physicalmemory    = 2
    reduce_memory_GB  = 2
    (  map_memory_GB) = 0.6
    iosort_memory_GB  = 0.5


    def __init__(self, *args, **kwargs):
        """ 参考 TaskBase, 确保在 继承时还可以有TaskBase的覆写日期功能。 """
        super(HadoopExt, self).__init__(*args, **kwargs)
        self.orig_date_value = self.orig_date_value or arrow.get(self.date_value)
        self.reset_date()

    def output(self):
        return luigi.HDFS(self.data_file)

    # overwrite
    def job_runner(self):
        return luigi.hadoop.DefaultHadoopJobRunner()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.data_file)

    def jobconfs_opts(self):
        return [
                "fs.defaultFS=hdfs://BJ-NAMENODE-145:8020",
                "mapred.job.tracker=BJ-NAMENODE-145:8031",
                "yarn.resourcemanager.address=BJ-NAMENODE-145:8032",
                "yarn.resourcemanager.scheduler.address=BJ-NAMENODE-145:8030",
                "yarn.resourcemanager.admin.address=BJ-NAMENODE-145:8033",
                "yarn.resourcemanager.webapp.address=BJ-NAMENODE-145:8088",
                "mapreduce.framework.name=yarn",

                # 这些参数好像一直就没怎么有用。
                # NOTE 配置了以下 会导致 Java heap space 错误。
                # "mapreduce.tasktracker.reserved.physicalmemory=%s" % (1024*self.__class__.physicalmemory),
                #"mapreduce.map.java.opts=-Xmx%sM"    % int(1024*self.__class__.map_memory_GB), # 修改为整数, 下同。
                #"mapreduce.reduce.java.opts=-Xmx%sM" % int(1024*self.__class__.reduce_memory_GB),
                #"mapreduce.task.io.sort.mb=%s"       % int(1024*self.__class__.iosort_memory_GB),
                'mapred.reduce.tasks=%s' % self.n_reduce_tasks,
               ]

    def jobconfs(self):
        jcs = super(luigi.hadoop.JobTask, self).jobconfs()
        for conf_opt_1 in self.jobconfs_opts():
            jcs.append(conf_opt_1)
        return jcs



    # TestCase related attrs
    def mrtest_input(self):  raise NotImplemented
    def mrtest_output(self): raise NotImplemented
    def mrtest_attrs(self):  return dict()



# http://doc.mapr.com/display/MapR/mapred-site.xml
"""
mapreduce.framework.name	yarn	Execution framework set to Hadoop YARN.
mapreduce.map.java.opts	-Xmx1024M	Larger heap-size for child jvms of maps.
mapreduce.map.memory.mb	1024	Larger resource limit for maps.
mapreduce.reduce.java.opts	-Xmx2560M	Larger heap-size for child jvms of reduces.
mapreduce.reduce.memory.mb	3072	Larger resource limit for reduces.
mapreduce.reduce.shuffle.parallelcopies	50	Higher number of parallel copies run by reduces to fetch outputs from very large number of maps.
"""
