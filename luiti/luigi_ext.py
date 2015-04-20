#-*-coding:utf-8-*-

__all__ = ['luigi']

import luigi
from luigi import Event
import luigi.hadoop
luigi.hadoop = luigi.hadoop
import luigi.hdfs
luigi.hdfs = luigi.hdfs

from .hadoop_ext import HadoopExt
luigi.hadoop.HadoopExt = HadoopExt # write back
# NOTE 对 luigi.hadoop 兼容 "track the job: "


import os, sys
import arrow
from .parameter import ArrowParameter
import importlib
from inflector import Inflector
from etl_utils import cached_property, singleton
from collections import defaultdict

from .utils import IOUtils, TargetUtils
from . import manager
luigi.HDFS = TargetUtils.hdfs # 本来就是需要读取全局配置，所以索性就绑定在 luigi 命名空间了吧。

def persist_files(*files): # 装饰器
    """ 多个data_file 可以用 DSL 描述，然后和 event_handler(Event.FAILURE) 绑定在一起 """
    def func(cls):
        # 1. 设置 持久化文件属性
        def wrap(file1): # 这样才可以保存 file1 变量，而不至于被覆写。
            def _file(self):
                return os.path.join(self.data_dir, file1 + ".json")
            return _file

        cls.__persist_files = files
        for file1 in cls.__persist_files:
            setattr(cls, file1, property(wrap(file1))) # @decorator

        # 2. 绑定 失败时删除这些文件
        def clean_tmp(task, exception):
            for file1 in files: IOUtils.remove_files(getattr(task, file1))
            # IOUtils.remove_files(task.data_file) # NOTE 好像 Hadoop 会自动处理失败任务的输出文件的，否则就会导致其在N次重试一直在running。
        cls.event_handler(Event.FAILURE)(clean_tmp)

        return cls

    return func
luigi.persist_files = persist_files



def ref_tasks(*tasks): # 装饰器
    """
    自动把依赖 Task 链接起来，通过属性访问。

    Example:

    ```python
    @ref_tasks("TaskB", "TaskC")
    class TaskA(TaskWeekBase):
        pass

    TaskA().TaskB == TaskB
    TaskA().TaskC == TaskC
    ```
    """
    def wrap_cls(ref_task_name):
        def _func(self):
            v1 = self.__dict__.get(ref_task_name, None)
            if v1 is None:
                v1 = manager.load_a_task_by_name(ref_task_name)
                self.__dict__[ref_task_name] = v1
            return v1
        return _func

    def wrap_instance(ref_task_name, task_name):
        def _func(self):
            v1 = self.__dict__.get(task_name, None)
            if v1 is None:
                v1 = getattr(self, ref_task_name)(self.date_value)
                self.__dict__[task_name] = v1
            return v1
        return _func

    def __getstate__(self):
        """ Fix luiti_tasks module namespace conflicts. """
        for ref_task1 in self._ref_tasks:
            cname = ref_task1           # class    name
            iname = ref_task1 + "_task" # instance name

            # delete instance property is enough.
            #if hasattr(self.__class__, cname):  delattr(self.__class__, cname)
            #if hasattr(self.__class__, iname):  delattr(self.__class__, iname)

            if cname in self.__dict__:          del self.__dict__[cname]
            if iname in self.__dict__:          del self.__dict__[iname]
        return self.__dict__

    def __setstate__(self, d1):
        # 1. default
        self.__dict__.update(d1)
        # 2. plug other package in `.__init_luiti`
        luigi.luiti_config.curr_project_name = self.package_name
        luigi.luiti_config.link_packages()

# cached_property 捕获不了 ref_task_name 变量, 被重置为某一个了。。
# property 可以捕获 ref_task_name 变量。
    def func(cls):
        cls._ref_tasks = tasks
        for ref_task_name in cls._ref_tasks:
            setattr(cls, ref_task_name, property(wrap_cls(ref_task_name)))

            # TODO 根据当前日期返回。
            task_name = "%s_%s" % (ref_task_name, "task")
            setattr(cls, task_name, property(wrap_instance(ref_task_name, task_name)))

            # clear ref task info when pickle.dump
            setattr(cls, "__getstate__", __getstate__)
        return cls
    return func
luigi.ref_tasks = ref_tasks



def check_date_range(): # 装饰器
    """
    从数据库导数据时，必须注意时间范围内的所有数据是否都齐全了。如果未齐全，
    即在当前时间范围里导的话，那么就会缺失数据了，相当于提前导了。

    比如在周六就把这周的关联数据导出来，那么周日的数据就没包含在里面。应该在下周一后才开始导。
    """
    def decorator(orig_run):
        def new_run(self):
            # 说明时间未到，然后就直接退出
            if self.date_value_by_type_in_begin <= ArrowParameter.now() < self.date_value_by_type_in_end:
                return False
            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
# TODO support Hadoop
luigi.check_date_range = check_date_range


def check_runtime_range(**opts_1): # 装饰器
    """
    Support hour/weekday indexed range.

    Optional params:
    1. hour_num
    2. weekday_num
    3. now
    """
    def decorator(orig_run):
        def new_run(self):
            default_opts = {
                    "hour_num"      : range(1, 25),
                    "weekday_num"   : range(1, 8),
                }
            opts = dict(default_opts.items() + opts_1.items())

            now            = ArrowParameter.now()           # get current time
            hour_24        = int(now.format("H"))  # 0, 1, 2, ..., 23, 24
            day_of_week_7  = int(now.format("d"))  # 1, 2, 3, ..., 6, 7

            is_false = False
            if hour_24       not in opts['hour_num']:    is_false = True
            if day_of_week_7 not in opts['weekday_num']: is_false = True
            if is_false:
                print "[info]", now, " is not in ", opts, ", so the task exited."
                return False

            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
luigi.check_runtime_range = check_runtime_range



def mr_local(**opts):
    """
    Sometimes Hadoop streaming sucks, so we only use the solid HDFS, and turn MapReduce job into local mode.

    And `mr_local` is optimized by a fixed chunk write operation.
    """

    def mr_run(self):
        """ Overwrite BaseHadoopJobTask#run function. """
# TODO maybe model cache
        from etl_utils import process_notifier
        map_kv_dict = defaultdict(list)

        inputs = self.input()
        if not isinstance(inputs, list): inputs = [inputs]
        for input_hdfs_1 in inputs:
            for line2 in TargetUtils.line_read(input_hdfs_1):
                for map_key_3, map_val_3 in self.mapper(line2):
                    map_kv_dict[map_key_3].append(map_val_3)

        with self.output().open("w") as output1:
            fixed_chunk = list()
            for reduce_key_2 in process_notifier(map_kv_dict.keys()):
                reduce_vals_2 = map_kv_dict[reduce_key_2]
                for _, reduce_val_2 in self.reducer(reduce_key_2, reduce_vals_2):
                    fixed_chunk.append(reduce_val_2)

                    if len(fixed_chunk) % self.chunk_size == 0:
                        output1.write("\n".join(fixed_chunk) + "\n")
                        fixed_chunk = list()
                del map_kv_dict[reduce_key_2]
            output1.write("\n".join(fixed_chunk) + "\n")


    def wrap(cls):
        cls.run = mr_run
        cls.run_mode = "mr_local"

        opts["chunk_size"] = opts.get("chunk_size", 100)
        for k1, v1 in opts.iteritems():
            setattr(cls, k1, v1)

        return cls
    return wrap
luigi.mr_local = mr_local # bind it.





def plug_packages(*package_names):
    """
    Let luigi know which packages should be attached, and can send to YARN, etc.

    Package format can be any valid Python package name, such as "project_B" or "project_C==0.0.2", etc.

    Usage: use `active_packages` decorator to notice luigi that these packages should include.
    """
    #if len(manager.luiti_config.attached_package_names) > 1:
    #    return False # default is luiti. and can plug only once.

    for p1 in package_names:
        # load all packages's depended pacakges.
        manager.luiti_config.attached_package_names.add(p1)
# TODO why should do `luigi.hadoop.attach` in `active_packages`
luigi.plug_packages = plug_packages



orig_create_packages_archive = luigi.hadoop.create_packages_archive
def create_packages_archive_with_support_egg(packages, filename):
    """
    Fix original luigi's `create_packages_archive` cannt attach egg packages
    (zip file type) to tarfile, Cause it's coping file mechanism by absolute
    path.
    """
    # 1. original create tar file
    orig_create_packages_archive(packages, filename)

    # 2. append python egg packages that 1. not covered
    import tarfile
    tar = tarfile.open(filename, "a")

    logger = luigi.hadoop.logger
    fake_exists_path = "/" # root is awlays exists
    def get_parent_zip_file_within_absolute_path(path1):
        path2      = path1[:]
        is_success = False
        while path2 != fake_exists_path:
            path2 = os.path.dirname(path2)
            if os.path.isfile(path2):
                is_success = True
                break
        return is_success, path2

    def add(src, dst):
        logger.debug('adding to tar: %s -> %s', src, dst)
        tar.add(src, dst)

    import zipfile
    import tempfile
    for package1 in packages:
        path2 = (getattr(package1, "__path__", []) + [fake_exists_path])[0]
        if os.path.exists(path2):     continue # so luigi can import it.
        if not path2.startswith("/"): continue # we only care about libraries.

        is_success, zipfilename3 = get_parent_zip_file_within_absolute_path(path2)
        if is_success:
            tmp_dir3 = tempfile.mkdtemp()
            zipfile.ZipFile(zipfilename3).extractall(tmp_dir3)

            for root4, dirs4, files4 in os.walk(tmp_dir3):
                curr_dir5 = os.path.basename(root4)
                for file5 in files4:
                    if file5.endswith(".pyc"): continue
                    add(os.path.join(root4, file5), os.path.join(root4.replace(tmp_dir3, "").lstrip("/"), file5))

    client_cfg = os.path.join(os.getcwd(), "client.cfg")
    if os.path.exists(client_cfg):
        tar.add(client_cfg, "client.cfg")
    tar.close()
luigi.hadoop.create_packages_archive = create_packages_archive_with_support_egg




luigi.ensure_active_packages = lambda : manager.active_packages # make a wrap
luigi.luiti_config = manager.luiti_config
manager.luiti_config.linked_luigi = luigi
