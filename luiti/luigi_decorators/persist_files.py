# -*-coding:utf-8-*-

__all__ = ["persist_files"]

import os
from luigi import Event
from ..utils import IOUtils


# NOTE deprecated
def persist_files(*files):  # 装饰器
    """ 多个data_file 可以用 DSL 描述，然后和 event_handler(Event.FAILURE) 绑定在一起 """
    def func(cls):
        # 1. 设置 持久化文件属性
        def wrap(file1):  # 这样才可以保存 file1 变量，而不至于被覆写。
            def _file(self):
                return os.path.join(self.data_dir, file1 + ".json")
            return _file

        setattr(cls, "__persist_files", files)
        for file1 in getattr(cls, "__persist_files"):
            setattr(cls, file1, property(wrap(file1)))  # @decorator

        # 2. 绑定 失败时删除这些文件
        def clean_tmp(task, exception):
            for file1 in files:
                IOUtils.remove_files(getattr(task, file1))
            # IOUtils.remove_files(task.data_file)
            # NOTE 好像 Hadoop 会自动处理失败任务的输出文件的，否则就会导致其在N次重试一直在running。
        cls.event_handler(Event.FAILURE)(clean_tmp)

        return cls

    return func
