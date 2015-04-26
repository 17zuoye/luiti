# -*-coding:utf-8-*-

__all__ = ['TaskRangeHadoop']

from .task_base import luigi
from .task_range import TaskRange


class TaskRangeHadoop(luigi.hadoop.HadoopExt, TaskRange):

    pass
