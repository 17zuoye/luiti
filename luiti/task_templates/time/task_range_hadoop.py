# -*-coding:utf-8-*-

__all__ = ['TaskRangeHadoop']

from .task_range import TaskRange
from ...luigi_extensions import luigi


class TaskRangeHadoop(luigi.hadoop.HadoopExt, TaskRange):

    pass
