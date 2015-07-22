# -*-coding:utf-8-*-

__all__ = ["TaskWeekHadoop"]

from .task_week import TaskWeek
from ...luigi_extensions import luigi


class TaskWeekHadoop(luigi.hadoop.HadoopExt, TaskWeek):
    pass
