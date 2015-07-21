# -*-coding:utf-8-*-

from .task_quarter import TaskQuarter
from ...luigi_extensions import luigi


class TaskQuarterHadoop(luigi.hadoop.HadoopExt, TaskQuarter):

    pass
