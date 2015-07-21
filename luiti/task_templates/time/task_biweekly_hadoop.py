# -*-coding:utf-8-*-

from .task_biweekly import TaskBiweekly
from ...luigi_extensions import luigi


class TaskBiweeklyHadoop(luigi.hadoop.HadoopExt, TaskBiweekly):

    pass
