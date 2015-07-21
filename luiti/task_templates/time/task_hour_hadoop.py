# -*-coding:utf-8-*-

from .task_hour import TaskHour
from ...luigi_extensions import luigi


class TaskHourHadoop(luigi.hadoop.HadoopExt, TaskHour):

    pass
