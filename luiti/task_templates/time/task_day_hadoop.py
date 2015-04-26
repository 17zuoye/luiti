#-*-coding:utf-8-*-

from .task_day import TaskDay
from ...luigi_decorators import luigi


class TaskDayHadoop(luigi.hadoop.HadoopExt, TaskDay):

    pass
