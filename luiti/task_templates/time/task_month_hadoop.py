# -*-coding:utf-8-*-

from .task_month import TaskMonth
from ...luigi_decorators import luigi


class TaskMonthHadoop(luigi.hadoop.HadoopExt, TaskMonth):

    pass
