# -*-coding:utf-8-*-

from .task_month import TaskMonth
from ...luigi_extensions import luigi


class TaskMonthHadoop(luigi.hadoop.HadoopExt, TaskMonth):

    pass
