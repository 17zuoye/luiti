# -*-coding:utf-8-*-

from .task_year import TaskYear
from ...luigi_extensions import luigi


class TaskYearHadoop(luigi.hadoop.HadoopExt, TaskYear):

    pass
