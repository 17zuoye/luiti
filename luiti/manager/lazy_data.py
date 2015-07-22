# -*-coding:utf-8-*-

__all__ = ["ld"]


from etl_utils import singleton, cached_property

from .loader import Loader
from .dep import Dep
from .table import Table


@singleton()
class LazyData(object):

    @cached_property
    def all_task_classes(self):
        return [i1['task_cls'] for i1 in self.result['success']]

    @cached_property
    def result(self):
        return Loader.load_all_tasks()

ld = LazyData()
Dep.ld = ld
Table.ld = ld
