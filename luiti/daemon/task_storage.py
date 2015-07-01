# -*-coding:utf-8-*-

__all__ = ["TaskStorageSet", "TaskStorageDict"]

from UserDict import UserDict

"""
Task#__hash isn't consistent when one is from task_instances, and another is from `requires`.

Here we use #task_id to compare that if two tasks are the same one.
"""


class TaskStorageSet(set):
    """
    hash(luigi.Task) don't work well, so use `luigi.Task.task_id` fix it temporarily.
    """

    def __init__(self, task_list=list()):
        self.store = dict()

        for t1 in task_list:
            self.add(t1)

    def __contains__(self, t1):
        return t1.task_id in self.store

    def add(self, t1):
        self.store[t1.task_id] = t1

    def remove(self, t1):
        del self.store[t1.task_id]

    def __repr__(self):
        return repr(self.store.keys())

    def __len__(self):
        return len(self.store)

    def __iter__(self):
        return self.store.itervalues()


class TaskStorageDict(UserDict):

    def __getitem__(self, ti):
        if ti.task_id in self.data:
            return self.data[ti.task_id]
        if hasattr(self.__class__, "__missing__"):
            return self.__class__.__missing__(self, ti)
        raise KeyError(ti)

    def __setitem__(self, ti, item):
        self.data[ti.task_id] = item

    def __delitem__(self, ti):
        del self.data[ti.task_id]

    def __missing__(self, ti):
        s1 = TaskStorageSet()
        self.data[ti.task_id] = s1
        return s1
