# -*-coding:utf-8-*-

from dateutil import tz
from .parameter import ArrowParameter


class TaskInit(object):

    @staticmethod
    def setup(task_instance):
        """
        Let luigi'Task supports luiti's operations.
        """
        self = task_instance

        # 在跨期的时候用于判断 该周应该是该周的哪些天。
        # 比如这学期开学是 2015-02-17(星期二) 开学, 那么这周的数据只有 0217-0222。
        # 而在寒假里(即run 2015-02-16(星期天) 的 task 时，那么该周的天只有 0216 一天。
        self.orig_date_value = \
            ArrowParameter.get(self.date_value).replace(tzinfo=tz.tzlocal())

        # reset date to at the beginning of current date type here
        self.reset_date()

        self.data_file      # force load it now, or `output` still load it.
        self.package_name   # force load it now, use to serialize

        # Fix luigi.Task#__eq__
        """
        >>> t1.param_args
        (<Arrow [2015-06-23T00:00:00+08:00]>,)
        >>> map(str, t1.param_args)
        ['2015-06-23T00:00:00+08:00']

        def __eq__(self, other):
            return self.__class__ == other.__class__ and self.param_args == other.param_args
        """
        self.param_kwargs["date_value"] = ArrowParameter.get(self.param_kwargs["date_value"])
        self.param_args = tuple(sorted(map(str, [value for key, value in self.param_kwargs.iteritems()])))

        # NOTE below codes are copied from luigi's Task
        # Build up task id
        task_id_parts = ["%s=%s" % (k1, v1) for k1, v1 in self.param_kwargs.iteritems() if k1 not in ["pool"]]
        self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))
        self.__hash = hash(self.task_id)
