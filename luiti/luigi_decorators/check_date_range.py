# -*-coding:utf-8-*-

__all__ = ["check_date_range"]

from ..parameter import ArrowParameter


def check_date_range():  # 装饰器
    """
    从数据库导数据时，必须注意时间范围内的所有数据是否都齐全了。如果未齐全，
    即在当前时间范围里导的话，那么就会缺失数据了，相当于提前导了。

    比如在周六就把这周的关联数据导出来，那么周日的数据就没包含在里面。应该在下周一后才开始导。
    """
    def decorator(orig_run):
        def new_run(self):
            # 说明时间未到，然后就直接退出
            if self.date_value_by_type_in_begin <= \
                    ArrowParameter.now() < self.date_value_by_type_in_end:
                return False
            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
# TODO support Hadoop
