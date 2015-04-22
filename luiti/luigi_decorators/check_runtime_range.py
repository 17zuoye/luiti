#-*-coding:utf-8-*-

__all__ = ["check_runtime_range"]

from ..parameter import ArrowParameter

def check_runtime_range(**opts_1): # 装饰器
    """
    Support hour/weekday indexed range.

    Optional params:
    1. hour_num
    2. weekday_num
    3. now
    """
    def decorator(orig_run):
        def new_run(self):
            default_opts = {
                    "hour_num"      : range(1, 25),
                    "weekday_num"   : range(1, 8),
                }
            opts = dict(default_opts.items() + opts_1.items())

            now            = ArrowParameter.now()           # get current time
            hour_24        = int(now.format("H"))  # 0, 1, 2, ..., 23, 24
            day_of_week_7  = int(now.format("d"))  # 1, 2, 3, ..., 6, 7

            is_false = False
            if hour_24       not in opts['hour_num']:    is_false = True
            if day_of_week_7 not in opts['weekday_num']: is_false = True
            if is_false:
                print "[info]", now, " is not in ", opts, ", so the task exited."
                return False

            return orig_run(self)
        return new_run

    def func(cls):
        cls.run = decorator(cls.run)
        return cls
    return func
