#-*-coding:utf-8-*-

__all__ = ["ref_tasks"]


from ..manager import load_a_task_by_name, luiti_config

def ref_tasks(*tasks):  # 装饰器
    """
    自动把依赖 Task 链接起来，通过属性访问。

    Example:

    ```python
    @ref_tasks("TaskB", "TaskC")
    class TaskA(TaskWeekBase):
        pass

    TaskA().TaskB == TaskB
    TaskA().TaskC == TaskC
    ```
    """
    def wrap_cls(ref_task_name):
        def _func(self):
            v1 = self.__dict__.get(ref_task_name, None)
            if v1 is None:
                v1 = load_a_task_by_name(ref_task_name)
                self.__dict__[ref_task_name] = v1
            return v1
        return _func

    def wrap_instance(ref_task_name, task_name):
        def _func(self):
            v1 = self.__dict__.get(task_name, None)
            if v1 is None:
                v1 = getattr(self, ref_task_name)(self.date_value)
                self.__dict__[task_name] = v1
            return v1
        return _func

    def __getstate__(self):
        """ Fix luiti_tasks module namespace conflicts. """
        for ref_task1 in self._ref_tasks:
            cname = ref_task1           # class    name
            iname = ref_task1 + "_task"  # instance name

            # delete instance property is enough.
            #if hasattr(self.__class__, cname):  delattr(self.__class__, cname)
            #if hasattr(self.__class__, iname):  delattr(self.__class__, iname)

            if cname in self.__dict__:
                del self.__dict__[cname]
            if iname in self.__dict__:
                del self.__dict__[iname]
        return self.__dict__

    def __setstate__(self, d1):
        # 1. default
        self.__dict__.update(d1)
        # 2. plug other package in `.__init_luiti`
        luiti_config.curr_project_name = self.package_name
        luiti_config.link_packages()

# cached_property 捕获不了 ref_task_name 变量, 被重置为某一个了。。
# property 可以捕获 ref_task_name 变量。
    def func(cls):
        setattr(cls, "_ref_tasks", tasks)
        for ref_task_name in cls._ref_tasks:
            setattr(cls, ref_task_name, property(wrap_cls(ref_task_name)))

            # TODO 根据当前日期返回。
            task_name = "%s_%s" % (ref_task_name, "task")
            setattr(cls, task_name, property(wrap_instance(ref_task_name, task_name)))

            # clear ref task info when pickle.dump
            setattr(cls, "__getstate__", __getstate__)
        return cls
    return func
