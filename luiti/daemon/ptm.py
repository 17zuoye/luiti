# -*-coding:utf-8-*-

__all__ = ["PTM"]


import sys
from etl_utils import singleton, cached_property
import importlib
import inspect

from .. import manager
from .visualiser_env_template import VisualiserEnvTemplate


@singleton()
class PackageTaskManagementClass(object):
    """
    Manage packages and tasks.

    When webui daemon started, these values are readed, and will not be modified. It means they are static.
    """

    @cached_property
    def current_package_name(self):
        return manager.luiti_config.get_curr_project_name()

    @cached_property
    def current_init_luiti(self):
        self.current_package_path  # insert pacakge into sys.path
        __init_luiti = self.current_package_name + ".luiti_tasks.__init_luiti"
        return importlib.import_module(__init_luiti)

    @cached_property
    def current_package_path(self):
        p1 = manager.luiti_config.get_curr_project_path()
        sys.path.insert(0, p1)
        return p1

    @cached_property
    def current_luiti_visualiser_env(self):
        env = getattr(self.current_init_luiti, "luiti_visualiser_env", VisualiserEnvTemplate())
        assert isinstance(env, VisualiserEnvTemplate), env
        return env.data

    @cached_property
    def load_all_tasks_result(self):
        return manager.load_all_tasks()

    @cached_property
    def task_classes(self):
        return [i1["task_cls"] for i1 in self.load_all_tasks_result["success"]]

    @cached_property
    def task_class_names(self):
        return sorted([i1.__name__ for i1 in self.task_classes])

    @cached_property
    def task_clsname_to_package(self):
        return manager.PackageMap.task_clsname_to_package

    @cached_property
    def task_clsname_to_source_file(self):
        def get_pyfile(task_cls):
            f1 = inspect.getfile(task_cls)
            return f1.replace(".pyc", ".py")

        return {task_cls.__name__: get_pyfile(task_cls) for task_cls in self.task_classes}

    @cached_property
    def task_clsname_to_package_name(self):
        return {t1: p1.__name__ for t1, p1 in self.task_clsname_to_package.iteritems()}

    @cached_property
    def task_package_names(self):
        return sorted([p1.__name__ for p1 in set(self.task_clsname_to_package.values())])

    @cached_property
    def package_to_task_clsnames(self):
        return {package.__name__: sorted(list(task_clsnames)) for package, task_clsnames
                in manager.PackageMap.package_to_task_clsnames.iteritems()}


PTM = PackageTaskManagementClass()
