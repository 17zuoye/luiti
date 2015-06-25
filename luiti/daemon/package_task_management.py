# -*-coding:utf-8-*-

__all__ = ["PTM"]


import sys
from etl_utils import singleton, cached_property
import importlib
import inspect

from .. import manager
from .template import Template
from .params_in_webui import ParamsInWebUI
from .graph import Graph


@singleton()
class PackageTaskManagementClass(ParamsInWebUI):
    """
    Manage packages and tasks.
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
        # TODO assert must setup `luiti_visualiser_env`
        return getattr(self.current_init_luiti, "luiti_visualiser_env")

    @cached_property
    def load_all_tasks_result(self):
        return manager.load_all_tasks()

    @cached_property
    def task_classes(self):
        return [i1["task_cls"] for i1 in self.load_all_tasks_result["success"]]

    @cached_property
    def task_class_names(self):
        return [i1.__name__ for i1 in self.task_classes]

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
        return {package.__name__: list(task_clsnames) for package, task_clsnames
                in manager.PackageMap.package_to_task_clsnames.iteritems()}

    def get_env(self, raw_params=dict()):
        query_params = self.generate_query_params()
        default_query = self.generate_default_query(query_params)

        default_packages = self.current_luiti_visualiser_env["package_config"]["defaults"]

        selected_packages = raw_params.get("luiti_package", default_packages)
        selected_task_cls_names = raw_params.get("task_cls", self.task_class_names)

        selected_query = self.generate_selected_query(default_query, raw_params, selected_packages)

        total_task_instances = self.generate_total_task_instances(default_query, selected_query, selected_task_cls_names)
        graph_infos = Graph.analysis_dependencies_between_nodes(total_task_instances)

        selected_task_instances = filter(lambda ti: ti.package_name in selected_packages, total_task_instances)
        selected_task_instances = sorted(list(set(selected_task_instances)))

        nodes = ([Template.a_node(ti) for ti in selected_task_instances])
        nodeid_to_node_dict = {node["id"]: node for node in nodes}

        edges = Template.edges_from_task_instances(selected_task_instances)

        nodes_groups = Graph.split_edges_into_groups(edges, nodes, selected_task_instances)
        nodes_groups_in_view = [sorted(list(nodes_set)) for nodes_set in nodes_groups]

        return {
            "query_params": query_params,

            "title": "A DAG timely visualiser.",
            "selected_query": selected_query,
            "default_query": default_query,
            "luiti_visualiser_env": PTM.current_luiti_visualiser_env,

            "task_class_names": PTM.task_class_names,
            "task_package_names": PTM.task_package_names,
            "task_clsname_to_package_name": PTM.task_clsname_to_package_name,
            "package_to_task_clsnames": PTM.package_to_task_clsnames,

            "nodes": nodes,
            "edges": edges,
            "nodes_groups": nodes_groups_in_view,
            "nodeid_to_node_dict": nodeid_to_node_dict,

            "graph_infos": graph_infos,

            "errors": {
                "load_tasks": self.load_all_tasks_result["failure"],
            }
        }


PTM = PackageTaskManagementClass()
