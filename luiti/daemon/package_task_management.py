# -*-coding:utf-8-*-

__all__ = ["PTM"]


import sys
from etl_utils import singleton, cached_property
import importlib
from copy import deepcopy
import itertools
import luigi
from ..parameter import ArrowParameter

from .. import manager
from .template import Template
from .params_in_webui import ParamsInWebUI


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
        import inspect

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

    def split_edges_into_groups(self, edges, nodes, task_instances):
        edges = deepcopy(edges)
        groups = list()  # element is set

        # make sure every node appear, even has not link to other tasks.
        for ti in task_instances:
            edges.append(Template.an_edge(ti, ti))

        # 1. first time, divid edges into groups.
        for edge in edges:
            is_in_current_groups = False
            for group in groups:
                if (edge["from"] in group) or (edge["to"] in group):
                    is_in_current_groups = True
                    group.add(edge["from"])
                    group.add(edge["to"])
            if is_in_current_groups is False:
                groups.append(set([edge["from"], edge["to"]]))

        # 2. second time, merge groups that has common tasks
        # iterate to reduce redudant group
        result = list()
        for group1 in groups:
            append_idx = None
            for idx2, group2 in enumerate(result):
                if len(group1 & group2) > 0:
                    append_idx = idx2
                    break
            if append_idx is None:
                result.append(group1)
            else:
                result[append_idx] = result[append_idx] | group1

        result = sorted(result, key=lambda i1: (-len(i1), i1))
        return result

    def get_env(self, raw_params=dict()):
        query_params = self.generate_query_params()

        # assign default params
        default_params = {
            "date_value": str(self.yesterday()),
            # to insert more key-value
        }
        # get config from current package's luiti_visualiser_env
        accepted_params = PTM.current_luiti_visualiser_env["task_params"]
        for task_param, task_param_opt in accepted_params.iteritems():
            query_params["accepted"][task_param] = task_param_opt["values"]
            default_params[task_param] = task_param_opt["default"]

        # **remove** luiti_package and task_cls query str
        selected_params = {k1: v1 for k1, v1 in raw_params.iteritems() if k1 in accepted_params or k1 == "date_value"}
        selected_params_with_kv_array = list()
        for k1, v1 in selected_params.iteritems():
            k1_v2_list = list()
            for v2 in v1:
                # Fix overwrited params type in luiti
                # TODO Fix luigi.Task#__eq__
                if k1 == "date_value":
                    v2 = ArrowParameter.get(v2)
                else:
                    v2 = unicode(v2)
                k1_v2_list.append({"key": k1, "val": v2})
            selected_params_with_kv_array.append(k1_v2_list)

        possible_params = map(list, itertools.product(*selected_params_with_kv_array))

        selected_task_cls_names = raw_params.get("task_cls", self.task_class_names)

        total_task_instances = list()
        _default_params = [{"key": k1, "val": v1} for k1, v1 in default_params.iteritems()]
        for ti in PTM.task_classes:
            if ti.__name__ not in selected_task_cls_names:
                continue

            for _params in possible_params:
                _real_task_params = dict()
                for kv2 in (_default_params + _params):
                    has_key = hasattr(ti, kv2["key"])
                    is_luigi_params = isinstance(getattr(ti, kv2["key"], None), luigi.Parameter)
                    if has_key and is_luigi_params:
                        _real_task_params[kv2["key"]] = kv2["val"]
                task_instance = ti(**_real_task_params)
                total_task_instances.append(task_instance)

        default_packages = PTM.current_luiti_visualiser_env["package_config"]["defaults"]
        selected_packages = raw_params.get("luiti_package", default_packages)

        selected_task_instances = filter(lambda ti: ti.package_name in selected_packages, total_task_instances)

        nodes = ([Template.a_node(ti) for ti in selected_task_instances])
        nodeid_to_node_dict = {node["id"]: node for node in nodes}

        edges = Template.edges_from_task_instances(selected_task_instances)

        nodes_groups = PTM.split_edges_into_groups(edges, nodes, selected_task_instances)
        nodes_groups_in_view = [sorted(list(nodes_set)) for nodes_set in nodes_groups]

        selected_params["luiti_package"] = selected_packages

        return {
            "query_params": query_params,

            "title": "A DAG timely visualiser.",
            "selected_params": selected_params,
            "default_params": default_params,
            "luiti_visualiser_env": PTM.current_luiti_visualiser_env,

            "task_class_names": PTM.task_class_names,
            "task_package_names": PTM.task_package_names,
            "task_clsname_to_package_name": PTM.task_clsname_to_package_name,
            "package_to_task_clsnames": PTM.package_to_task_clsnames,

            "nodes": nodes,
            "edges": edges,
            "nodes_groups": nodes_groups_in_view,
            "nodeid_to_node_dict": nodeid_to_node_dict,

            "errors": {
                "load_tasks": self.load_all_tasks_result["failure"],
            }
        }


PTM = PackageTaskManagementClass()
