# -*-coding:utf-8-*-

__all__ = ["PTM"]


import sys
from etl_utils import singleton, cached_property
import importlib
from copy import deepcopy
from .. import arrow, ArrowParameter

from .. import manager
from .template import Template


@singleton()
class PackageTaskManagementClass(object):
    """
    Manage packages and tasks.
    """

    @cached_property
    def current_package_name(self):
        return manager.luiti_config.get_curr_project_name()

    @cached_property
    def current_init_luiti(self):
        return importlib.import_module(self.current_package_name + ".luiti_tasks.__init_luiti")

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
        return {package.__name__: list(task_clsnames) for package, task_clsnames in manager.PackageMap.package_to_task_clsnames.iteritems()}

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

    def get_env(self):
        # yesterday
        sample_day = ArrowParameter.now().replace(days=-1).floor("day")

        PTM.current_luiti_visualiser_env["date_end"] = PTM.current_luiti_visualiser_env.get("date_end", ArrowParameter.now().replace(days=-1).floor("day").format("YYYY-MM-DD"))

        accepted_date_values = sorted(map(str, arrow.Arrow.range("day", ArrowParameter.get(PTM.current_luiti_visualiser_env["date_begin"]), ArrowParameter.get(PTM.current_luiti_visualiser_env["date_end"]))))

        config = {
            "accepted_params": {
                "date_value": accepted_date_values,
                "task_cls": PTM.task_class_names,
                "luiti_package": PTM.task_package_names,
            }
        }

        current_params = {
            "date_value": str(sample_day),
        }

        for task_param, task_param_opt in PTM.current_luiti_visualiser_env["task_params"].iteritems():
            config["accepted_params"][task_param] = task_param_opt["values"]
            current_params[task_param] = task_param_opt["default"]

        task_instances = map(lambda i1: i1(date_value=sample_day), PTM.task_classes)
        selected_task_instances = filter(lambda ti: ti.package_name in PTM.current_luiti_visualiser_env["package_config"]["defaults"], task_instances)

        nodes = ([Template.a_node(ti) for ti in selected_task_instances])
        nodeid_to_node_dict = {node["id"]: node for node in nodes}

        edges = Template.edges_from_task_instances(selected_task_instances)

        nodes_groups = PTM.split_edges_into_groups(edges, nodes, selected_task_instances)
        nodes_groups_in_view = [sorted(list(nodes_set)) for nodes_set in nodes_groups]

        return {
            "config": config,

            "title": "A DAG timely visualiser.",
            "current_params": current_params,
            "luiti_visualiser_env": PTM.current_luiti_visualiser_env,

            "task_class_names": PTM.task_class_names,
            "task_package_names": PTM.task_package_names,
            "task_clsname_to_package_name": PTM.task_clsname_to_package_name,
            "package_to_task_clsnames": PTM.package_to_task_clsnames,

            "nodes": nodes,
            "edges": edges,
            "nodes_groups": nodes_groups_in_view,
            "nodeid_to_node_dict": nodeid_to_node_dict,
        }


PTM = PackageTaskManagementClass()
