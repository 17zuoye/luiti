# -*-coding:utf-8-*-

__all__ = ["Graph"]

from copy import deepcopy

from .template import Template
from .utils import stringify
from .task_storage import TaskStorageSet, TaskStorageDict


class Graph(object):
    """
    Analysis graph relation between nodes.
    """

    @staticmethod
    def analysis_dependencies_between_nodes(task_instances, selected_packages):
        """
        Based on Data:
        1. Task_instances
        2. Their `requires` informations.

        Related function is luiti.manager.dep.Dep.find_dep_on_tasks
        """
        uniq_set = TaskStorageSet(task_instances)

        # 1. raw `requires` and `invert` informations.
        task_instances_to_their_direct_requires = TaskStorageDict()
        task_instances_to_their_direct_upons = TaskStorageDict()

        for task_instance in task_instances:
            deps = Utils.read_requires_from_task(task_instance, selected_packages)
            selected_deps = [d1 for d1 in deps if d1 in uniq_set]
            task_instances_to_their_direct_requires[task_instance] = TaskStorageSet(selected_deps)
            for dep1 in selected_deps:
                task_instances_to_their_direct_upons[dep1].add(task_instance)

        # 2. unfold `requires` and `invert` informations.
        task_instances_to_their_total_requires = TaskStorageDict()
        task_instances_to_their_total_upons = TaskStorageDict()

        for task_instance in task_instances:
            Utils.add_total_deps(task_instances_to_their_total_requires, task_instances_to_their_direct_requires, task_instance)
            Utils.add_total_deps(task_instances_to_their_total_upons, task_instances_to_their_direct_upons, task_instance)

        def generate_result(_type="python"):
            """
            provide two versions of graph infos.

            1. one for front-end javascript.
            2. another for API python.
            """
            def wrap(obj):
                if _type == "python":
                    return obj
                if _type == "json":
                    return stringify(obj)

            return {
                "requires": {
                    "direct": wrap(task_instances_to_their_direct_requires),
                    "total": wrap(task_instances_to_their_total_requires),
                },
                "upons": {
                    "direct": wrap(task_instances_to_their_direct_upons),
                    "total": wrap(task_instances_to_their_total_upons),
                },
            }

        return {
            "python": generate_result("python"),
            "json": generate_result("json"),
        }

    @staticmethod
    def split_edges_into_groups(edges, nodes, task_instances):
        """
        Put linked task instances into a group.
        """
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


class Utils(object):
    """ only for this file """

    @staticmethod
    def read_requires_from_task(task_instance, selected_packages):
        deps = task_instance.requires()
        if not isinstance(deps, list):
            deps = [deps]
        # make sure it's a valid luiti task
        deps = filter(lambda i1: hasattr(i1, "package_name"), deps)
        # filter is very important, or can't find dict data.
        deps = filter(lambda i1: i1.package_name in selected_packages, deps)
        return deps

    @staticmethod
    def add_total_deps(store, tree, store_node, fetch_node=None):
        """ add all recursive dependencies.
        1. `store_node` used to store in a result store.
        2. `fetch_node` used to fetch dependencies from a tree.
        """
        fetch_node = fetch_node or store_node

        for d1 in tree[fetch_node]:
            if d1 == store_node:
                continue

            store[store_node].add(d1)

            for d2 in tree[d1]:
                if d2 not in store[store_node]:
                    Utils.add_total_deps(store, tree, store_node, d2)
