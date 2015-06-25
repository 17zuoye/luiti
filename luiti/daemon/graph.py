# -*-coding:utf-8-*-

__all__ = ["Graph"]

from copy import deepcopy
from collections import defaultdict

from .template import Template


class Graph(object):
    """
    Analysis graph relation between nodes.
    """

    @staticmethod
    def analysis_dependencies_between_nodes(task_instances):
        """
        Based on Data:
        1. Task_instances
        2. Their `requires` informations.

        Related function is luiti.manager.dep.Dep.find_dep_on_tasks
        """
        uniq_set = set(task_instances)

        # 1. raw `requires` and `invert` informations.
        task_instances_to_their_direct_requires = defaultdict(set)
        task_instances_to_their_direct_upons = defaultdict(set)

        def read_requires_from_task(task_instance):
            deps = task_instance.requires()
            if not isinstance(deps, list):
                deps = [deps]
            deps = filter(lambda i1: i1, deps)
            return deps

        for task_instance in task_instances:
            deps = read_requires_from_task(task_instance)
            selected_deps = [d1 for d1 in deps if d1 in uniq_set]
            task_instances_to_their_direct_requires[task_instance] = selected_deps

            for dep1 in selected_deps:
                task_instances_to_their_direct_upons[dep1].add(task_instance)

        # 2. unfold `requires` and `invert` informations.
        task_instances_to_their_total_requires = defaultdict(set)
        task_instances_to_their_total_upons = defaultdict(set)

        def add_total_deps(store, tree, store_node, fetch_node=None):
            """ add all recursive dependencies.
            1. store_node used to store in a result store.
            2. fetch_node used to fetch dependencies from a tree.
            """
            fetch_node = fetch_node or store_node
            deps = tree[store_node]

            for d1 in deps:
                store[store_node].add(d1)

            for d1 in deps:
                if store_node == d1:
                    continue
                if d1 not in store[store_node]:
                    add_total_deps(store, tree, store_node, d1)

        for task_instance in task_instances:
            add_total_deps(task_instances_to_their_total_requires, task_instances_to_their_direct_requires, task_instance)
            add_total_deps(task_instances_to_their_total_upons, task_instances_to_their_direct_upons, task_instance)

        def stringify(default_dict):
            return {str(k1): map(str, vs1) for k1, vs1 in default_dict.iteritems()}

        result = {
            "requires": {
                "direct": stringify(task_instances_to_their_direct_requires),
                "total": stringify(task_instances_to_their_total_requires),
            },
            "upons": {
                "direct": stringify(task_instances_to_their_direct_upons),
                "total": stringify(task_instances_to_their_total_upons),
            },
        }
        return result

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
