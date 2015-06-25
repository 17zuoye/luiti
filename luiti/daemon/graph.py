# -*-coding:utf-8-*-

__all__ = ["Graph"]

from copy import deepcopy
from .template import Template


class Graph(object):

    @staticmethod
    def analysis_dependencies_between_nodes(self, task_instances):
        pass

    @staticmethod
    def split_edges_into_groups(edges, nodes, task_instances):
        # TODO Graph related
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
