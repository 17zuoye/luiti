# -*-coding:utf-8-*-

__all__ = ["Query"]

from ..graph import Graph
from ..template import Template


class Query(object):
    """
    Use params to query some data from luiti.
    """

    def get_env(self, raw_params=dict()):
        """
        Generate all data needed.
        """
        query_params = self.generate_query_params()
        default_query = self.generate_default_query(query_params)

        default_packages = self.current_luiti_visualiser_env["package_config"]["defaults"]

        selected_packages = raw_params.get("luiti_package", default_packages)
        selected_task_cls_names = set(raw_params.get("task_cls", []))
        selected_task_cls_names  # s

        selected_query = self.generate_selected_query(default_query, raw_params, selected_packages)

        # total_task_instances = self.generate_total_task_instances(default_query, selected_query, selected_task_cls_names)
        # TODO filter nodes
        total_task_instances = self.generate_total_task_instances(default_query, selected_query, self.task_class_names)
        graph_infos_data = Graph.analysis_dependencies_between_nodes(total_task_instances, selected_packages)
        graph_infos_python = graph_infos_data["python"]

        selected_task_instances = filter(lambda ti: ti.package_name in selected_packages, total_task_instances)
        selected_task_instances = sorted(list(set(selected_task_instances)))

        # To avoid only self is in the graph.
        # If select task class, then to find linked task instances.
        if selected_task_cls_names:
            pure_selected_task_instances = [ti for ti in selected_task_instances if ti.task_clsname in selected_task_cls_names]
            pure_linked = set([])
            for ti in pure_selected_task_instances:
                for t2 in graph_infos_python["requires"]["total"][ti]:
                    pure_linked.add(t2)
                for t2 in graph_infos_python["upons"]["total"][ti]:
                    pure_linked.add(t2)
            # filter that tasks are linked, in current task_classes.
            selected_task_instances = [ti for ti in selected_task_instances if ti in pure_linked]
            selected_task_instances.extend(pure_selected_task_instances)
        # import pdb
        # pdb.set_trace()

        nodes = ([Template.a_node(ti) for ti in selected_task_instances])
        nodeid_to_node_dict = {node["id"]: node for node in nodes}

        edges = Template.edges_from_task_instances(selected_task_instances)

        nodes_groups = Graph.split_edges_into_groups(edges, nodes, selected_task_instances)
        nodes_groups_in_view = [sorted(list(nodes_set)) for nodes_set in nodes_groups]

        task_instance_repr_to_info = self.generate_task_instance_repr_to_info(selected_task_instances)

        return {
            "query_params": query_params,

            "title": "A DAG timely visualiser.",
            "selected_query": selected_query,
            "default_query": default_query,
            "luiti_visualiser_env": self.current_luiti_visualiser_env,

            "task_class_names": self.task_class_names,
            "task_package_names": self.task_package_names,
            "task_clsname_to_package_name": self.task_clsname_to_package_name,
            "package_to_task_clsnames": self.package_to_task_clsnames,

            "nodes": nodes,
            "edges": edges,
            "nodes_groups": nodes_groups_in_view,
            "nodeid_to_node_dict": nodeid_to_node_dict,

            "graph_infos": graph_infos_data["json"],
            "task_instance_repr_to_info": task_instance_repr_to_info,

            "errors": {
                "load_tasks": self.load_all_tasks_result["failure"],
            }
        }
