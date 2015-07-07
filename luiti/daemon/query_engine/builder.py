# -*-coding:utf-8-*-

__all__ = ["QueryBuilder"]

import arrow
from etl_utils import cached_property
from copy import deepcopy

from ...luigi_extensions import ArrowParameter
from ..graph import Graph
from ..utils import stringify, Template, TaskStorageSet
from .params import Params
from .create_task import CreateTask


class QueryBuilder(object):
    """
    Construct a query builder.

    All propertyies are generated lazily by using `cached_property`, as in a **DAG**.
    """

    def __init__(self, ptm, raw_params):
        assert isinstance(raw_params, dict), raw_params

        self.raw_params = raw_params
        self.ptm = ptm

    @cached_property
    def date_begin(self):
        return self.ptm.current_luiti_visualiser_env["date_begin"]

    @cached_property
    def date_end(self):
        date_end = self.ptm.current_luiti_visualiser_env.get("date_end", self.yesterday_str)
        self.ptm.current_luiti_visualiser_env["date_end"] = date_end
        return date_end

    @cached_property
    def yesterday(self):
        return ArrowParameter.now().replace(days=-1).floor("day")

    @cached_property
    def yesterday_str(self):
        return self.yesterday.format("YYYY-MM-DD")

    @cached_property
    def accepted_params(self):
        """
        Comes from current luiti that selected.
        """
        return self.ptm.current_luiti_visualiser_env["additional_task_parameters"]

    @cached_property
    def accepted_query_params(self):
        """
        provide to visualSearch.js, used for autocomplete.

        user query via URL search.

        autocomplete params key/value.
        """
        # date range related.
        days_range = arrow.Arrow.range("day",
                                       ArrowParameter.get(self.date_begin),
                                       ArrowParameter.get(self.date_end))
        accepted_date_values = sorted(map(str, days_range))

        # result
        return {
            "date_value": accepted_date_values,
            "task_cls": self.ptm.task_class_names,
            "luiti_package": self.ptm.task_package_names,
        }

    @cached_property
    def default_query(self):
        """ Query provide by user config. """
        # assign default params
        default_query = {
            "date_value": str(self.yesterday),
            # to insert more key-value
        }

        # get config from current package's luiti_visualiser_env
        for task_param, task_param_opt in self.accepted_params.iteritems():
            self.accepted_query_params[task_param] = task_param_opt["values"]
            default_query[task_param] = task_param_opt["default"]

        return default_query

    @cached_property
    def selected_query(self):
        selected_query = {k1: v1 for k1, v1 in self.raw_params.iteritems() if k1 in self.accepted_params or k1 == "date_value"}
        selected_query["luiti_package"] = self.selected_packages
        selected_query = dict(self.default_query.items() + selected_query.items())

        return selected_query

    @cached_property
    def default_packages(self):
        """ user provided. """
        return self.ptm.current_luiti_visualiser_env["package_config"].get("defaults", [])

    @cached_property
    def selected_packages(self):
        result = self.raw_params.get("luiti_package", self.default_packages)
        result = result or self.ptm.task_package_names
        return result

    @cached_property
    def selected_task_cls_names(self):
        """
        current selected.
        """
        result = set(self.raw_params.get("task_cls", []))

        # modify other cached_property
        self.selected_query["task_cls"] = list(result)

        return result

    @cached_property
    def total_task_instances(self):
        """
        Total task instances.
        """
        # 1. build possible params.
        # **remove** luiti_package and task_cls query str
        params_array = Params.build_params_array(self.default_query, self.selected_query)

        # 2. and generate task instances.
        total_task_instances = list()
        for ti in self.ptm.task_classes:
            # TODO why below two lines exist before.
            # if ti.__name__ not in self.selected_task_cls_names:
            #     continue

            for _params in params_array:
                task_instance = CreateTask.new(ti, _params)
                total_task_instances.append(task_instance)

        result = sorted(list(set(total_task_instances)))
        return result

    @cached_property
    def selected_task_instances(self):
        """ nodes that drawed in vis.js """
        # filter by package
        result = sorted(list(set(self.total_task_instances)))
        result = filter(lambda ti: ti.package_name in self.selected_packages,
                        result)

        # To avoid only self is in the graph.
        # If select task class, then to find linked task instances.
        if not self.selected_task_cls_names:
            return result

        pure_selected_task_instances = [ti for ti in result if ti.task_clsname in self.selected_task_cls_names]
        pure_linked = TaskStorageSet()
        for ti in pure_selected_task_instances:
            for t2 in self.graph_infos_python["requires"]["direct"][ti]:
                pure_linked.add(t2)
            for t2 in self.graph_infos_python["upons"]["direct"][ti]:
                pure_linked.add(t2)

        # filter that tasks are linked, in current task_classes.
        result = [ti for ti in result if ti in pure_linked]
        result.extend(pure_selected_task_instances)
        result = list(set(result))
        return result

    @cached_property
    def graph_infos_data(self):
        return Graph.analysis_dependencies_between_nodes(self.total_task_instances,
                                                         self.selected_packages)

    @cached_property
    def graph_infos_python(self):
        return self.graph_infos_data["python"]

    @cached_property
    def nodes(self):
        return [Template.a_node(ti) for ti in self.selected_task_instances]

    @cached_property
    def edges(self):
        return Template.edges_from_nodes(self.selected_task_instances)

    @cached_property
    def nodes_groups(self):
        return Graph.split_edges_into_groups(self.edges,
                                             self.nodes,
                                             self.selected_task_instances)

    @cached_property
    def nodes_groups_in_view(self):
        return [sorted(list(nodes_set)) for nodes_set in self.nodes_groups]

    @cached_property
    def task_instance_repr_to_info(self):
        result = dict()
        for ti in self.selected_task_instances:
            param_kwargs = deepcopy(ti.param_kwargs)
            if "pool" in param_kwargs:
                del param_kwargs["pool"]
            result[str(ti)] = {"task_cls": ti.task_clsname, "param_kwargs": stringify(param_kwargs)}
        return result

    @cached_property
    def result(self):
        return {
            "title": "A DAG timely visualiser.",

            "queryparams": {
                "accepted": self.accepted_query_params,
                "selected_query": self.selected_query,
                "default_query": self.default_query,
                "luiti_visualiser_env": self.ptm.current_luiti_visualiser_env,
            },

            "ptm": {
                "task_class_names": self.ptm.task_class_names,
                "task_package_names": self.ptm.task_package_names,
                "task_clsname_to_package_name": self.ptm.task_clsname_to_package_name,
                "package_to_task_clsnames": self.ptm.package_to_task_clsnames,
                "task_instance_repr_to_info": self.task_instance_repr_to_info,
            },

            "nodeedge": {
                "nodes": self.nodes,
                "edges": self.edges,
                "nodes_groups": self.nodes_groups_in_view,
                "graph_infos": self.graph_infos_data["json"],
            },

            "errors": {
                "load_tasks": self.ptm.load_all_tasks_result["failure"],
            }
        }
