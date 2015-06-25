# -*-coding:utf-8-*-

from ..parameter import arrow, ArrowParameter
from etl_utils import cached_property
import itertools
import luigi

# TODO use a builder


class ParamsInWebUI(object):
    """
    The module belongs to .package_task_management.PTM

    Related to query.
    """
    def yesterday(self):
        return ArrowParameter.now().replace(days=-1).floor("day")

    @cached_property
    def accepted_params(self):
        """
        Comes from current luiti that selected.
        """
        # TODO provide a template
        return self.current_luiti_visualiser_env["task_params"]

    def generate_query_params(self):
        """
        provide config to visualSearch.js
        """
        # date range related.
        yesterday_str = self.yesterday().format("YYYY-MM-DD")
        self.current_luiti_visualiser_env["date_end"] = self.current_luiti_visualiser_env.get("date_end", yesterday_str)

        date_begin = ArrowParameter.get(self.current_luiti_visualiser_env["date_begin"])
        date_end = ArrowParameter.get(self.current_luiti_visualiser_env["date_end"])
        accepted_date_values = sorted(map(str, arrow.Arrow.range("day", date_begin, date_end)))

        # result
        query_params = {
            "accepted": {
                "date_value": accepted_date_values,
                "task_cls": self.task_class_names,
                "luiti_package": self.task_package_names,
            }
        }

        return query_params

    def generate_default_query(self, query_params):
        # assign default params
        default_query = {
            "date_value": str(self.yesterday()),
            # to insert more key-value
        }
        # get config from current package's luiti_visualiser_env
        for task_param, task_param_opt in self.accepted_params.iteritems():
            query_params["accepted"][task_param] = task_param_opt["values"]
            default_query[task_param] = task_param_opt["default"]

        return default_query

    def generate_selected_query(self, default_query, raw_params, selected_packages):
        selected_query = {k1: v1 for k1, v1 in raw_params.iteritems() if k1 in self.accepted_params or k1 == "date_value"}
        selected_query["luiti_package"] = selected_packages
        selected_query = dict(default_query.items() + selected_query.items())

        return selected_query

    def generate_total_task_instances(self, default_query, selected_query, selected_task_cls_names):
        # **remove** luiti_package and task_cls query str
        selected_query_with_kv_array = list()
        for k1, v1 in selected_query.iteritems():
            k1_v2_list = list()

            # v1 is params value list
            if not isinstance(v1, list):
                v1 = [v1]
            for v2 in v1:
                # Already overwrited params type and luigi.Task#__eq__ in luiti.
                # See more details at task_templates.time.task_base.py
                if k1 == "date_value":
                    v2 = ArrowParameter.get(v2)
                else:
                    v2 = unicode(v2)
                k1_v2_list.append({"key": k1, "val": v2})
            selected_query_with_kv_array.append(k1_v2_list)

        possible_params = map(list, itertools.product(*selected_query_with_kv_array))

        total_task_instances = list()
        _default_query = [{"key": key, "val": val} for key, val in default_query.iteritems()]
        for ti in self.task_classes:
            if ti.__name__ not in selected_task_cls_names:
                continue

            for _params in possible_params:
                _real_task_params = dict()
                for kv2 in (_default_query + _params):
                    has_key = hasattr(ti, kv2["key"])
                    is_luigi_params = isinstance(getattr(ti, kv2["key"], None), luigi.Parameter)
                    if has_key and is_luigi_params:
                        _real_task_params[kv2["key"]] = kv2["val"]
                task_instance = ti(**_real_task_params)
                total_task_instances.append(task_instance)

        return total_task_instances
