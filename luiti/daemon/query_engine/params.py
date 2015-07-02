# -*-coding:utf-8-*-

__all__ = ["Params"]

from ...luigi_extensions import ArrowParameter
import itertools


class Params(object):

    @staticmethod
    def build_params_array(default_query, selected_query):
        """
        1. build possible params
        2. and with default params
        """

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

        possible_params_in_kv = map(list, itertools.product(*selected_query_with_kv_array))

        params_array = list()
        for kv_list in possible_params_in_kv:
            opt = {kv1["key"]: kv1["val"] for kv1 in kv_list}
            opt = dict(default_query.items() + opt.items())
            params_array.append(opt)

        return sorted(params_array)
