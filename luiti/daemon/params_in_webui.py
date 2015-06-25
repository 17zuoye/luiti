# -*-coding:utf-8-*-

from ..parameter import arrow, ArrowParameter


class ParamsInWebUI(object):
    """
    The module belongs to .package_task_management.PTM
    """

    def generate_query_params(self):
        """
        provide config to visualSearch.js
        """
        yesterday_str = self.yesterday().format("YYYY-MM-DD")
        self.current_luiti_visualiser_env["date_end"] = self.current_luiti_visualiser_env.get("date_end", yesterday_str)

        date_begin = ArrowParameter.get(self.current_luiti_visualiser_env["date_begin"])
        date_end = ArrowParameter.get(self.current_luiti_visualiser_env["date_end"])
        accepted_date_values = sorted(map(str, arrow.Arrow.range("day", date_begin, date_end)))

        query_params = {
            "accepted": {
                "date_value": accepted_date_values,
                "task_cls": self.task_class_names,
                "luiti_package": self.task_package_names,
            }
        }

        return query_params

    def yesterday(self):
        return ArrowParameter.now().replace(days=-1).floor("day")
