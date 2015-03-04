#-*-coding:utf-8-*-

__all__ = ['DateUtils']


import arrow

class DateUtils:
    arrow = arrow

    @staticmethod
    def arrow_str(arrow1): return arrow.get(arrow1).datetime.strftime("%Y-%m-%d")

    @staticmethod
    def days_in_week(arrow1):
        arrow1 = arrow.get(arrow1)
        return arrow.Arrow.range(
                    'day',
                    arrow1.floor('week'),
                    arrow1.ceil('week'),)

    @staticmethod
    def weeks_in_range(arrow1, arrow2):
        return arrow.Arrow.range(
                    'week',
                    arrow.get(arrow1).floor('week'),
                    arrow.get(arrow2).ceil('week'),)

    @staticmethod
    def fixed_weeks_in_range(date_range_str):
        """ 修复 一个范围内所有全部覆盖的weeks，即最坏情况是掐头去尾。"""
        # 兼容如果date_range的最后一个不是星期天，那该周日志就不完整。
        assert len(date_range_str) == 21 # e.g. "2014-09-01-2014-11-19"
        first_date = arrow.get(date_range_str[0:10])
        last_date  = arrow.get(date_range_str[11:21])
        dates = DateUtils.weeks_in_range(first_date, last_date)
        if len(dates) > 0:
            if last_date.weekday() != 6:    # 6 index is Sunday
                dates = dates[:-1]
            if first_date.weekday()  != 0:  # 0 index is Monday
                dates = dates[1:]
        return dates

    @staticmethod
    def date_value_by_type_in_last(date_value_1, date_type_1):
        val1 = arrow.get(date_value_1).replace(**{(date_type_1+'s'):-1}) \
                                      .floor(date_type_1)
        return val1
