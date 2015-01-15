#-*-coding:utf-8-*-

from ..parameter import ArrowParameter

class Files(object):

    @staticmethod
    def get_all_date_files(date_range, task_classes):
        """ return all instances in date range. """
        assert len(date_range) == 17, "[error] correct format is \"20140901-20140905\", but the input is %s" % date_range
        first_date, last_date = date_range[0:8], date_range[9:]
        first_date, last_date = ArrowParameter.get(first_date, "YYYYMMDD"), ArrowParameter.get(last_date, "YYYYMMDD")

        return dict({ file_3 : task_instance_2 \
                    for task1 in task_classes \
                    for task_instance_2 in task1.instances_by_date_range(first_date, last_date) \
                    for file_3 in task_instance_2._persist_files + [task_instance_2.data_file]})
