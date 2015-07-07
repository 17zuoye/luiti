# -*-coding:utf-8-*-

from .__init_luiti import TaskDay, luigi


@luigi.ref_tasks("DumpWebLogDay")
class CleanWebLogDay(TaskDay):
    """
    Clean web log
    """

    def requires(self):
        return self.DumpWebLogDay_task
