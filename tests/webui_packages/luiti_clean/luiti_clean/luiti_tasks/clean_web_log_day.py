# -*-coding:utf-8-*-

from .__init_luiti import WebuiDay, luigi


@luigi.ref_tasks("DumpWebLogDay")
class CleanWebLogDay(WebuiDay):
    """
    Clean web log
    """

    def requires(self):
        return self.DumpWebLogDay_task
