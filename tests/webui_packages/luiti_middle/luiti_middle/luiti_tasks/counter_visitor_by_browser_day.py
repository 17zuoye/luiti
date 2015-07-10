# -*-coding:utf-8-*-

from .__init_luiti import WebuiDay, luigi


@luigi.ref_tasks("CleanWebLogDay", "DumpBrowserMapDay")
class CounterVisitorByBrowserDay(WebuiDay):
    """
    I'm
    Counter
    Visitor
    By
    Browser
    Day.
    """

    def requires(self):
        return [self.CleanWebLogDay_task, self.DumpBrowserMapDay_task]
