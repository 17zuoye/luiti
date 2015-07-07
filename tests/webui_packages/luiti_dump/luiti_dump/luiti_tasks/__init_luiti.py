# -*-coding:utf-8-*-

__all__ = ["TaskDay", "luigi"]


from luiti_webui_tests import TaskDay, luigi
luigi.plug_packages("luiti_dump", "luiti_clean", "luiti_middle", "luiti_summary")
