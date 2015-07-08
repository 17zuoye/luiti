# -*-coding:utf-8-*-

__all__ = ["WebuiDay", "luigi"]


from luiti_webui_tests import WebuiDay, luigi
luigi.plug_packages("luiti_dump", "luiti_clean", "luiti_middle", "luiti_summary")
