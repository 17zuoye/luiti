# -*-coding:utf-8-*-

__all__ = ["TaskDay", "luigi"]


from luiti_webui_tests import TaskDay, luigi, VisualiserEnvTemplate
luigi.plug_packages("luiti_dump", "luiti_clean", "luiti_middle", "luiti_summary")


luiti_visualiser_env = VisualiserEnvTemplate({
    "file_web_url_prefix": lambda: "http://HUE/filebrowser/#/",
    "date_begin": "2014-09-01",
    "additional_task_parameters": {
        "language": {
            "values": ["Chinese", "English"],
            "default": "English",
        }
    },
    "package_config": {
        "defaults": ["luiti_summary", ],
    }
})
