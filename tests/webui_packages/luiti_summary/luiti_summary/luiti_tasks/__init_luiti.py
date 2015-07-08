# -*-coding:utf-8-*-

__all__ = ["WebuiDay", "luigi"]


from luiti_webui_tests import WebuiDay, luigi, VisualiserEnvTemplate
luigi.plug_packages("luiti_dump", "luiti_clean", "luiti_middle", "luiti_summary")


# plug more packages, compact with old tests, without to migrate them to webui_packages totally.
luigi.plug_packages("project_A", "project_B", "zip_package_by_luiti")


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
