# -*-coding:utf-8-*-

from .__init_luiti import TaskDay, cached_property


class ImportPackagesDay(TaskDay):

    root_dir = "/foobar"

    @cached_property
    def egg_library(self):
        import zip_package_by_luiti  # test import library from zip file
        return zip_package_by_luiti
