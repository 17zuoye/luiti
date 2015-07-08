# -*-coding:utf-8-*-

"""
Provide test environment for webui_packages.
"""

import os
from etl_utils import cached_property
from luiti import luigi, TaskDay, VisualiserEnvTemplate
from luigi.mock import MockTarget


@cached_property
def root_dir(self):
    return os.path.join("/webui_packages", self.package_name)


def data_file(self):
    return os.path.join(self.root_dir, self.task_clsname, self.date_str)


def mock_output(self):
    """ Use luigi's feature. """
    return MockTarget(self.data_file)


class WebuiDay(TaskDay):
    """
    Don't overwrite TaskDay or TaskBase, or will fail other tests files.
    """
    pass


WebuiDay.extend({
    "root_dir": root_dir,
    "data_file": data_file,
    "output": mock_output,
})


__all__ = ["luigi", "WebuiDay", "VisualiserEnvTemplate"]
