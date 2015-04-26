# -*-coding:utf-8-*-

__all__ = ["plug_packages"]

from ..manager import luiti_config


def plug_packages(*package_names):
    """
    Let luigi know which packages should be attached, and can send to
    YARN, etc.

    Package format can be any valid Python package name, such as "project_B" or
    "project_C==0.0.2", etc.

    Usage: use `active_packages` decorator to notice luigi that these packages
    should include.
    """
    # if len(manager.luiti_config.attached_package_names) > 1:
    #    return False # default is luiti. and can plug only once.

    for p1 in package_names:
        # load all packages's depended pacakges.
        luiti_config.attached_package_names.add(p1)
# TODO why should do `luigi.hadoop.attach` in `active_packages`
