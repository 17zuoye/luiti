#-*-coding:utf-8-*-

from etl_utils import singleton, cached_property

@singleton()
class LuitiConfigClass(object):
    """ Make sure init variables only once. """
    curr_project_name = None
    curr_project_dir  = None

    @cached_property
    def attached_package_names(self): return set(['luiti'])

    @cached_property
    def luiti_tasks_packages(self): return set([])

luiti_config = LuitiConfigClass()
