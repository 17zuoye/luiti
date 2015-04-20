#-*-coding:utf-8-*-

from .loader              import Loader
from .table               import Table
from .dep                 import Dep
from .files               import Files

from .config              import luiti_config
from .package_map         import PackageMap
from .active_packages     import active_packages

from .sys_argv            import SysArgv

from .generate_from_templates import GenerateFromTemplates



#################
### API list  ###
#################

find_dep_on_tasks                                 = Dep.find_dep_on_tasks
get_all_date_file_to_task_instances               = Files.get_all_date_file_to_task_instances
soft_delete_files                                 = Files.soft_delete_files
load_all_tasks                                    = Loader.load_all_tasks
load_a_task_by_name                               = Loader.load_a_task_by_name
import2                                           = luiti_config.import2
print_all_tasks                                   = Table.print_all_tasks
print_files_by_task_cls_and_date_range            = Table.print_files_by_task_cls_and_date_range
new_a_project                                     = GenerateFromTemplates.new_a_project
generate_a_task                                   = GenerateFromTemplates.generate_a_task


from etl_utils import singleton, cached_property
@singleton()
class LazyData(object):

    @cached_property
    def all_task_classes(self):
        return [i1['task_cls'] for i1 in ld.result['success']]

    @cached_property
    def result(self):
        return load_all_tasks()

ld = LazyData()
