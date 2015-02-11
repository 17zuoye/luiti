#-*-coding:utf-8-*-

from inflector    import Inflector
from etl_utils import singleton, cached_property
import arrow

@singleton()
class LuitiConfigClass(object):
    """ Make sure init variables only once. """
    DateTypes          = ["range", "week"] + arrow.Arrow._ATTRS # ['year', 'month', ...]

    curr_project_name = None
    curr_project_dir  = None

    linked_luigi      = None

    @cached_property
    def attached_package_names(self): return set(['luiti'])

    @cached_property
    def luiti_tasks_packages(self): return set([])

    @staticmethod
    def import2(a_package):
        return __import__(a_package, None, None, 'non_empty')

    @staticmethod
    def get_date_type(name1):
        """ Inherit class must be in TaskBase{Day,Week,Month,Range} style.  """
        assert isinstance(name1, (str, unicode))
        str1 = Inflector().underscore(name1).split("_")[-1].lower()
        assert str1 in luiti_config.DateTypes
        return str1

    @staticmethod
    def get_time_task(name1):
        type2 = luiti_config.get_date_type(name1)
        return "Task" + Inflector().camelize(type2)


luiti_config = LuitiConfigClass()
