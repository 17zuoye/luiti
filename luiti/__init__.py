#-*-coding:utf-8-*-

__all__ = [
            'luigi', 'luiti_setup', 'config',

            'TaskBase',
            'TaskHour',
            'TaskDay',
            'TaskDayHadoop',
            'TaskWeek',
            'TaskWeekHadoop',
            'TaskMonth',
            'TaskRange',
            'TaskRangeHadoop',

            'RootTask',
            'StaticFile',
            'MongoImportTask',
            'HadoopExt',

            'manager',

            'IOUtils', 'DateUtils', 'TargetUtils', 'HDFSUtils',
            'MRUtils', 'MathUtils', 'CommandUtils',
            'CompressUtils',

            'ArrowParameter',

            'os', 're', 'defaultdict', 'json', 'cached_property',
            'arrow',

            'MrTestCase',
          ]

import os
import sys
import re
from collections import defaultdict
import json
import arrow
from etl_utils import cached_property

from .luigi_ext            import luigi
from .hadoop_ext           import HadoopExt
from .env                  import env

from .task_templates.time.task_base            import TaskBase
from .task_templates.time.task_hour            import TaskHour
from .task_templates.time.task_day             import TaskDay
from .task_templates.time.task_week            import TaskWeek
from .task_templates.time.task_month           import TaskMonth
from .task_templates.time.task_range           import TaskRange

from .task_templates.time.task_day_hadoop      import TaskDayHadoop
from .task_templates.time.task_week_hadoop     import TaskWeekHadoop
from .task_templates.time.task_range_hadoop    import TaskRangeHadoop

from .task_templates.other.root_task           import RootTask
from .task_templates.other.static_file         import StaticFile
from .task_templates.other.mongo_import_task   import MongoImportTask

from .                     import manager
from .utils                import IOUtils, DateUtils, TargetUtils, HDFSUtils, \
                                  MRUtils, MathUtils, CommandUtils, \
                                  CompressUtils

from .parameter            import ArrowParameter

from .mr_test_case         import MrTestCase



# TODO 整理
def luiti_setup(opts=None):
    # 1. default value
    assert 'sys_path' in opts, "opts['sys_path'] must be configed! Or there will is none sys.path existed in distributed Hadoop servers."
    default_opts = {
            'sys_path'                : [],
            'LUIGI_CONFIG_PATH'       : '/etc/luigi/client.cfg',
        }
    opts = dict(default_opts.items() + (opts or dict()).items())

    # 2. config
    env.sys_path                      = opts['sys_path']
    os.environ['LUIGI_CONFIG_PATH']   = opts['LUIGI_CONFIG_PATH']

    # 3. setup env
    env.setup()

config = manager.luiti_config
