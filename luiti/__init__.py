#-*-coding:utf-8-*-

__all__ = [
            'luigi', 'luiti_setup',

            'TaskBase',
            'TaskDay',
            'TaskDayHadoop',
            'TaskWeek',
            'TaskWeekHadoop',
            'TaskRange',
            'TaskRangeHadoop',

            'RootTask',
            'StaticFile',
            'MongoTask',

            'IOUtils', 'DateUtils', 'TargetUtils', 'HDFSUtils',
            'MRUtils', 'MathUtils', 'TaskUtils', 'CommandUtils',
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
from .env                  import env

from .task_base            import TaskBase
from .task_day             import TaskDay
from .task_week            import TaskWeek
from .task_range           import TaskRange

from .task_day_hadoop      import TaskDayHadoop
from .task_week_hadoop     import TaskWeekHadoop
from .task_range_hadoop    import TaskRangeHadoop

from .root_task            import RootTask
from .static_file          import StaticFile
from .mongo_task           import MongoTask

from .utils                import IOUtils, DateUtils, TargetUtils, HDFSUtils, \
                                  MRUtils, MathUtils, TaskUtils, CommandUtils, \
                                  CompressUtils

from .parameter            import ArrowParameter

from .mr_test_case         import MrTestCase



def __init__(self, *args, **kwargs):
    super(TaskBase, self).__init__(*args, **kwargs)

    # 在跨期的时候用于判断 该周应该是该周的哪些天。
    # 比如这学期开学是 2015-02-17(星期二) 开学, 那么这周的数据只有 0217-0222。
    # 而在寒假里(即run 2015-02-16(星期天) 的 task 时，那么该周的天只有 0216 一天。
    self.orig_date_value = arrow.get(self.date_value)

    self.reset_date() # reset date to at the beginning of current date type here




def luiti_setup(opts=dict()):
    # 1. default value
    assert 'sys_path' in opts, "opts['sys_path'] must by be configed! Or there will is none sys.path existed in distributed Hadoop servers."
    default_opts = {
            'sys_path'                : [],
            'LUIGI_CONFIG_PATH'       : '/etc/luigi/client.cfg',
            'LUITI_DIRS'              : [],
        }
    opts = dict(default_opts.items() + opts.items())

    # 2. config
    env.sys_path                      = opts['sys_path']
    os.environ['LUIGI_CONFIG_PATH']   = opts['LUIGI_CONFIG_PATH']

    # 3. setup env
    env.setup()

    # 4. setup luiti tasks
    # 4.1. Add luiti tasks dir
    assert len(opts['LUITI_DIRS']) >= 1, "LUITI_DIRS must be configed! e.g. LUITI_DIRS=['/path/to/tasks/parent/dir', '/another/luiti/tasks/dir']  "
    for luiti_tasks_dir_1 in opts['LUITI_DIRS']:
        if luiti_tasks_dir_1 not in sys.path:
            sys.path.insert(0, luiti_tasks_dir_1)
