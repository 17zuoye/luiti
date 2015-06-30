# -*-coding:utf-8-*-

__all__ = ['luigi', 'config',

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

           'os', 're', 'sys', 'defaultdict', 'json', 'cached_property',
           'arrow',

           'MrTestCase', ]

import os
import sys
import re
from collections import defaultdict
import json
from etl_utils import cached_property

from .luigi_decorators import luigi
from .hadoop_ext import HadoopExt
from .task_templates.time.task_base import TaskBase
from .task_templates.time.task_hour import TaskHour
from .task_templates.time.task_day import TaskDay
from .task_templates.time.task_week import TaskWeek
from .task_templates.time.task_month import TaskMonth
from .task_templates.time.task_range import TaskRange

from .task_templates.time.task_day_hadoop import TaskDayHadoop
from .task_templates.time.task_week_hadoop import TaskWeekHadoop
from .task_templates.time.task_range_hadoop import TaskRangeHadoop

from .task_templates.other.root_task import RootTask
from .task_templates.other.static_file import StaticFile
from .task_templates.other.mongo_import_task import MongoImportTask

from . import manager
from .utils import IOUtils, DateUtils, TargetUtils, HDFSUtils, \
    MRUtils, MathUtils, CommandUtils, CompressUtils

import arrow
from .luigi_extensions import ArrowParameter

from .mr_test_case import MrTestCase


config = manager.luiti_config

luigi.tmp_dir = "/tmp"  # default one
