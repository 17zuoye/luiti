# -*-coding:utf-8-*-

import os
import sys
import argparse
from etl_utils import cached_property

curr_dir = os.getcwd()


class Cli(object):
    """ Luiti command line interface. """

    def run(self):
        self.check_argv()
        getattr(self.executor, self.subcommand)()

    def check_argv(self):
        """ check arguments """
        if len(sys.argv) == 1:
            self.parser.print_help()
            exit(0)
        else:
            if "project_dir" in self.args_main:
                self.luiti_config.curr_project_dir = self.args_main.project_dir
        self.luiti_config.curr_project_dir = self.luiti_config.curr_project_dir or curr_dir

        # check if it's a valid project.
        if self.subcommand != "new":  # not new a project
            self.luiti_config.fix_project_dir()

            if not os.path.exists(os.path.join(self.luiti_config.curr_project_dir, "luiti_tasks")):
                print u"""
            Current work directory '%s' has no sub directory 'luiti_tasks', make sure
            you have already `cd path/to/your/luiti/project/` .
                """ % (self.luiti_config.curr_project_dir)
                exit(1)

    @cached_property
    def subparsers(self):
        self.parser  # pre cache
        return self._subparsers

    @cached_property
    def parser(self):
        parser_main = argparse.ArgumentParser(description="Luiti tasks manager.")
        subparsers = parser_main.add_subparsers(title="subcommands",
                                                description='valid subcommands',)
        self._subparsers = subparsers  # link

        parser_ls = subparsers.add_parser('ls', help=u"list all current luiti tasks.")

        parser_new = subparsers.add_parser('new', help=u"create a new luiti project.")
# TODO remove argument key, just value only.
        parser_new.add_argument(
            '--project-name',
            help=u"必须指定项目名称，比如 --project-name=table_base", required=True, )

        parser_gene = subparsers.add_parser(
            'generate', help=u"generate a new luiti task python file.")

        parser_info = subparsers.add_parser('info', help=u"show a detailed task.")

        parser_clean = subparsers.add_parser(
            'clean', help=u"manage files that outputed by luiti tasks.")

        parser_run = subparsers.add_parser('run', help=u"run a luiti task.")

        parser_webui = subparsers.add_parser('webui', help=u"start a luiti DAG visualiser.")
        parser_webui.add_argument(
            '--luiti-packages', default="",
            help=u"Add luiti packages, separated by comma.", required=False, )
        parser_webui.add_argument(
            '--host', default="0.0.0.0",
            help=u"webui server host", required=False, )
        parser_webui.add_argument(
            '--port', default=8082,
            help=u"webui server port", required=False, )
        parser_webui.add_argument(
            '--background', default=False,
            help=u"run in a background mode", required=False, )

        for parser_1 in [parser_ls, parser_gene, parser_info,
                         parser_clean, parser_run, parser_webui]:
            parser_1.add_argument(
                "--project-dir",
                help=u"force use another project directory.",
                default=curr_dir)

            if parser_1 not in [parser_ls, parser_new, parser_webui]:
                parser_1.add_argument(
                    '--task-name',
                    help=u"必须指定任务名称，比如 --task-name=Redmine5954ParentReportWeek",
                    required=True, )

        parser_clean.add_argument(
            '--date-range',
            help=u"必须指定时间周期，比如 --date-range=20141001-20141015",
            required=True, )
        parser_clean.add_argument(
            '--dry',
            help=u"可以指定 **假运行** 模式 , 比如 --dry=true。",
            default=True, choices=[True, False], type=bool_type)
        parser_clean.add_argument(
            '--dep',
            help=u"可以指定 --dep=true，这样就会寻找依赖于当前Task之上的任务",
            default=False, choices=[True, False], type=bool_type)
        parser_clean.add_argument(
            '--force',
            help=u"强制删除文件",
            default=False, choices=[True, False], type=bool_type)

        parser_run.add_argument(
            '--date-value',
            help=u"必须指定具体时间，比如 --date-value=2014-09-01T00:00:01+08:00 。后面的时区信息是必须的。",
            required=True, )

        return parser_main

    @cached_property
    def subcommand(self):
        return sys.argv[1]

    @cached_property
    def args_main(self):
        return self.parser.parse_args()

    @cached_property
    def luiti_config(self):
        from .config import luiti_config
        return luiti_config

    @cached_property
    def curr_task(self):
        if ("task_name" in self.args_main) and (self.subcommand not in ["generate"]):
            return self.Loader.load_a_task_by_name(self.args_main.task_name)
        raise ValueError("Current subcommand [%s] dont support!" % self.subcommand)

    @cached_property
    def load_a_task_by_name(self):
        from .loader import Loader
        return Loader.load_a_task_by_name

    @cached_property
    def executor(self):
        return Executor(self)


class Executor(object):
    """
    Execute subcommand.
    """

    def __init__(self, cli):
        self.cli = cli
        self.args_main = self.cli.args_main
        self.luiti_config = self.cli.luiti_config

    def ls(self):
        from .table import Table
        from .lazy_data import ld
        Table.print_all_tasks(ld.result)

    def new(self):
        from .generate_from_templates import GenerateFromTemplates
        GenerateFromTemplates.new_a_project(self.args_main.project_name)

    def generate(self):
        from .generate_from_templates import GenerateFromTemplates
        GenerateFromTemplates.generate_a_task(self.args_main.task_name, self.luiti_config.curr_project_dir)

    def info(self):
        from .table import Table
        Table.print_task_info(self.cli.curr_task)

    def clean(self):
        from .dep import Dep
        from .lazy_data import ld
        from .files import Files
        from .table import Table

        dep_tasks_on_curr_task = [self.cli.curr_task]
        if self.args_main.dep:
            dep_tasks_on_curr_task = Dep.find_dep_on_tasks(
                self.cli.curr_task, ld.all_task_classes)
            dep_tasks_on_curr_task.insert(0, self.cli.curr_task)
        dep_file_to_task_instances = Files.get_all_date_file_to_task_instances(
            self.args_main.date_range, dep_tasks_on_curr_task)

        Table.print_files_by_task_cls_and_date_range(
            self.cli.curr_task,
            self.args_main,
            {
                "dep_file_to_task_instances": dep_file_to_task_instances,
                "task_classes_count": len(dep_tasks_on_curr_task),
                "dep_tasks_on_curr_task": dep_tasks_on_curr_task,
            })

        if self.args_main.dry:
            print "\nTips: just set --dry=false to soft-delete theses files.\n"
        else:
            Files.soft_delete_files(*dep_file_to_task_instances.keys())

    def run(self):
        from .sys_argv import SysArgv

        # 1. 把参数修复为 luigi 接受的参数，即把只有 luiti 依赖的参数去除。
        SysArgv.convert_to_luigi_accepted_argv(self.parser_main.subparsers)

        # 2. run it!
        # luigi.run(main_task_cls=curr_task)  # old style
        from luiti.schedule import SensorSchedule
        # luiti only need these two parameters. Other parameters can be passed by Shell environment variables.
        SensorSchedule.run(self.cli.curr_task, self.args_main.date_value)

    def webui(self):
        from ..luigi_extensions import luigi
        # add other luiti packages.
        for luiti_package in self.args_main.luiti_packages.split(","):
            luigi.plug_packages(luiti_package)

        from luiti.daemon import Server

        import daemon
        # TODO add logger and pidfile
        if self.args_main.background:
            ctx = daemon.DaemonContext()
            with ctx:
                Server(self.args_main.host, self.args_main.port).run()
        else:
            Server(self.args_main.host, self.args_main.port).run()


def bool_type(arg1):
    """ A Boolean convertor. """
    str_to_val_map = {"false": False, "true": True}
    val1 = str_to_val_map.get(arg1.lower(), None)
    assert val1 is not None, ValueError("[err value] %s" % arg1)
    return val1
