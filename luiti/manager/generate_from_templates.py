#-*-coding:utf-8-*-

__all__ = ['GenerateFromTemplates']

import os
from inflector import Inflector
from .config import luiti_config

join   = os.path.join
exists = os.path.exists


class GenerateFromTemplates(object):

    @staticmethod
    def new_a_project(project_name):
        project_name                 = Inflector().underscore(project_name)
        readme_path                  = join(project_name, "README.markdown")
        setup_path                   = join(project_name, "setup.py")
        package_dir                  = join(project_name, project_name)
        package_init                 = join(package_dir, "__init__.py")
        package_luiti_tasks_init     = join(package_dir, "luiti_tasks/__init__.py")
        package_luiti_tasks_luiti    = join(package_dir, "luiti_tasks/__init_luiti.py")

        write_content_to_file(a_project_readme(project_name),     readme_path)
        write_content_to_file(a_project_setup(project_name),      setup_path)
        write_content_to_file(u"",                                package_init)
        write_content_to_file(u"",                                package_luiti_tasks_init)
        write_content_to_file(a_project_init_luiti(),             package_luiti_tasks_luiti)

        # important files
        return [readme_path, setup_path, package_luiti_tasks_luiti]


    @staticmethod
    def generate_a_task(task_name):
        path = join('luiti_tasks', Inflector().underscore(task_name) + ".py")
        content = write_content_to_file(
                    a_task_template(Inflector().classify(task_name)),
                    path,
             )
        print "[info] write to %s . " % path
        return content



""" 1. Project """
a_project_readme = lambda project_name: u"""
%s
=======================

TODO ...
""".strip()  % (Inflector().titleize(project_name), )

a_project_setup = lambda project_name: u"""
#-*-coding:utf-8-*-

from setuptools import setup

setup(
    name="%s",
    version="0.0.1",
    packages=[
                "%s",
                "%s/luiti_tasks",
             ],
    zip_safe=False,
)
""".strip() % (project_name, project_name, project_name, )

a_project_init_luiti = lambda : u"""
#-*-coding:utf-8-*-

from luiti import *
luigi.plug_packages("package_a", "package_b==4.2")
""".strip()



""" 2. Task """
a_task_template = lambda task_clsname: u"""
#-*-coding:utf-8-*-

from luiti import *

@luigi.ref_tasks()
class %s(%s):

    root_dir = "/foobar"
""".strip() % (task_clsname, luiti_config.get_time_task(task_clsname), )


def write_content_to_file(content, path):
    if exists(path):
        raise ValueError("path [%s] is already exists!" % path)

    dir1 = os.path.dirname(path)
    if not exists(dir1): os.mkdir(dir1)

    f1 = open(path, 'w')
    f1.write(content.encode("UTF-8"))
    f1.close()

    print "[info] generate %s ." % path

    return content
