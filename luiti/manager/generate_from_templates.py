#-*-coding:utf-8-*-

__all__ = ['GenerateFromTemplates']

import os
from inflector import Inflector
from .config import luiti_config


class GenerateFromTemplates(object):

    @staticmethod
    def new_a_project(project_name):
        pass
# TODO



    @staticmethod
    def generate_a_task(task_name):
        path = os.path.join('luiti_tasks', Inflector().underscore(task_name) + ".py")
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

""".strip()  % (Inflector().titleize(project_name), )




""" 2. Task """
a_task_template = lambda task_clsname: u"""
#-*-coding:utf-8-*-

from luiti import *

@luigi.ref_tasks()
class %s(%s):

    root_dir = "/foobar"
""".strip() % (task_clsname, luiti_config.get_time_task(task_clsname), )


def write_content_to_file(content, path):
    if os.path.exists(path):
        raise ValueError("path [%s] is already exists!" % path)

    f1 = open(path, 'w')
    f1.write(content.encode("UTF-8"))
    f1.close()

    return content
