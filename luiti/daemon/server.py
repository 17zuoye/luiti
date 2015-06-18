# -*-coding:utf-8-*-

"""
A DAG timely visualiser.

Draw DAG tasks under selected parameters.
"""

from __future__ import unicode_literals

import os
import sys
curr_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, curr_dir)

from copy import deepcopy

import logging
logger = logging.getLogger("luiti.server")

import pkg_resources
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.netutil
import tornado.web
import tornado.escape


# 1. Setup business package env
from luiti import manager, luigi, arrow, ArrowParameter
current_package_name = manager.luiti_config.get_curr_project_name()
current_package_path = manager.luiti_config.get_curr_project_path()
sys.path.insert(0, current_package_path)

import importlib
__init_luiti = importlib.import_module(current_package_name + ".luiti_tasks.__init_luiti")
luiti_visualiser_env = getattr(__init_luiti, "luiti_visualiser_env")


result = manager.load_all_tasks()

# list current package's related tasks, group by package name.


task_classes = [i1["task_cls"] for i1 in result["success"]]
task_class_names = [i1.__name__ for i1 in task_classes]
task_clsname_to_package = manager.PackageMap.task_clsname_to_package
task_clsname_to_package_name = {t1: p1.__name__ for t1, p1 in task_clsname_to_package.iteritems()}
task_package_names = sorted([p1.__name__ for p1 in set(task_clsname_to_package.values())])
package_to_task_clsnames = {package.__name__: list(task_clsnames) for package, task_clsnames in manager.PackageMap.package_to_task_clsnames.iteritems()}


def generate_task_doc(ti):
    """ Get task doc from class. """
    doc = (ti.task_class.__doc__ or "").strip()
    if isinstance(doc, str):
        doc = doc.decode("UTF-8")
    return doc


def generate_a_node(ti):
    result = {"id": ti.task_id,
              "label": ti.task_class.__name__,
              "group": ti.package_name,

              "detail": str(ti),
              "data_file": ti.data_file,
              "task_doc": generate_task_doc(ti),
              "task_file": ti.task_class.__module__.replace(".", "/") + ".py",
              "package_name": ti.package_name}
    result["size"] = 20
    return result


def generate_an_edge(from_task, to_task):
    arrows = "to"  # default
    if from_task == to_task:
        arrows = "self_to_self"

    result = {"id": from_task.task_id + " " + to_task.task_id,  # id is uniq.
              "from": from_task.task_id,
              "source_name": from_task.task_class.__name__,
              "to": to_task.task_id,
              "target_name": to_task.task_class.__name__,
              "strength": 1.0,
              "arrows": arrows}

    return result


def generate_edges_from_task_instances(task_instances):
    edges = list()
    for ti in task_instances:
        t2_in_requires = ti.requires()
        if not isinstance(t2_in_requires, list):
            t2_in_requires = [t2_in_requires]
        for t2 in t2_in_requires:
            if t2 is None:  # dep on none tasks
                continue
            if t2 not in task_instances:
                continue
            edges.append(generate_an_edge(t2, ti))
    return edges


def split_edges_into_groups(edges, nodes, task_instances):
    edges = deepcopy(edges)
    groups = list()  # element is set

    # make sure every node appear, even has not link to other tasks.
    for ti in task_instances:
        edges.append(generate_an_edge(ti, ti))

    # 1. first time, divid edges into groups.
    for edge in edges:
        is_in_current_groups = False
        for group in groups:
            if (edge["from"] in group) or (edge["to"] in group):
                is_in_current_groups = True
                group.add(edge["from"])
                group.add(edge["to"])
        if is_in_current_groups is False:
            groups.append(set([edge["from"], edge["to"]]))

    # 2. second time, merge groups that has common tasks
    # iterate to reduce redudant group
    result = list()
    for group1 in groups:
        append_idx = None
        for idx2, group2 in enumerate(result):
            if len(group1 & group2) > 0:
                append_idx = idx2
                break
        if append_idx is None:
            result.append(group1)
        else:
            result[append_idx] = result[append_idx] | group1

    result = sorted(result, key=lambda i1: (-len(i1), i1))
    return result


def generate_current_env():
    # yesterday
    sample_day = ArrowParameter.now().replace(days=-1).floor("day")

    accepted_date_values = sorted(map(str, arrow.Arrow.range("day", ArrowParameter.get(luiti_visualiser_env["date_begin"]), ArrowParameter.get(luiti_visualiser_env["date_end"]))))

    config = {
        "accepted_params": {
            "date_value": accepted_date_values,
            "task_cls": task_class_names,
            "luiti_package": task_package_names,
        }
    }

    current_params = {
        "date_value": str(sample_day),
    }

    for task_param, task_param_opt in luiti_visualiser_env["task_params"].iteritems():
        config["accepted_params"][task_param] = task_param_opt["values"]
        current_params[task_param] = task_param_opt["default"]

    task_instances = map(lambda i1: i1(date_value=sample_day), task_classes)
    selected_task_instances = filter(lambda ti: ti.package_name in luiti_visualiser_env["package_config"]["defaults"], task_instances)

    nodes = ([generate_a_node(ti) for ti in selected_task_instances])
    nodeid_to_node_dict = {node["id"]: node for node in nodes}

    edges = generate_edges_from_task_instances(selected_task_instances)

    nodes_groups = split_edges_into_groups(edges, nodes, selected_task_instances)
    nodes_groups_in_view = [sorted(list(nodes_set)) for nodes_set in nodes_groups]

    return {
        "config": config,

        "title": __doc__.strip().split("\n")[0],
        "readme": __doc__,
        "current_params": current_params,
        "luiti_visualiser_env": luiti_visualiser_env,

        "task_class_names": task_class_names,
        "task_package_names": task_package_names,
        "task_clsname_to_package_name": task_clsname_to_package_name,
        "package_to_task_clsnames": package_to_task_clsnames,

        "nodes": nodes,
        "edges": edges,
        "nodes_groups": nodes_groups_in_view,
        "nodeid_to_node_dict": nodeid_to_node_dict,
    }


class IndexHandler(tornado.web.RequestHandler):

    def get(self):
        # one query key has multiple values
        """
        date_values = map(lambda i1: ArrowParameter.get(i1), self.get_query_arguments("date_value"))
        gids = map(int, self.get_query_arguments("gid"))
        subjects = self.get_query_arguments("subject")
        """

        self.render("index.html")

    def get_template_path(self):
        return pkg_resources.resource_filename(__name__, "../webui")


class InitDataHandler(tornado.web.RequestHandler):

    def get(self):
        self.write(generate_current_env())


def app(scheduler):
    settings = {
        "unescape": tornado.escape.xhtml_unescape,
        "autoreload": True
    }

    handlers = [
        (r'/luiti_visualiser/(.*)', tornado.web.StaticFileHandler, {'path': os.path.join(os.path.dirname(os.path.abspath(__file__)), "luiti_visualiser")}),
        (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': os.path.join(luigi.__path__[0], "static")}),
        (r'/luiti/dag_visualiser', IndexHandler, {}),
        (r'/luiti/dag_visualiser/init_data.json', InitDataHandler, {}),
        (r'/', tornado.web.RedirectHandler, {"url": "/luiti/dag_visualiser"})
    ]
    api_app = tornado.web.Application(handlers, **settings)
    return api_app


def _init_api(scheduler, api_port=None, address=None):
    api_app = app(scheduler)
    api_sockets = tornado.netutil.bind_sockets(api_port, address=address)
    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    # Return the bound socket names.  Useful for connecting client in test scenarios.
    return [s.getsockname() for s in api_sockets]


def run(api_port=8082, address=None, scheduler=None):
    """
    Runs one instance of the API server.
    """
    _init_api(scheduler, api_port, address)

    # prune work DAG every 60 seconds
    # pruner = tornado.ioloop.PeriodicCallback(scheduler.prune, 60000)
    # pruner.start()

    logger.info("Scheduler starting up")
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    run()
