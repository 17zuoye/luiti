# -*-coding:utf-8-*-

"""
A DAG timely visualiser.

Draw DAG tasks under selected parameters.
"""

from __future__ import unicode_literals

__all__ = ["run"]

import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.netutil
import tornado.web
import tornado.escape

import logging
logger = logging.getLogger("luiti.server")


# 1. Setup business package env
# list current package's related tasks, group by package name.
from .web_handler import api_handlers


def app(scheduler):
    settings = {
        "unescape": tornado.escape.xhtml_unescape,
        # "autoreload": True
    }

    api_app = tornado.web.Application(api_handlers, **settings)
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
