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
from tornado.log import enable_pretty_logging
enable_pretty_logging()


import logging
logger = logging.getLogger("luiti.server")


# 1. Setup business package env
# list current package's related tasks, group by package name.
from .handlers import web_handlers


def app():
    settings = {
        "unescape": tornado.escape.xhtml_unescape,
        # "autoreload": True
    }

    api_app = tornado.web.Application(web_handlers, **settings)
    return api_app


def run(address, api_port):
    """
    Runs one instance of the API server.
    """
    api_app = app()

    api_sockets = tornado.netutil.bind_sockets(api_port, address=address)
    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    logger.info("Scheduler starting up")
    tornado.ioloop.IOLoop.instance().start()
