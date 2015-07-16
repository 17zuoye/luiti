# -*-coding:utf-8-*-

"""
A DAG timely visualiser.

Draw DAG tasks under selected parameters.
"""

from __future__ import unicode_literals

__all__ = ["Server"]

from etl_utils import cached_property
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


class Server(object):
    """ A tornado server.  """

    welcome_doc = u"""
( \      |\     /|\__   __/\__   __/\__   __/
| (      | )   ( |   ) (      ) (      ) (
| |      | |   | |   | |      | |      | |
| |      | |   | |   | |      | |      | |
| |      | |   | |   | |      | |      | |
| (____/\| (___) |___) (___   | |   ___) (___
(_______/(_______)\_______/   )_(   \_______/
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

        # Fix cant open http://0.0.0.0 on browser.
        self.url = "http://%s:%s" % (self.host.replace("0.0.0.0", "localhost"), self.port)

        print self.welcome_doc
        print "Luiti WebUI is mounted on %s" % self.url

    def run(self):
        """
        Runs one instance of the API server.
        """

        api_sockets = tornado.netutil.bind_sockets(self.port, address=self.host)
        server = tornado.httpserver.HTTPServer(self.app)
        server.add_sockets(api_sockets)

        logger.info("Scheduler starting up")
        tornado.ioloop.IOLoop.instance().start()

    @cached_property
    def app(self):
        """ return a API app instance. """
        settings = {
            "unescape": tornado.escape.xhtml_unescape,
            # "autoreload": True
        }

        return tornado.web.Application(web_handlers, **settings)
