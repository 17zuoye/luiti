# -*-coding:utf-8-*-

__all__ = ["web_handlers"]

from etl_utils import cached_property
import pkg_resources
import tornado.web

from .assets import assets_main_dir, assets_thirdparty_dir
from ..ptm import PTM
from ..query_engine import Query


class IndexHandler(tornado.web.RequestHandler):

    def get(self):
        # one query key has multiple values
        self.render("index.html")

    def get_template_path(self):
        return pkg_resources.resource_filename(__name__, "../../webui")


class InitDataHandler(tornado.web.RequestHandler):

    @cached_property
    def query_engine(self):
        return Query(PTM)

    def get(self):
        params = self.request.query_arguments
        data = self.query_engine.get_env(params)

        self.write(data)


class CodeShowHandler(tornado.web.RequestHandler):

    class CodeCache(dict):

        @cached_property
        def highlight(self):
            """ Lazy load pygments, so user dont need to load all daemon code. """
            import pygments
            from pygments.lexers import PythonLexer
            lexer = PythonLexer()

            def func(source_code):
                return pygments.highlight(source_code, lexer, self.formatter)
            return func

        @cached_property
        def formatter(self):
            from pygments.formatters import HtmlFormatter
            return HtmlFormatter(linenos=True)

        @cached_property
        def css_html(self):
            return u"""<style type="text/css">%s</style>""" % self.formatter.get_style_defs('.highlight')

        def __missing__(self, source_file):
            source_code = file(source_file).read()

            path_html = u"""<div>source_file: %s</div>""" % source_file
            code_html = self.highlight(source_code)

            body_html = path_html + code_html + self.css_html
            title = source_file.split("/")[-1]

            return u"""
            <html lang="en">
                <head>
                    <title>%s</title>
                </head>
                <body>
                    %s
                </body>
            </html>
            """ % (title, body_html)

    code_cache = CodeCache()

    def get(self, package_name, task_cls_name):
        # assert package and task exist!
        assert package_name in PTM.task_package_names
        assert task_cls_name in PTM.task_clsname_to_package

        # TODO add cache
        source_file = PTM.task_clsname_to_source_file[task_cls_name]
        source_code = self.code_cache[source_file]
        self.write(source_code)


web_handlers = [
    # make a static HTML vis URL
    (r'/luiti/bower_components/(.*)', tornado.web.StaticFileHandler, {'path': assets_thirdparty_dir}),
    (r'/luiti/assets/(.*)', tornado.web.StaticFileHandler, {'path': assets_main_dir}),

    (r'/luiti/code/([^/]+)/([^/]+)', CodeShowHandler, {}),
    (r'/luiti/dag_visualiser', IndexHandler, {}),
    (r'/luiti/init_data.json', InitDataHandler, {}),
    (r'/', tornado.web.RedirectHandler, {"url": "/luiti/dag_visualiser"})
]
