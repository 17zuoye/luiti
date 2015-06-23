# -*-coding:utf-8-*-

__all__ = ["web_handlers"]

import pkg_resources
import tornado.web

from .web_assets import assets_main_dir, assets_thirdparty_dir
from .package_task_management import PTM

from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter


class IndexHandler(tornado.web.RequestHandler):

    def get(self):
        # one query key has multiple values
        self.render("index.html")

    def get_template_path(self):
        return pkg_resources.resource_filename(__name__, "../webui")


class InitDataHandler(tornado.web.RequestHandler):

    def get(self):
        params = self.request.query_arguments
        data = PTM.get_env(params)

        self.write(data)


class CodeShowHandler(tornado.web.RequestHandler):

    class CodeCache(dict):

        def __missing__(self, source_file):
            source_code = file(source_file).read()
            formatter = HtmlFormatter(linenos=True)

            path_html = u"""<div>source_file: %s</div>""" % source_file
            code_html = highlight(source_code, PythonLexer(), formatter)
            css_html = u"""<style type="text/css">%s</style>""" % formatter.get_style_defs('.highlight')

            return path_html + code_html + css_html

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
    (r'/bower_components/(.*)', tornado.web.StaticFileHandler, {'path': assets_thirdparty_dir}),
    (r'/assets/(.*)', tornado.web.StaticFileHandler, {'path': assets_main_dir}),
    (r'/luiti/code/([^/]+)/([^/]+)', CodeShowHandler, {}),
    (r'/luiti/dag_visualiser', IndexHandler, {}),
    (r'/luiti/dag_visualiser/init_data.json', InitDataHandler, {}),
    (r'/', tornado.web.RedirectHandler, {"url": "/luiti/dag_visualiser"})
]
