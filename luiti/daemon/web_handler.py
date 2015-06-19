# -*-coding:utf-8-*-

__all__ = ["api_handlers"]

import pkg_resources
import tornado.web

from .web_assets import assets_main_dir, assets_thirdparty_dir
from .package_task_management import PTM


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
        self.write(PTM.get_env())


api_handlers = [
    (r'/luiti/assets/thirdparty/(.*)', tornado.web.StaticFileHandler, {'path': assets_thirdparty_dir}),
    (r'/luiti/assets/main/(.*)', tornado.web.StaticFileHandler, {'path': assets_main_dir}),
    (r'/luiti/dag_visualiser', IndexHandler, {}),
    (r'/luiti/dag_visualiser/init_data.json', InitDataHandler, {}),
    (r'/', tornado.web.RedirectHandler, {"url": "/luiti/dag_visualiser"})
]
