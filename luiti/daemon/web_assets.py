# -*-coding:utf-8-*-

__all__ = ["assets_main_dir", "assets_thirdparty_dir"]


import os


luiti_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

assets_main_dir = os.path.join(luiti_dir, "webui/assets")
assets_thirdparty_dir = os.path.join(luiti_dir, "webui/bower_components")

assert os.path.isdir(assets_main_dir), "%s is not exists!" % assets_main_dir
assert os.path.isdir(assets_thirdparty_dir), "%s is not exists!" % assets_thirdparty_dir
