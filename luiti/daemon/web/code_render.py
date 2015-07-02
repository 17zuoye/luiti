# -*-coding:utf-8-*-

__all__ = ["CodeRender"]

from etl_utils import cached_property


class CodeRender(dict):
    """ Highlight luiti task code written in Python. """

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
