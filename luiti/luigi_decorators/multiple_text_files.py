# -*-coding:utf-8-*-

__all__ = ["multiple_text_files"]

import os
import commands
from etl_utils import cached_property
from ..utils import CommandUtils
import luigi


def multiple_text_files(opts=dict()):
    """
    Let current task class's result can support outputing into multiple files.

    Usage:

    ```python
    @luigi.multiple_text_files
    class ManAndWomanDay(TaskDayHadoop):
        def mapper(self, line1):
            item1 = MRUtils.json_parse(line1)
            yield item1['uid'], item1

        def reducer(self, uid1, vals_1):
            for item1 in vals_1:
                yield item1["gender"], MRUtils.str_dump(item1)
    ```

    So above code separate man and woman into two files. File name such as
    1. man_and_woman_day.json/man
    2. man_and_woman_day.json/woman

    But not the default one
    1. man_and_woman_day.json/part-00000

    WARN:
        when use `@luigi.multiple_text_files`, consider to wrap subfolders with
        StaticFile task class.
    """
    def func(task_cls):
        cjc = CompileJavaCode()

        def compile_java_code(self):
            """ compile java code dynamically. """
            if not os.path.exists(cjc.target_jar):
                CommandUtils.execute(cjc.compile_cmd)

        setattr(task_cls, "output_format", cjc.output_format)
        setattr(task_cls, "libjars", [cjc.target_jar, ])
        setattr(task_cls, "compile_java_code", compile_java_code)
        return task_cls

    # Comptible with old API.
    if isinstance(opts, dict):
        return func
    if issubclass(opts, luigi.Task):
        return func(opts)
    raise ValueError(opts)


class CompileJavaCode(object):
    """
    assemble jar.
    """

    java_namespace = "com.voxlearning.bigdata.MrOutput"
    java_lib = "MultipleTextFiles"
    output_format = ".".join([java_namespace, java_lib])
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @cached_property
    def java_file(self):
        return self.java_lib + ".java"

    @cached_property
    def target_class(self):
        return self.java_lib + ".class"

    @cached_property
    def target_jar(self):
        return os.path.join(self.root_dir, "java", self.java_lib + ".jar")

    @cached_property
    def compile_cmd(self):
        classes_dir = self.java_namespace.replace(".", "/")
        javac_cmd = commands.getoutput("which javac")
        java_classpath = commands.getoutput("hadoop classpath")
        jar_cmd = commands.getoutput("which jar")

        compile_cmd = ";\n".join([
            # no absolute path, compact with java namespace.
            "cd %s/java" % self.root_dir,

            """%s -classpath "%s" %s""" % (javac_cmd,
                                           java_classpath, self.java_file, ),
            "rm -rf %s" % classes_dir,
            "mkdir -p %s" % classes_dir,
            "cp %s %s" % (self.target_class, classes_dir),
            "%s cvf %s %s/*.class" % (jar_cmd, self.target_jar, classes_dir, ),
        ])
        return compile_cmd
