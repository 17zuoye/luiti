# -*-coding:utf-8-*-

__all__ = ["multiple_text_files"]

import os
import commands
from ..utils import CommandUtils


def multiple_text_files(task_cls):
    """
    让当前 MapReduce 任务的结果可以输出到多个文件。

     使用说明:

    ```python
    @luigi.MultipleTextOutputFormat
    class ManAndWomanDay(TaskDayHadoop):
        def mapper(self, line1):
            item1 = MRUtils.json_parse(line1)
            yield item1['uid'], item1

        def reducer(self, uid1, vals_1):
            for item1 in vals_1:
                yield item1["gender"], MRUtils.str_dump(item1)
    ```

    以上代码就把男性和女性分流到两个文件了。文件名类似
    1. man_and_woman_day.json/man
    2. man_and_woman_day.json/woman

    而不是默认输出的
    1. man_and_woman_day.json/part-00000

    WARN:
        when use `@luigi.multiple_text_files`, consider to wrap subfolders with
        StaticFile task class.
    """

    java_namespace = "com.voxlearning.bigdata.MrOutput"
    java_lib = "MultipleTextFiles"
    output_format = ".".join([java_namespace, java_lib])

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    java_file = java_lib + ".java"
    target_class = java_lib + ".class"
    target_jar = os.path.join(root_dir, "java", java_lib + ".jar")

    def compile_java_code(self):
        """ compile java code dynamically. """
        if os.path.exists(target_jar):
            return False

        classes_dir = java_namespace.replace(".", "/")
        javac_cmd = commands.getoutput("which javac")
        java_classpath = commands.getoutput("hadoop classpath")
        jar_cmd = commands.getoutput("which jar")

        compile_cmd = ";\n".join([
            # no absolute path, compact with java namespace.
            "cd %s/java" % root_dir,

            """%s -classpath "%s" %s""" % (javac_cmd,
                                           java_classpath, java_file, ),
            "rm -rf %s" % classes_dir,
            "mkdir -p %s" % classes_dir,
            "cp %s %s" % (target_class, classes_dir),
            "%s cvf %s %s/*.class" % (jar_cmd, target_jar, classes_dir, ),
        ])

        CommandUtils.execute(compile_cmd)

    setattr(task_cls, "output_format", output_format)
    setattr(task_cls, "libjars", [target_jar, ])
    setattr(task_cls, "compile_java_code", compile_java_code)
    return task_cls
