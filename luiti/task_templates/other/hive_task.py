# -*-coding:utf-8-*-

__all__ = ["HiveTask"]


from etl_utils import cached_property
from luigi.contrib.hive import HiveQueryTask

from ...utils import TargetUtils
from ...luigi_extensions import luigi, TaskBase


@luigi.as_a_luiti_task()
class HiveTask(HiveQueryTask, TaskBase):
    """
    Hive SQL Template, follows luiti `date_value` date mode。


    Implement:
      1. hive_db
      2. sql_main

    Example:
      from luiti.task_templates import HiveTask

      class AnotherHiveDay(HiveTask):
          root_dir = "/another/hive/result/"
          use_hive_db = "main_hive_database"

          @cached_property
          def sql_main(self):
              return "select * from example_table;"

    """

    run_mode = "mr_distribute"

    def output(self):
        """ Hive query default output directory has no _SUCCESS, not chunk filename is not MR style, see more details at `TargetUtils.hdfs_dir` . """
        assert "ValueError" not in self.data_file, self.data_file
        return TargetUtils.hdfs_dir(self.data_file)

    def query(self):
        sql = u"""
USE %s;
INSERT OVERWRITE DIRECTORY "%s" %s
""".replace("\n", " ") % (self.use_hive_db, self.data_file, self.sql_main.strip())

        if self.run_mode == "mr_distribute":
            print "[info.luiti] run Hive SQL := %s" % sql

        return sql.strip()

    @cached_property
    def data_root(self):
        raise ValueError("Old API. Please use luiti's standard property `root_dir` instead.")

    @cached_property
    def root_dir(self):
        # or a cached_property
        if self.__class__.data_root not in [NotImplementedError, ValueError]:
            return self.data_root  # from instance
        raise ValueError

    @cached_property
    def use_hive_db(self):
        if self.hive_db is not NotImplementedError:
            return self.hive_db
        raise ValueError

    # Deprecated API, use `use_hive_db` instead.
    hive_db = NotImplementedError

    @cached_property
    def sql_main(self):
        """
        Need to implemented in subclass
        """
        raise ValueError
