#-*-coding:utf-8-*-

from ..time.task_base import *
from etl_utils import cached_property, process_notifier
from ...utils import CommandUtils, TargetUtils, MRUtils, HDFSUtils
import luigi
import os
import arrow
import json


class MongoImportTask(TaskBase):

    report_status_collection_name     = "report_status"
    report_status_namespace           = "latestCollection"
    report_name                       = NotImplementedError

    system_tmp = "/tmp" # default

    @cached_property
    def report_status_collection_model(self):
        return self.mongodb_connection[self.database_name][self.report_status_collection_name]

    @cached_property
    def data_file_collection_model(self):
        return self.mongodb_connection[self.database_name][self.collection_name]

    # 1. config
    @cached_property
    def source_task(self):
        raise NotImplementedError

    @cached_property
    def mongodb_connection(self):
        raise NotImplementedError

    @cached_property
    def database_name(self):
        raise NotImplementedError

    @cached_property
    def index_schema(self):
        raise NotImplementedError

    def convert_line_to_json(self, line1):
        classid_uid_1, val_1 = MRUtils.split_mr_kv(line1)
        return val_1

    def run_before_hook(self):
        pass
    def run_after_hook(self):
        pass
    def find_invalid_record(self, stat_1):
        return False

    # 2. common
    def requires(self):
        return [getattr(self, _ref_task_1)(self.date_value) for _ref_task_1 in self._ref_tasks]

    def run(self):
        self.run_before_hook()

        # 1. check is already done.
        if self.is_collection_exists(): return False

        # 2. check report status collection is valid
        if self.report_status_collection_model.count() == 0:
            self.report_status_collection_model.insert({self.report_status_namespace:{}})
        assert self.report_status_collection_model.count() == 1, "更新纪录 只能有一条！"

        # 3. output json with err
        data_file1   = self.source_task_instance.data_file
        source1      = luigi.HDFS(data_file1)
        tmp_file1    = open(self.tmp_filepath,    'w')
        tmp_errfile1 = open(self.tmp_errfilepath, 'w')

        for line1 in process_notifier(TargetUtils.line_read(source1), u"[read lines] %s" % source1):
            if len(line1) == 0: continue # meaningless data.
            stat_2 = self.convert_line_to_json(line1)
            if self.find_invalid_record(stat_2):
                tmp_errfile1.write(line1 + "\n")
            else:
                tmp_file1.write(json.dumps(stat_2) + "\n")
        tmp_file1.close()
        tmp_errfile1.close()

        # 4. upload to mongodb
        HDFSUtils.copyFromLocal(self.tmp_errfilepath, self.data_file_err)
        CommandUtils.execute(self.mongo_ensure_index)
        CommandUtils.execute(self.mongoimport_command)

        # 5. clean tmp
        CommandUtils.execute("rm -f %s" % self.tmp_filepath)
        CommandUtils.execute("rm -f %s" % self.tmp_errfilepath)

        # 6. update report status
        item1 = self.report_status_collection_model.find()[0]
        del item1['_id']
        item1[self.report_status_namespace][self.report_name] = {
                'collection_name' : self.collection_name,
                'updated_at'      : arrow.now().datetime,
            }
        self.report_status_collection_model.find_and_modify(
                query={},
                update={"$set": item1},
                full_response= True
            )

        self.run_after_hook()

        return True


    def is_collection_exists(self):
        return self.data_file_collection_model.count() > 0

    @cached_property
    def source_task_instance(self):
        return self.source_task(self.date_value)


    @cached_property
    def mongoimport_command(self):
        return "/usr/bin/mongoimport " + \
                  ("--host %s "        % self.mongodb_connection.host) + \
                  ("--port %s "        % self.mongodb_connection.port) + \
                  ("--db %s "          % self.database_name) + \
                  ("--collection %s "  % self.collection_name) + \
                  ("--file %s "        % self.tmp_filepath)

    @cached_property
    def mongo_ensure_index(self):
        if not isinstance(self.index_schema, (str, unicode)):
            self.index_schema = json.dumps(self.index_schema)
        js_str = "db.%s.ensureIndex(%s)" % (self.collection_name, self.index_schema)
        return self.mongo_eval(js_str)


    def mongo_eval(self, js_str):
        return "/usr/bin/mongo " + \
                  ("%s:%s/%s "        % (self.mongodb_connection.host, self.mongodb_connection.port, self.database_name)) + \
                  ("--eval \"%s\" "    % js_str)

    @cached_property
    def collection_name(self):
        """ e.g. redmine5954_parent_report_week_20140901 """
        return self.data_name + "_" + self.date_value.strftime("%Y%m%d")

    @cached_property
    def tmp_filepath(self):
        return self.tmp_dir + "/" + self.date_value.strftime("%Y%m%d")

    @cached_property
    def tmp_errfilepath(self):
        return self.tmp_filepath + ".err"

    @cached_property
    def data_file_err(self):
        return self.data_file + ".err"

    @cached_property
    def tmp_dir(self):
        dir1 = self.system_tmp + "/" + self.task_class.__name__
        os.system("mkdir -p %s" % dir1)
        return dir1
