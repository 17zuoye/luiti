# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest

from luiti.tests import date_begin


class TestLuitiUtils(unittest.TestCase):

    def test_main(self):
        from luiti import MongoImportTask

        class AnotherMongoDay(MongoImportTask):
            root_dir = "/tmp"

            mongodb_connection_address = ('192.168.20.111', 37001)
            database_name = "17zuoye_crm"
            collection_name = "teacher_report"
            tmp_filepath = "/foobar.json"

        mongo_task = AnotherMongoDay(date_value=date_begin)
        self.assertEqual(mongo_task.mongodb_connection_host, "192.168.20.111")
        self.assertEqual(mongo_task.mongodb_connection_port, 37001)
        self.assertEqual(mongo_task.mongoimport_command, "/usr/bin/mongoimport --host 192.168.20.111 --port 37001 --db 17zuoye_crm --collection teacher_report --file /foobar.json")


if __name__ == '__main__':
    unittest.main()
