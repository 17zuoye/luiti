# -*- coding: utf-8 -*-

import os
import sys
RootDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RootDir)
os.environ['LUIGI_CONFIG_PATH'] = RootDir + '/tests/client.cfg'

import unittest

# add egg libarary
# import pdb
# pdb.set_trace()
sys.path.insert(0, os.path.join(
    RootDir, "tests/zip_package_by_luiti"))

# 1. change to work dir
project_dir = RootDir + "/tests/project_A"

# 3. setup tests variables
from luiti import manager, luigi, ArrowParameter
luiti_config = luigi.luiti_config  # make a ref
day_str = "2014-09-01T00:00:00+08:00"  # ISO 8601 format
day_arrow = ArrowParameter.get(day_str)


class TestLuiti(unittest.TestCase):

    def setUp(self):
        # let luiti find `luiti_tasks` dir
        os.chdir(project_dir)

    def test_ref_tasks(self):
        ADay = manager.load_a_task_by_name("ADay")
        BDay = manager.load_a_task_by_name("BDay")
        CDay = manager.load_a_task_by_name("CDay")

        ADay_task = ADay(day_arrow)

        self.assertEqual(ADay_task.BDay, BDay)
        self.assertEqual(ADay_task.CDay, CDay)

        self.assertEqual(ADay_task.count, 1)
        self.assertEqual(ADay_task.BDay_task.count, 2)
        self.assertEqual(ADay_task.CDay_task.count, 3)
        self.assertEqual(ADay_task.total_count, 6)

        self.assertEqual(ADay_task.date_value, ADay_task.BDay_task.date_value)
        self.assertEqual(ADay_task.date_value, ADay_task.CDay_task.date_value)

    def test_multiple_luiti_tasks(self):
        DDay = manager.load_a_task_by_name("DDay")
        HDay = manager.load_a_task_by_name("HDay")

        DDay_task = DDay(day_arrow)
        self.assertEqual(DDay_task.HDay, HDay)
        self.assertEqual(DDay_task.total_count, 12)

        # hash is luigi's test task unique method
        method = hash
        self.assertEqual(method(DDay_task.HDay_task), method(HDay(day_arrow)))

    def test_serialize_and_unserialize(self):
        """
        Fix multiple projects use the same `luiti_tasks` namespace.

        But if there are same directory(e.g. ./templetes`  under the
         `luiti_tasks`, and luiti can't figure out the right choice.
        """
        import pickle
        import cPickle

        def serialize_and_unserialize_a_task_instance(cls_name, serialize):
            task_cls = manager.load_a_task_by_name(cls_name)
            task_instance = task_cls(day_arrow)

            task_instance_2 = serialize.loads(serialize.dumps(task_instance))
            # already set when in serialize.laod
            package_name_2 = getattr(task_instance_2, "package_name")
            self.assertEqual(package_name_2, "project_A")

            self.assertEqual(hash(task_instance), hash(task_instance_2))

            for ref_task_name_3 in task_cls._ref_tasks:
                self.assertEqual(
                    getattr(task_instance, ref_task_name_3),
                    getattr(task_instance_2, ref_task_name_3))
                self.assertEqual(
                    hash(getattr(task_instance, ref_task_name_3+"_task")),
                    hash(getattr(task_instance_2, ref_task_name_3+"_task")))

        serialize_and_unserialize_a_task_instance('ADay', pickle)
        serialize_and_unserialize_a_task_instance('ADay', cPickle)
        serialize_and_unserialize_a_task_instance('DDay', pickle)
        serialize_and_unserialize_a_task_instance('DDay', cPickle)

        self.assertEqual(luiti_config.curr_project_name, "project_A")

    def test_plug_packages(self):
        global manager, luigi
        package_names = [i1.__name__ for i1
                         in luiti_config.luiti_tasks_packages]
        self.assertTrue("project_A" in package_names)
        self.assertTrue("project_B" in package_names)

        self.assertTrue("ADay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("BDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("CDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("DDay" in manager.PackageMap.task_clsname_to_package)
        self.assertTrue("HDay" in manager.PackageMap.task_clsname_to_package)

    def test_run_python_on_distributed_system(self):
        # 1. setup env
        import luigi.hadoop
        tar_dir = "/tmp/luiti_tests/tmp"
        tar_name = "project_A.tar"
        tar_file = tar_dir + "/" + tar_name
        os.system("mkdir -p %s" % tar_dir)

        DDay = manager.load_a_task_by_name("DDay")
        DDay_task = DDay("2014-09-01")

        # 2. package it
        import luiti
        import etl_utils
        import zip_package_by_luiti
        # mimic luigi.hadoop.create_packages_archive
        new_packages = list(
            luiti_config.luiti_tasks_packages) + \
            [__import__(DDay_task.__module__, None, None, 'dummy')] + \
            [luigi, luiti, etl_utils, zip_package_by_luiti]
        luigi.hadoop.create_packages_archive(new_packages, tar_file)

        # 3. unpackage it
        #    mimic luigi.mrrunner.Runner.extract_packages_archive
        os.chdir(tar_dir)
        import tarfile
        tar = tarfile.open(tar_name)
        for tarinfo in tar:
            tar.extract(tarinfo)
        tar.close()

        # 4. test
        unziped_items = os.listdir('.')
        self.assertTrue("etl_utils" in unziped_items)
        self.assertTrue("luigi" in unziped_items)
        self.assertTrue("luiti" in unziped_items)
        self.assertTrue("project_A" in unziped_items)
        self.assertTrue("project_B" in unziped_items)

        self.assertTrue("zip_package_by_luiti" in unziped_items)
        self.assertTrue("subfold" not in unziped_items)  # it's a subfold

        # 5. clean up
        os.system("rm -rf /tmp/luiti_tests")

    def test_mr_local(self):
        from luiti import TaskDayHadoop, luigi, \
            StaticFile, IOUtils, json, MRUtils

        class JsonsData(StaticFile):
            root_dir = "/"  # fix luiti requirement
            filepath = os.path.join(RootDir, "tests/jsons_data/mr_local.json")

            def output(self):
                assert self.filepath, u"Please assign `filepath` !"
                return IOUtils.local_target(self.filepath)

        @luigi.mr_local()
        class MrLocalDay(TaskDayHadoop):
            root_dir = "/"  # fix luiti requirement
            filepath = os.path.join(
                RootDir, "tests/jsons_data/mr_local_output.json")

            def requires(self):
                return [JsonsData(), ]  # as a list

            def output(self):
                return IOUtils.local_target(self.filepath)

            def mapper(self, line1):
                item1 = MRUtils.json_parse(line1)
                yield item1["uid"], item1

            def reducer(self, uid_1, vals_1):
                yield "", MRUtils.str_dump(
                    {"uid": uid_1, "count": len(vals_1)})

        t1 = MrLocalDay(day_arrow)
        t1.run()
        lines = file(t1.filepath).read().split("\n")
        result = sorted([json.loads(line1) for line1 in lines if line1])

        self.assertEqual(
            result,
            [
                {"count": 1, "uid": 2},
                {"count": 1, "uid": 3},
                {"count": 3, "uid": 1}])
        os.system("rm -f %s" % t1.filepath)

    def test_egg_zip_python_package(self):
        """
        TODO improve tests, now maybe it's meanless.
        """
        ImportPackagesDay = manager.load_a_task_by_name("ImportPackagesDay")
        self.assertTrue(
            "zip_package_by_luiti" in
            ImportPackagesDay(day_arrow).egg_library.__path__[0])
        import zip_package_by_luiti
        import zip_package_by_luiti.subfold
        zip_package_by_luiti.subfold


if __name__ == '__main__':
    unittest.main()
