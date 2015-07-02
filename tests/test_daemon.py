# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.task_templates import TaskDay
from luiti import luigi, config
from luiti.luigi_extensions import ArrowParameter
current_time = ArrowParameter.now()


def setup_luiti_env_in_test():
    # setup luiti package path
    sys.path.insert(0, os.path.join(root_dir, "tests"))
    sys.path.insert(0, os.path.join(root_dir, "tests/zip_package_by_luiti"))

    # setup env
    config.curr_project_dir = os.path.join(root_dir, "tests/project_A")


class FoobarDay(TaskDay):
    root_dir = "/tmp"
    foobar = luigi.Parameter()


class TestDaemon(unittest.TestCase):

    def test_main(self):
        from luiti.daemon import run
        run

    def test_utils(self):
        from luiti.daemon.utils import TaskStorageSet, TaskStorageDict

        current_time = ArrowParameter.now()
        t1 = FoobarDay(date_value=current_time, foobar="1")
        t2 = FoobarDay(date_value=current_time, foobar="1")

        s1 = TaskStorageSet()
        s1.add(t1)
        self.assertEqual(len(s1), 1)
        s1.add(t2)
        self.assertEqual(len(s1), 1)
        self.assertTrue(t1 in s1)
        self.assertTrue(t2 in s1)

        d1 = TaskStorageDict()
        d1[t1].add(t1)
        d1[t2].add(t2)
        self.assertEqual(len(d1), 1)
        self.assertEqual(len(d1[t1]), 1)

    def test_create_task(self):
        """ test tasks are the same one completely. """
        from luiti.daemon.query_engine.create_task import CreateTask

        t1 = CreateTask.new(FoobarDay, {"date_value": current_time, "foobar": 1})
        t2 = CreateTask.new(FoobarDay, {"date_value": current_time, "foobar": 1})

        self.assertEqual(t1, t2)
        self.assertEqual(t1.task_id, t2.task_id)
        self.assertEqual(hash(t1), hash(t2))

    def test_VisualiserEnvTemplate(self):
        from luiti.daemon import VisualiserEnvTemplate

        env = VisualiserEnvTemplate({
            "file_web_url_prefix": "http://HUE:8888/filebrowser/#/",
            "date_begin": lambda: "2014-09-01",
            "additional_task_parameters": {
                "subject": {
                    "values": ["english", "math"],
                    "default": "english",
                }
            },
            "package_config": {
                "defaults": ["foo", "bar", ],
            }
        }).data

        self.assertTrue(isinstance(env, dict), "raw dict")
        self.assertEqual(env["date_begin"], "2014-09-01", "eval lambda")

    def test_ptm(self):
        from luiti.daemon.ptm import PTM

        setup_luiti_env_in_test()

        self.assertEqual(PTM.current_package_name, "project_A")
        self.assertEqual(PTM.current_package_path, config.curr_project_dir)
        self.assertTrue(isinstance(PTM.current_luiti_visualiser_env, dict))

        self.assertEqual(len(PTM.task_classes), 8)
        self.assertEqual(PTM.task_class_names, ['ADay', 'BDay', 'CDay', 'DDay', 'FoobarDay', 'HDay', 'ImportPackagesDay', 'MultipleDependentDay'])

        self.assertEqual(len(PTM.task_clsname_to_package), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package["ADay"].__name__, "project_A")

        self.assertEqual(len(PTM.task_clsname_to_source_file), len(PTM.task_classes))
        self.assertTrue("project_A/luiti_tasks/a_day.py" in PTM.task_clsname_to_source_file["ADay"])

        self.assertEqual(len(PTM.task_clsname_to_package_name), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package_name["ADay"], "project_A")

        self.assertEqual(PTM.task_package_names, ["project_A", "project_B"])

        self.assertEqual(PTM.package_to_task_clsnames, {
            'project_B': ['HDay'],
            'project_A': ['ADay', 'BDay', 'CDay', 'DDay', 'FoobarDay', 'ImportPackagesDay', 'MultipleDependentDay'],
        })

    def test_Graph(self):
        from luiti.daemon.query_engine.builder import QueryBuilder
        from luiti.daemon.ptm import PTM
        from luiti.daemon.graph import Graph, Utils
        setup_luiti_env_in_test()

        builder = QueryBuilder(PTM, {"luiti_package": ["project_A", "project_B"]})
        builder.total_task_instances
        builder.selected_task_instances

        result = Graph.analysis_dependencies_between_nodes(builder.selected_task_instances, ["project_A"])
        self.assertEqual(sorted(result.keys()), ["json", "python"])

        d1 = result["json"]["upons"]["direct"]
        c_instance = filter(lambda i1: "CDay(" in i1, d1.keys())[0]
        c_deps = d1[c_instance]
        self.assertEqual(len(c_deps), 1)
        self.assertTrue("ADay" in repr(c_deps))

        d2 = result["json"]["requires"]["total"]
        a_instance = filter(lambda i1: "ADay(" in i1, d1.keys())[0]
        a_deps = d2[a_instance]
        self.assertEqual(len(a_deps), 2)
        self.assertTrue("BDay(" in repr(a_deps))
        self.assertTrue("CDay(" in repr(a_deps))
        # TODO why FoobarDay dont appear here?

        Utils

    def test_query_engine_params(self):
        from luiti.daemon.query_engine.params import Params

        default_query = {"date_value": "2014-09-01", "language": "Chinese", "gender": "Male"}
        selected_query = {"date_value": "2015-07-02", "city": ["Beijing", "London"]}

        params_array = Params.build_params_array(default_query, selected_query)

        base_expected_opt = {
            "date_value": ArrowParameter.get("2015-07-02"),
            "city": ["Beijing", "London"],
            "language": "Chinese",
            "gender": "Male",
        }
        expected_opts = [
            dict(base_expected_opt.items() + {"city": "Beijing"}.items()),
            dict(base_expected_opt.items() + {"city": "London"}.items()),
        ]
        self.assertEqual(params_array, expected_opts)

    def test_QueryBuilder(self):
        from luiti.daemon.query_engine.builder import QueryBuilder
        from luiti.daemon.ptm import PTM
        setup_luiti_env_in_test()

        builder = QueryBuilder(PTM, {})

        self.assertEqual(builder.date_begin, "2014-09-01")
        self.assertEqual(builder.date_end, builder.yesterday_str)
        self.assertEqual(builder.accepted_params, {'language': {'default': 'English', 'values': ['Chinese', 'English']}})
        self.assertTrue(len(builder.accepted_query_params["date_value"]) > 200)  # many days
        self.assertEqual(len(builder.accepted_query_params), 3)

        self.assertEqual(sorted(builder.default_query.keys()), ['date_value', 'language'])
        self.assertEqual(builder.default_query["language"], "English")

        self.assertEqual(sorted(builder.selected_query.keys()), ["date_value", "language", "luiti_package"])
        self.assertEqual(builder.selected_query["luiti_package"], ["project_A"])  # default

        self.assertEqual(list(builder.selected_task_cls_names), [])

        self.assertEqual(len(builder.total_task_instances), 8)
        self.assertEqual(len(builder.selected_task_instances), 7)
        HDay_task = list(set(builder.total_task_instances) - set(builder.selected_task_instances))[0]
        self.assertEqual(HDay_task.task_clsname, "HDay", "HDay is project_B, and project_B in not selected")

        self.assertTrue("requires" in builder.graph_infos_data["json"])
        # ... to be continued.


if __name__ == '__main__':
    unittest.main()
