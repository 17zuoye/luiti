# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

import unittest

from luiti.task_templates import TaskDay
from luiti import luigi
from luiti.luigi_extensions import ArrowParameter
current_time = ArrowParameter.now()

from luiti.tests import SetupLuitiPackages, date_begin
config = SetupLuitiPackages.config


class FoobarDay(TaskDay):
    root_dir = "/tmp"
    foobar = luigi.Parameter()


class TestDaemon(unittest.TestCase):

    def test_Server(self):
        from luiti.daemon import Server
        s1 = Server("0.0.0.0", 8082)
        self.assertEqual(s1.url, "http://localhost:8082")
        self.assertTrue(s1.app)

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
        from luiti.utils import VisualiserEnvTemplate

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

        self.assertEqual(PTM.current_package_name, "luiti_summary")
        self.assertTrue(PTM.current_package_path in config.curr_project_dir)
        self.assertTrue(isinstance(PTM.current_luiti_visualiser_env, dict))

        self.assertEqual(len(PTM.task_classes), 7 + 8)
        webui_task_clses = ['BetaReportDay', 'CleanWebLogDay', 'CounterVisitorByBrowserDay', 'CounterVisitorByRegionDay', 'CounterVisitorDay', 'DumpBrowserMapDay', 'DumpWebLogDay']
        self.assertTrue(len(set(webui_task_clses) - set(PTM.task_class_names)) == 0, "is the subset.")

        self.assertEqual(len(PTM.task_clsname_to_package), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package["DumpBrowserMapDay"].__name__, "luiti_dump")

        self.assertEqual(len(PTM.task_clsname_to_source_file), len(PTM.task_classes))
        self.assertTrue("luiti_dump/luiti_tasks/dump_browser_map_day.py" in PTM.task_clsname_to_source_file["DumpBrowserMapDay"])

        self.assertEqual(len(PTM.task_clsname_to_package_name), len(PTM.task_classes))
        self.assertEqual(PTM.task_clsname_to_package_name["DumpBrowserMapDay"], "luiti_dump")

        self.assertEqual(PTM.task_package_names, ['luiti_clean', 'luiti_dump', 'luiti_middle', 'luiti_summary', "project_A", "project_B"])

        self.assertEqual(PTM.package_to_task_clsnames, {
            'luiti_clean': ['CleanWebLogDay'],
            'luiti_dump': ['DumpBrowserMapDay', 'DumpWebLogDay'],
            'luiti_middle': ['CounterVisitorByBrowserDay',
                             'CounterVisitorByRegionDay',
                             'CounterVisitorDay'],
            'luiti_summary': ['BetaReportDay'],
            'project_A': ['ADay',
                          'BDay',
                          'CDay',
                          'DDay',
                          'FoobarDay',
                          'ImportPackagesDay',
                          'MultipleDependentDay'],
            'project_B': ['HDay']
        })

    def test_Graph(self):
        from luiti.daemon.query_engine.builder import QueryBuilder
        from luiti.daemon.ptm import PTM
        from luiti.daemon.graph import Graph, Utils

        builder = QueryBuilder(PTM, {"luiti_package": []})  # default select all packages.
        builder.total_task_instances
        builder.selected_task_instances

        result = Graph.analysis_dependencies_between_nodes(builder.selected_task_instances, ["luiti_summary", "luiti_middle"])
        self.assertEqual(sorted(result.keys()), ["json", "python"])

        d1 = result["json"]["upons"]["direct"]
        c_instance = filter(lambda i1: "CounterVisitorByBrowserDay(" in i1, d1.keys())[0]
        c_deps = d1[c_instance]
        self.assertEqual(len(c_deps), 2)
        self.assertTrue("CounterVisitorDay" in repr(c_deps))
        self.assertTrue("BetaReportDay" in repr(c_deps))

        d2 = result["json"]["requires"]["total"]
        a_instance = filter(lambda i1: "BetaReportDay(" in i1, d1.keys())[0]
        a_deps = d2[a_instance]
        self.assertEqual(len(a_deps), 3)
        self.assertTrue("CounterVisitorByBrowserDay(" in repr(a_deps))
        self.assertTrue("CounterVisitorDay(" in repr(a_deps))
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

        builder = QueryBuilder(PTM, {"luiti_package": ['luiti_clean', 'luiti_dump', 'luiti_middle', 'luiti_summary']})

        # test default query value
        self.assertEqual(builder.date_begin, "2014-09-01")
        self.assertEqual(builder.date_end, builder.yesterday_str)
        self.assertEqual(builder.accepted_params, {'language': {'default': 'English', 'values': ['Chinese', 'English']}})
        self.assertTrue(len(builder.accepted_query_params["date_value"]) > 200)  # many days
        self.assertEqual(len(builder.accepted_query_params), 3)
        self.assertEqual(sorted(builder.default_query.keys()), ['date_value', 'language'])
        self.assertEqual(builder.default_query["language"], "English")

        self.assertEqual(sorted(builder.selected_query.keys()), ["date_value", "language", "luiti_package"])

        self.assertEqual(list(builder.selected_task_cls_names), [])

        self.assertEqual(len(builder.total_task_instances), (7 + 8), "with project_A+B")
        self.assertEqual(len(builder.selected_task_instances), 7)

        rest_tasks_info = repr(sorted(list(set(builder.total_task_instances) - set(builder.selected_task_instances))))
        self.assertTrue("ADay(" in rest_tasks_info, "ADay is project_A, and project_A in not selected")

        self.assertTrue("requires" in builder.graph_infos_data["json"])
        # ... to be continued.

        self.assertEqual(QueryBuilder(PTM, {}).selected_query["luiti_package"], ["luiti_summary"])  # default

    def test_CodeRender(self):
        from luiti.daemon.web.code_render import CodeRender

        cr = CodeRender()
        self.assertTrue(callable(cr.highlight))
        self.assertEqual(cr.formatter.name, "HTML")

        self.assertTrue(isinstance(cr[__file__], unicode))
        self.assertTrue(len(cr[__file__]) > 1000, "large html code")

    def test_Template(self):
        from luiti import TaskDay
        from luiti.daemon.utils.template import Template

        class SampleDay(TaskDay):
            """ doc """
            root_dir = "/foobar"

            def requires(self):
                if self == t2:  # hack
                    return []
                return t2

        t1 = SampleDay(date_value=date_begin)
        t2 = SampleDay(date_value="2014-01-01")

        node_info_sample = {
            'task_doc': 'doc',
            'group': '__main__',
            'package_name': '__main__',
            'task_file': '__main__.py',
            'data_file': '/foobar/2014-09-01/sample_day.json',
            'detail': 'SampleDay(date_value=2014-09-01T00:00:00+08:00)',
            'size': 20,
            'id': 'SampleDay(date_value=2014-09-01T00:00:00+08:00)',
            'label': 'SampleDay'}

        def remove_tzinfo(d1):
            """ Travis's tzinfo is different """
            for k2 in ["detail", "id", "to", "from",
                       "package_name", "group", "task_file"]:  # different in tox mode, "__main__" vs "test_daemon"
                if k2 in d1:
                    del d1[k2]
            return d1

        self.assertEqual(Template.task_doc(t1), "doc")
        self.assertEqual(remove_tzinfo(Template.a_node(t1)), remove_tzinfo(node_info_sample))

        edge_sample_1 = {
            'strength': 1.0,
            'source_name': 'SampleDay',
            'target_name': 'SampleDay',
            'arrows': 'self_to_self',
            'to': 'SampleDay(date_value=2014-09-01T00:00:00+08:00)',
            'from': 'SampleDay(date_value=2014-09-01T00:00:00+08:00)',
            'id': 'SampleDay(date_value=2014-09-01T00:00:00+08:00) SampleDay(date_value=2014-09-01T00:00:00+08:00)'}
        self.assertEqual(remove_tzinfo(Template.an_edge(t1, t1)), remove_tzinfo(edge_sample_1))

        edge_sample_2 = {
            'strength': 1.0,
            'source_name': 'SampleDay',
            'target_name': 'SampleDay',
            'arrows': 'to',
            'to': 'SampleDay(date_value=2014-09-01T00:00:00+08:00)',
            'from': 'SampleDay(date_value=2014-01-01T00:00:00+08:00)',
            'id': 'SampleDay(date_value=2014-01-01T00:00:00+08:00) SampleDay(date_value=2014-09-01T00:00:00+08:00)'}

        edges = Template.edges_from_nodes([t1, t2])
        self.assertTrue(isinstance(edges, list))
        self.assertEqual(remove_tzinfo(edges[0]), remove_tzinfo(edge_sample_2))


if __name__ == '__main__':
    unittest.main()
