# -*- coding: utf-8 -*-

import os
import sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest
import mock


class TestLuitiUtils(unittest.TestCase):

    def test_mr(self):
        from luiti import MRUtils

        item1 = {"class_id": 3, "uid": 7}
        self.assertEqual(MRUtils.mr_key(item1), "3@@7")
        self.assertEqual(MRUtils.mr_key(item1, "hid009"), "3@@7@@hid009")

        self.assertEqual(MRUtils.json_parse("{\"你好\":\"世界\"}"), {u"你好": u"世界"})

        self.assertFalse(MRUtils.is_mr_line("[1,2]"))
        self.assertTrue(MRUtils.is_mr_line("hello\t{framework:luigi}"))
        self.assertTrue(MRUtils.is_mr_line("1@@" + "2" * 40 + "\t[world]"))

        self.assertEqual(
            MRUtils.unicode_value({u"hello": u"世界"}, "hello"), u"世界")

        self.assertEqual(
            MRUtils.split_mr_kv("hello\t[1,2,3,4]"), ["hello", [1, 2, 3, 4]])

        self.assertEqual(
            MRUtils.merge_keys_in_dict([{"a": 1}, {"a": 2}], ["a"]), {"a": 3})

        self.assertEqual(
            MRUtils.split_prefix_keys("1@@2@@other"), ["1", "2", "other"])

        prefix_str1 = "232@@8923802@@afenti"
        prefix_str2 = "\"" + prefix_str1
        self.assertEqual(
            MRUtils.select_prefix_keys(prefix_str1, [0, 1]), "232@@8923802")
        self.assertEqual(
            MRUtils.select_prefix_keys(prefix_str2, [0, 1]), "232@@8923802")

        self.assertEqual(
            MRUtils.str_dump({"hello": u"世界"}), """{"hello": "世界"}""")

        self.assertEqual(
            MRUtils.filter_dict(
                {"hello": "world", "foobar": "barfoo"}, "hello"),
            {"hello": "world"})

    def test_math(self):
        from luiti import MathUtils

        self.assertEqual(MathUtils.percent(5, 10), 0.5)
        self.assertEqual(MathUtils.percent(5, 0), 0)
        self.assertEqual(MathUtils.percent(5, None), 0)
        self.assertEqual(MathUtils.percent(None, 1), 0)

    def test_date(self):
        from luiti import DateUtils
        import arrow

        arrow1 = DateUtils.arrow.get("2014-10-01 12:01:01")
        arrow2 = DateUtils.arrow.get("2014-10-15 12:01:01")

        self.assertEqual(DateUtils.arrow_str(arrow1), "2014-10-01")

        self.assertEqual(len(DateUtils.days_in_week(arrow1)), 7)
        self.assertTrue(arrow1.floor('day') in DateUtils.days_in_week(arrow1))

        self.assertEqual(len(DateUtils.weeks_in_range(arrow1, arrow2)), 3)

        self.assertEqual(
            len(DateUtils.fixed_weeks_in_range("2014-10-01-2014-10-15")), 1)
        self.assertEqual(
            len(DateUtils.fixed_weeks_in_range("2014-09-29-2014-10-15")), 2)

        self.assertEqual(
            DateUtils.date_value_by_type_in_last("2014-09-01", "week"),
            arrow.get("2014-08-25"))

    def test_ext(self):
        from etl_utils import cached_property
        from luiti.utils import ExtUtils
        import inspect

        class Foobar(ExtUtils.ExtendClass):

            def method_1(self):
                return "method_1"

            @property
            def property_1(self):
                return "property_1"

            @cached_property
            def cached_property_1(self):
                return "cached_property_1"

        fb1 = Foobar()
        self.assertEqual(fb1.method_1(), "method_1")
        self.assertEqual(fb1.property_1, "property_1")
        self.assertEqual(fb1.cached_property_1, "cached_property_1")

        Foobar.extend({
            'not_exist_str': "not_exist_str",
            'method_1': lambda self: "method_2",
            'property_1': lambda self: "property_2",
            'cached_property_1': lambda self: "cached_property_2",
        })

        fb2 = Foobar()
        self.assertEqual(fb2.method_1(), "method_2")
        self.assertEqual(fb2.property_1, "property_2")
        self.assertEqual(fb2.cached_property_1, "cached_property_2")

        self.assertTrue(isinstance(Foobar.not_exist_str, str))
        self.assertTrue(inspect.ismethod(Foobar.method_1))
        self.assertTrue(isinstance(Foobar.property_1, property))
        self.assertTrue(isinstance(Foobar.cached_property_1, cached_property))

    def test_IOUtils(self):
        from luiti.utils import IOUtils

        self.assertEqual(IOUtils.json_dump({}), "{}")
        self.assertEqual(IOUtils.json_dump([{}]), "[{}]")

    def test_TargetUtils(self):
        from luiti.utils import TargetUtils
        from luigi.mock import MockTarget

        def mock_test_file(filename, data):
            class HdfsFile(MockTarget):
                pass

            f = HdfsFile(filename)
            with f.open("w") as w:
                w.write(data)

            return f
        g1 = TargetUtils.line_read(mock_test_file("g1", """\nline one\nline two\n   \n"""))
        self.assertTrue(list(g1), [u"line one", u"line two"])

        g2 = TargetUtils.json_read(mock_test_file("g1", """\n{"a": 1}\n[1, "b"]  \n \n"""))
        self.assertTrue(list(g2), [{"a": 1}, [1, "b"]])

    @mock.patch("luiti.utils.HDFSUtils.hdfs_cli")
    @mock.patch("luiti.utils.CommandUtils.execute")
    @mock.patch("luiti.utils.HDFSUtils.copyToLocal")
    @mock.patch("os.path.isdir")
    @mock.patch("luiti.utils.HDFSUtils.exists")
    def test_CompressUtils(self, hdfs_exists, os_path_isdir, copyToLocal, execute, hdfs_cli):
        """ a rough test ... """
        hdfs_exists.return_value = True
        os_path_isdir.return_value = False
        copyToLocal.return_value = True
        execute.return_value = True
        hdfs_cli.return_value = "hdfs"

        from luiti.utils import CompressUtils
        self.assertTrue(CompressUtils.unzip_with_upload(
            "orig", "dist",
            tmp_dir="/tmp",
            tmp_name="foobar"))

    @mock.patch("os.system")
    def test_CommandUtils(self, os_system):
        os_system.return_value = 0

        from luiti.utils import CommandUtils
        self.assertEqual(CommandUtils.execute("ls"), 0)
        self.assertEqual(CommandUtils.execute("ls", dry=True), 0)


if __name__ == '__main__':
    unittest.main()
