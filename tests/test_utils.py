# -*- coding: utf-8 -*-

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)
os.environ['LUIGI_CONFIG_PATH'] = root_dir + '/tests/client.cfg'

import unittest


class TestLuitiUtils(unittest.TestCase):

    def test_mr(self):
        from luiti import MRUtils

        item1 = {"class_id":3, "uid":7}
        self.assertEqual(MRUtils.mr_key(item1), "3@@7")
        self.assertEqual(MRUtils.mr_key(item1, "hid009"), "3@@7@@hid009")

        self.assertEqual(MRUtils.json_parse("{\"你好\":\"世界\"}"), {u"你好":u"世界"})

        self.assertFalse(MRUtils.is_mr_line("[1,2]"))
        self.assertTrue(MRUtils.is_mr_line("hello\t{framework:luigi}"))
        self.assertTrue(MRUtils.is_mr_line("1@@" + "2"*40 + "\t[world]"))

        self.assertEqual(MRUtils.unicode_value({u"hello":u"世界"}, "hello"), u"世界")

        self.assertEqual(MRUtils.split_mr_kv("hello\t[1,2,3,4]"), ["hello", [1,2,3,4]])

        self.assertEqual(MRUtils.class_uid_keys("1@@2@@other\t[]"), "1@@2")

        self.assertEqual(MRUtils.merge_keys_in_dict([{"a":1}, {"a":2}], ["a"]), {"a":3})

        self.assertEqual(MRUtils.split_prefix_keys("1@@2@@other"), ["1", "2", "other"])

        prefix_str1 = "232@@8923802@@afenti"
        prefix_str2 = "\"" + prefix_str1
        self.assertEqual(MRUtils.select_prefix_keys(prefix_str1, [0,1]), "232@@8923802")
        self.assertEqual(MRUtils.select_prefix_keys(prefix_str2, [0,1]), "232@@8923802")


    def test_math(self):
        from luiti import MathUtils

        self.assertEqual(MathUtils.percent(5, 10), 0.5)
        self.assertEqual(MathUtils.percent(5,  0), 0)
        self.assertEqual(MathUtils.percent(5,  None), 0)
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

        self.assertEqual(len(DateUtils.fixed_weeks_in_range("2014-10-01-2014-10-15")), 1)
        self.assertEqual(len(DateUtils.fixed_weeks_in_range("2014-09-29-2014-10-15")), 2)

        self.assertEqual(DateUtils.date_value_by_type_in_last("2014-09-01", "week"), arrow.get("2014-08-25"))


    def test_ext(self):
        from etl_utils import cached_property
        from luiti.utils import ExtUtils
        import inspect

        class Foobar(ExtUtils.ExtendClass):

            def method_1(self): return "method_1"

            @property
            def property_1(self): return "property_1"

            @cached_property
            def cached_property_1(self): return "cached_property_1"

        fb1 = Foobar()
        self.assertEqual(fb1.method_1(),             "method_1")
        self.assertEqual(fb1.property_1,             "property_1")
        self.assertEqual(fb1.cached_property_1,      "cached_property_1")


        Foobar.extend(
            not_exist_str       = "not_exist_str",
            method_1            = lambda self: "method_2",
            property_1          = lambda self: "property_2",
            cached_property_1   = lambda self: "cached_property_2",
          )

        fb2 = Foobar()
        self.assertEqual(fb2.method_1(),             "method_2")
        self.assertEqual(fb2.property_1,             "property_2")
        self.assertEqual(fb2.cached_property_1,      "cached_property_2")

        self.assertTrue(isinstance(Foobar.not_exist_str, str))
        self.assertTrue(inspect.ismethod(Foobar.method_1))
        self.assertTrue(isinstance(Foobar.property_1, property))
        self.assertTrue(isinstance(Foobar.cached_property_1, cached_property))


if __name__ == '__main__': unittest.main()
