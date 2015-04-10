# -*- coding: utf-8 -*-

__all__ = ['MrTestCase']


from collections import defaultdict
import json

from .utils import MRUtils
from .manager import Loader


def MrTestCase(cls, verbose=False):
    """
    功能: 集成测试数据到 类中 ，这样就方便引用了。
    """

    assert "mr_task_names" in dir(cls), "%s must assgin some task names!" % cls

    cls.maxDiff = None # compact large json diff

    def map_lines(text):
        assert isinstance(text, unicode)
        result = list()
        for l1 in text.split("\n"):
            l1 = l1.strip()
            if not l1: continue
            result.append(l1)
        return result

    def generate_closure_function(mr_task_name1):
        task_cls      = Loader.load_a_task_by_name(mr_task_name1) # keep it!
        if verbose: print "[task_cls]", task_cls

        def test_mr(self):
            task_instance_1 = task_cls("2014-09-01")
            if verbose: print "[task_instance]", task_instance_1

            task_instance_1.lines  = map_lines(task_instance_1.mrtest_input())
            result_expect          = sorted([json.loads(i2) for i2 in map_lines(task_instance_1.mrtest_output())])

            self.assertEqual(result_expect, run_map_reduce(task_instance_1))
        return test_mr


    for mr_task_name1 in cls.mr_task_names:
        test_method_name = "test_" + mr_task_name1
        if verbose: print
        if verbose: print "[test_method_name]", test_method_name
        setattr(cls, test_method_name, generate_closure_function(mr_task_name1))
        if verbose: print
        if verbose: print

    return cls





def run_map_reduce(task_instance_1):
    # 1. bind attrs
    for k1, v1 in task_instance_1.mrtest_attrs().iteritems():
        setattr(task_instance_1, k1, v1)

    # 2. map it!
    mapper_key_to_vals = defaultdict(list)
    for line1 in task_instance_1.lines:
        for key_1, val_1 in task_instance_1.mapper(line1.strip()):
            mapper_key_to_vals[key_1].append(val_1)

    # 3. reduce it!
    result_list = list()
    for key_1, vals_1 in mapper_key_to_vals.iteritems():
        vals_generator = iter(vals_1)
        for _, val_2 in task_instance_1.reducer(key_1, vals_generator):
            result_list.append(json.loads(val_2))
    return sorted(result_list)
