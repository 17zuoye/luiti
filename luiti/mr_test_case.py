# -*- coding: utf-8 -*-

__all__ = ['MrTestCase']


from collections import defaultdict
import json

from .utils import MRUtils
from .manager import Loader

# NOTE 集成测试数据到 类中 ，这样就方便引用了。

def MrTestCase(cls, verbose=False):

    assert "mr_task_names" in dir(cls), "%s must assgin some task names!" % cls

    cls.maxDiff = None # compact large json diff

    def generate_closure_function(mr_task_name1):
        task_cls      = Loader.load_a_task_by_name(mr_task_name1) # keep it!
        if verbose: print "[task_cls]", task_cls

        def test_mr(self):
            task_instance_1 = task_cls("2014-09-01")
            if verbose: print "[task_instance]", task_instance_1

            task_instance_1.lines         = [line1 for line1 in task_instance_1.mrtest_input().strip().split("\n") if line1.strip()]
            result_expect = dict([tuple(MRUtils.split_mr_kv(line1.strip())) \
                                        for line1 in task_instance_1.mrtest_output().strip().split("\n") if line1.strip()])


            self.assertEqual(run_map_reduce(task_instance_1), result_expect)
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
    #import pdb; pdb.set_trace()
    # 1. bind attrs
    for k1, v1 in task_instance_1.mrtest_attrs().iteritems():
        setattr(task_instance_1, k1, v1)

    # 2. map it!
    mapper_key_to_vals = defaultdict(list)
    for line1 in task_instance_1.lines:
        for key_1, val_1 in task_instance_1.mapper(line1.strip()):
            mapper_key_to_vals[key_1].append(val_1)

    # 3. reduce it!
    result_key_to_vals = dict()
    for key_1, vals_1 in mapper_key_to_vals.iteritems():
        for key_2, val_2 in task_instance_1.reducer(key_1, vals_1):
            result_key_to_vals[MRUtils.select_prefix_keys(unicode(key_2))] = json.loads(val_2)
    return result_key_to_vals
