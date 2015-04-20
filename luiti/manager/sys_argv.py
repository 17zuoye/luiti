#-*-coding:utf-8-*-

import sys

class SysArgv(object):

    @staticmethod
    def convert_to_luigi_accepted_argv(subparsers):
        old = sys.argv[:]
        luigi_keep_opts = ["--date-value"]

        def fetch_keys(parser1):
            return parser1.__dict__['_option_string_actions'].keys()

        luiti_only_opts = subparsers.choices.keys() + list(set([k3 for p2 in subparsers._name_parser_map.values() for k3 in fetch_keys(p2)]))
        luiti_only_opts = [i1 for i1 in luiti_only_opts if i1 not in  luigi_keep_opts]

        delete_argv_idxes = set([])
        for idx1, arg1 in enumerate(sys.argv):
            if idx1 in delete_argv_idxes: continue
            # 1. 排除 tasks, files, run 等
            if (not arg1.startswith("--")) and (arg1 in luiti_only_opts):
                delete_argv_idxes.add(idx1)
                continue
            # 2. 处理 --task-name 等参数
            if "=" in arg1:
                arg2, val2 = arg1.split("=", 1)
                if arg2 in luiti_only_opts:
                    delete_argv_idxes.add(idx1)
            else:
                if (arg1 in luiti_only_opts) and (arg1 not in luigi_keep_opts):
                    delete_argv_idxes.add(idx1)
                    delete_argv_idxes.add(idx1 + 1)
        sys.argv = [arg1 for idx1, arg1 in enumerate(sys.argv) if idx1 not in delete_argv_idxes]
