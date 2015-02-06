#-*-coding:utf-8-*-

import json
from etl_utils import JsonUtils


class MRUtils:

    map_key_split   = u"@@"              # map 多维度键 分隔符
    map_key_escape  = u"\""              # map 字符串默认 JSON dump
    mr_separator    = u"\t"              # map reduce 分隔符

    @staticmethod
    def mr_key(item1, postfix=''):
        """ example is "104017@@37771707" """
# TODO 业务代码应该剥离
        str1 = u"%s%s%s" % (item1.get('class_id', 0), MRUtils.map_key_split, item1.get('uid', 0),)
        if postfix: str1 += (MRUtils.map_key_split + unicode(postfix))
        return str1

    @staticmethod
    def json_parse(line1):
        line1 = line1.strip()
        if isinstance(line1, str):
            line1 = line1.decode("UTF-8")
        return json.loads(line1)

    @staticmethod
    def is_mr_line(line1):
        # 1. 目前标准的 MapReduce 输出
        head = line1[0:30]
        is_true_1 = (MRUtils.map_key_split in head) or (MRUtils.mr_separator in head)
        # 2. value 必须是 } 或 ]
        is_true_2 = (line1.endswith("}") or line1.endswith("]"))
        # 3. 外部Python程序写的一行一行JSON, 没有 map key 。
        is_true_3 = (not line1.startswith("{")) and (not line1.startswith("["))
        return is_true_1 and is_true_2 and is_true_3

    @staticmethod
    def unicode_value(item1, key1):
        val1 = item1.get(key1, u"")
        if isinstance(val1, str): val1 = val1.decode("UTF-8")
        return val1

    @staticmethod
    def split_mr_kv(line1):
        """ 返回一个 解析好的 [k,v] 数组。 """
        if isinstance(line1, str): line1 = line1.decode("UTF-8")
        k_str, v_str = line1.split(MRUtils.mr_separator, 1)

        return [
                MRUtils.select_prefix_keys(k_str),
                json.loads(v_str),
               ]


    ##############################
    #####   key related     ######
    ##############################
    @staticmethod
    def merge_keys_in_dict(vals_1, keys_1):
        """ 合并多个键的整数值。 """
        merge = {key_1:0 for key_1 in keys_1}
        for v_2 in vals_1:
            for key_1 in keys_1:
                merge[key_1] += v_2[key_1]
        return merge

    @staticmethod
    def concat_prefix_keys(*keys):
        items_str = map(unicode, keys)
        return MRUtils.map_key_split.join(items_str)

    @staticmethod
    def split_prefix_keys(line_part_a):
        """ return list """
        fixed_str = MRUtils.select_prefix_keys(line_part_a)
        return fixed_str.split(MRUtils.map_key_split)

    @staticmethod
    def select_prefix_keys(line_part_a, idxes=None):
        """
        根据索引数组 转化出新的 map key
        e.g. select_prefix_keys("232@@8923802@@afenti", [0,1]) # => "232@8923802"
        """
        if isinstance(line_part_a, str): line_part_a = line_part_a.decode("UTF-8")
        # 兼容解析格式错误的jsonkey
        if line_part_a.startswith(MRUtils.map_key_escape) and (not line_part_a.endswith(MRUtils.map_key_escape)):
            line_part_a = line_part_a[1:]
        if line_part_a.startswith(MRUtils.map_key_escape): # is a json
            line_part_a = json.loads(line_part_a)

        if idxes is None:
            return line_part_a
        else:
            parts = line_part_a.split(MRUtils.map_key_split)
            new_parts = []
            for idx_1 in idxes:
                new_parts.append(parts[idx_1])
            return MRUtils.map_key_split.join(new_parts)

    @staticmethod
    def str_dump(result_dict):
        return JsonUtils.unicode_dump(result_dict).encode("UTF-8")

    @staticmethod
    def filter_dict(d1, keys):
        if not isinstance(keys, list): keys = [keys]
        return {k1:d1[k1] for k1 in keys}
