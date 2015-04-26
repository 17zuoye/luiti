# -*-coding:utf-8-*-

__all__ = ["mr_local"]

from collections import defaultdict
from etl_utils import process_notifier
from ..utils import TargetUtils


def mr_local(**opts):
    """
    Sometimes Hadoop streaming sucks, so we only use the solid HDFS, and turn
    MapReduce job into local mode.

    And `mr_local` is optimized by a fixed chunk write operation.
    """

    def mr_run(self):
        """ Overwrite BaseHadoopJobTask#run function. """
# TODO maybe model cache
        map_kv_dict = defaultdict(list)

        inputs = self.input()
        if not isinstance(inputs, list):
            inputs = [inputs]
        for input_hdfs_1 in inputs:
            for line2 in TargetUtils.line_read(input_hdfs_1):
                for map_key_3, map_val_3 in self.mapper(line2):
                    map_kv_dict[map_key_3].append(map_val_3)

        with self.output().open("w") as output1:
            fixed_chunk = list()
            for reduce_key_2 in process_notifier(map_kv_dict.keys()):
                reduce_vals_2 = map_kv_dict[reduce_key_2]
                for _, reduce_val_2 in self.reducer(
                        reduce_key_2, reduce_vals_2):
                    fixed_chunk.append(reduce_val_2)

                    if len(fixed_chunk) % self.chunk_size == 0:
                        output1.write("\n".join(fixed_chunk) + "\n")
                        fixed_chunk = list()
                del map_kv_dict[reduce_key_2]
            output1.write("\n".join(fixed_chunk) + "\n")

    def wrap(cls):
        cls.run = mr_run
        cls.run_mode = "mr_local"

        opts["chunk_size"] = opts.get("chunk_size", 100)
        for k1, v1 in opts.iteritems():
            setattr(cls, k1, v1)

        return cls
    return wrap
