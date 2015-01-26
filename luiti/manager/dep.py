#-*-coding:utf-8-*-

from collections import defaultdict

class Dep(object):

    @staticmethod
    def find_dep_on_tasks(curr_task_1, task_classes_1):
        """ return all task classes. """
        # 找到的DAG库没有对应功能或不好用，比如 dagger。只能自己实现了。
        task_name_to_instance = {task_instance_1.__name__ : task_instance_1 for task_instance_1 in (task_classes_1 + [curr_task_1])}

        linked_dict = defaultdict(list) # dep_task => next_task
        for task_2 in task_classes_1:
            for ref_task_name_3 in task_2._ref_tasks:
                linked_dict[ref_task_name_3].append(task_2.__name__)

        result = set(linked_dict[curr_task_1.__name__] + [curr_task_1.__name__]) # filter linked to self
        _is_add = True
        while True:
            for next_task_name_1 in list(result): # make a copy
                next_task_names_2 = linked_dict[next_task_name_1]
                # 1. 没数据
                if len(next_task_names_2) == 0:
                    _is_add = False
                # 2. 有数据
                else:
                    for next_task_name_2 in next_task_names_2:
                        if next_task_name_2 in result:
                            _is_add = False
                        else:
                            result.add(next_task_name_2)

            if not _is_add: break

        result = [task_name_to_instance[name_1] for name_1 in result]
        result.remove(curr_task_1)
        return result
