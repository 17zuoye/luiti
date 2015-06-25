# -*-coding:utf-8-*-

import luigi


class Template(object):
    """
    Generate some output from entities.
    """

    @staticmethod
    def task_doc(ti):
        """ Get task doc from class. """
        doc = (ti.task_class.__doc__ or "").strip()
        if isinstance(doc, str):
            doc = doc.decode("UTF-8")
        return doc

    @staticmethod
    def a_node(ti):
        result = {"id": ti.task_id,
                  "label": ti.task_class.__name__,
                  "group": ti.package_name,

                  "detail": str(ti),
                  "data_file": ti.data_file,
                  "task_doc": Template.task_doc(ti),
                  "task_file": ti.task_class.__module__.replace(".", "/") + ".py",
                  "package_name": ti.package_name,
                  }
        result["size"] = 20
        return result

    @staticmethod
    def edges_from_task_instances(task_instances):
        assert isinstance(task_instances, list)
        if len(task_instances):
            assert isinstance(task_instances[0], luigi.Task)

        edges = list()
        for ti in task_instances:
            t2_in_requires = ti.requires()
            if not isinstance(t2_in_requires, list):
                t2_in_requires = [t2_in_requires]
            for t2 in t2_in_requires:
                if t2 is None:  # dep on none tasks
                    continue
                if t2 not in task_instances:
                    continue
                edges.append(Template.an_edge(t2, ti))
        return edges

    @staticmethod
    def an_edge(from_task, to_task):
        arrows = "to"  # default
        if from_task == to_task:
            arrows = "self_to_self"

        result = {"id": from_task.task_id + " " + to_task.task_id,  # id is uniq.
                  "from": from_task.task_id,
                  "source_name": from_task.task_class.__name__,
                  "to": to_task.task_id,
                  "target_name": to_task.task_class.__name__,
                  "strength": 1.0,
                  "arrows": arrows}

        return result
