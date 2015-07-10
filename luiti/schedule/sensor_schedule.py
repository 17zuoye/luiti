# -*-coding:utf-8-*-

import luigi
import time
from ..luigi_extensions import ArrowParameter, RootTask


class SensorSchedule(object):
    """
    If all tasks's runtime and output wrapped in Luigi's concept, it's perfect. But sometimes Luigi has to wait other blackbox systems to run and output, and don't know when it'll finish.

    It seems that we could time.sleep(a few seconds) to check that the output appears. But here comes the below errors:

        Luigi framework error:
    Traceback (most recent call last):
      File "/home/luigi/.pyenv/versions/2.7.9/lib/python2.7/site-packages/luigi-1.1.2-py2.7.egg/luigi/worker.py", line 465, in add
        current = queue.get()
      File "<string>", line 2, in get
      File "/home/luigi/.pyenv/versions/2.7.9/lib/python2.7/multiprocessing/managers.py", line 755, in _callmethod
        self._connect()
      File "/home/luigi/.pyenv/versions/2.7.9/lib/python2.7/multiprocessing/managers.py", line 742, in _connect
        conn = self._Client(self._token.address, authkey=self._authkey)
      File "/home/luigi/.pyenv/versions/2.7.9/lib/python2.7/multiprocessing/connection.py", line 175, in Client
        answer_challenge(c, authkey)
      File "/home/luigi/.pyenv/versions/2.7.9/lib/python2.7/multiprocessing/connection.py", line 428, in answer_challenge
        message = connection.recv_bytes(256)         # reject large message
IOError: [Errno 104] Connection reset by peer

    It doesn't work :-(

    Soon I found that [Airbnb](http://airbnb.com) released their open source project http://github.com/airbnb/airflow. Airflow has a Sensor concept, used to "Waits for events to happen. This could be a file appearing in HDFS, the existence of a Hive partition, or waiting for an arbitrary MySQL query to return a row. ". It's really great, but unfortunately it doesn't compact with Luigi. Here's the document http://pythonhosted.org/airflow/concepts.html#operators


    ----------------------------

    At last, I found a new solution, inspired by `Luiti webui` and `Airflow`. We can calculate the DAG on the client side via `requires` function, and check output manually with adding `is_external` attribute to luigi.Task.
    """
    default_wait_seconds = 5
    max_wait_seconds = 3600 * 2

    @classmethod
    def run(cls, curr_task, date_value):
        # 1. check date type
        assert issubclass(curr_task, luigi.Task), curr_task
        date_value = ArrowParameter.get(date_value)

        # 2. get run task instances list
        ordered_task_instances_list = read_all_required_tasks(curr_task)

        # 3. execute tasks
        for t1 in ordered_task_instances_list:
            should_wait = is_external(t1.__class__)
            if should_wait:
                sleep_seconds = 0
                while True:
                    # 1. output arrives
                    if t1.output().exists():
                        break
                    else:
                        sleep_seconds += cls.default_wait_seconds
                        # 2. time arrives
                        if sleep_seconds > cls.max_wait_seconds:
                            break
                        time.sleep(cls.default_wait_seconds)

            # Can't turn into a task instance
            # luigi.run(main_task_cls=t1 is not cls)  # support other task parameter, or yesterday (diff from current date_value)

            # Actually we do it manually.
            from luigi.interface import ArgParseInterface
            interface = ArgParseInterface()
            interface.run([t1])  # , worker_scheduler_factory, override_defaults=override_defaults) can be ignored.


def read_all_required_tasks(curr_task):
    """
    Start from a root task, and read task dependencies recursively.
    """
    # NOTE consider Luiti modifies Luigi, task instances maybe not unique. TODO Fix it
    def func(dep_dict, _curr_task):
        """ Generate a a dependencies graph. """
        required_task_instances = _curr_task.requires()
        if not isinstance(required_task_instances, list):
            required_task_instances = [required_task_instances]
        dep_dict[curr_task] = map(lambda t1: t1 and (not isinstance(t1, RootTask)), required_task_instances)

        for t1 in dep_dict[curr_task]:
            func(dep_dict, t1)

        return dep_dict

    # 1. generate a dep dict
    task_instances_dep_dict = func(dict(), curr_task)

    # 2. sort the DAG.
    from toposort import toposort_flatten
    ordered_task_instances_list = toposort_flatten(task_instances_dep_dict)

    return ordered_task_instances_list


def is_external(task_instance):
    """ Is the data came from uncontrolled outside blackbox. """
    # 0. default case
    if isinstance(task_instance, luigi.ExternalTask):
        return True
    # 1. assert is_external attribute exists.
    if getattr(task_instance, "is_external", False) is not True:
        return False
    # 2. assert run is NotImplementedError
    if getattr(task_instance, "run", NotImplementedError) != NotImplementedError:
        return False
    return True
