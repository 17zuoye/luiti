# -*-coding:utf-8-*-

from etl_utils import cached_property
import luigi
import time
from ..luigi_extensions import ArrowParameter, RootTask


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

At last, I found a new solution, inspired by `Luiti webui` and `Airflow`. We can calculate the DAG on the client side via `requires` function, and check `output().exists()` manually with adding `is_external` attribute to luigi.Task.
"""


class SensorSchedule(object):
    """
    Fix luigi don't support uncontrolled external tasks, so schedule task submit by client luiti itself.
    """

    default_wait_seconds = 5
    max_wait_seconds = 3600 * 2
    current_sleep_seconds = 0

    @classmethod
    def run(cls, curr_task, date_value):
        ss = SensorSchedule(curr_task, date_value)

        while True:
            # 1. all output arrives
            if ss.is_external_all_complete():
                break
            else:
                cls.current_sleep_seconds += cls.default_wait_seconds
                # 2. time arrives
                if cls.current_sleep_seconds > cls.max_wait_seconds:
                    exit()
                time.sleep(cls.default_wait_seconds)

        # A. Original Way
        # Can't turn into a task instance
        luigi.run(main_task_cls=curr_task)  # support other task parameter, or yesterday (diff from current date_value)

        # B. New Way
        # Actually we do it manually.
        #   Whatever how long task instances, interface.run will only run once, and exits current process.
        #   So we need to pass all current task instances to `interface.run` .
        # from luigi.interface import ArgParseInterface
        # interface = ArgParseInterface()
        # interface.run(ss.ordered_task_instances_list)  # , worker_scheduler_factory, override_defaults=override_defaults) can be ignored.
        """
        Raise below errors. It seems like submit related tasks to luigid work don't works.

        Traceback (most recent call last):
          File "/home/luigi/deploy/mvj3.20150511/ENV/bin/luiti", line 4, in <module>
            __import__('pkg_resources').run_script('luiti==0.2.0', 'luiti')
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/pkg_resources/__init__.py", line 723, in run_script
            self.require(requires)[0].run_script(script_name, ns)
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/pkg_resources/__init__.py", line 1636, in run_script
            exec(code, namespace, namespace)
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/luiti-0.2.0-py2.7.egg/EGG-INFO/scripts/luiti", line 162, in <module>
            SensorSchedule.run(curr_task, args_main.date_value)
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/luiti-0.2.0-py2.7.egg/luiti/schedule/sensor_schedule.py", line 70, in run
            interface.run(ss.ordered_task_instances_list)  # , worker_scheduler_factory, override_defaults=override_defaults) can be ignored.
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/luigi-1.1.2-py2.7.egg/luigi/interface.py", line 180, in run
            success &= w.add(t, env_params.parallel_scheduling)
          File "/home/luigi/deploy/mvj3.20150511/ENV/lib/python2.7/site-packages/luigi-1.1.2-py2.7.egg/luigi/worker.py", line 453, in add
            pool = multiprocessing.Pool()
          File "/home/luigi/.pyenv/versions/2.7.5/lib/python2.7/multiprocessing/__init__.py", line 232, in Pool
            return Pool(processes, initializer, initargs, maxtasksperchild)
          File "/home/luigi/.pyenv/versions/2.7.5/lib/python2.7/multiprocessing/pool.py", line 159, in __init__
            self._repopulate_pool()
          File "/home/luigi/.pyenv/versions/2.7.5/lib/python2.7/multiprocessing/pool.py", line 222, in _repopulate_pool
            w.start()
          File "/home/luigi/.pyenv/versions/2.7.5/lib/python2.7/multiprocessing/process.py", line 130, in start
            self._popen = Popen(self)
          File "/home/luigi/.pyenv/versions/2.7.5/lib/python2.7/multiprocessing/forking.py", line 121, in __init__
            self.pid = os.fork()
        OSError: [Errno 11] Resource temporarily unavailable
        """

    def __init__(self, curr_task, date_value):
        # 1. check data type
        assert issubclass(curr_task, luigi.Task), curr_task
        self.curr_task = curr_task
        self.date_value = ArrowParameter.get(date_value)

    @cached_property
    def curr_task_intance(self):
        return self.curr_task(date_value=self.date_value)

    @cached_property
    def ordered_task_instances_list(self):
        """
        Start from a root task, and read task dependencies recursively.
        """
        # NOTE consider Luiti modifies Luigi, same task instances maybe not unique. TODO Fix it
        def func(dep_dict, _curr_task):
            """ Generate a a dependencies graph. """
            required_task_instances = _curr_task.requires()
            if not isinstance(required_task_instances, list):
                required_task_instances = [required_task_instances]
            dep_dict[_curr_task] = set(filter(lambda t1: bool(t1) and (not isinstance(t1, RootTask)), required_task_instances))

            for t1 in dep_dict[_curr_task]:
                func(dep_dict, t1)

            return dep_dict

        # 1. generate a dep dict
        task_instances_dep_dict = func(dict(), self.curr_task_intance)

        # 2. sort the DAG.
        from toposort import toposort_flatten
        ordered_task_instances_list = toposort_flatten(task_instances_dep_dict)

        return ordered_task_instances_list

    @cached_property
    def external_task_instances_list(self):
        return filter(lambda i1: SensorSchedule.is_external(i1.__class__), self.ordered_task_instances_list)

    def is_external_all_complete(self):
        return len(filter(lambda t1: not t1.output().exists(), self.external_task_instances_list)) == 0

    @staticmethod
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
