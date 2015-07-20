Luiti
========================
[![Build Status](https://img.shields.io/travis/17zuoye/luiti/master.svg?style=flat)](https://travis-ci.org/17zuoye/luiti)
[![Coverage Status](https://coveralls.io/repos/17zuoye/luiti/badge.svg)](https://coveralls.io/r/17zuoye/luiti)
[![Health](https://landscape.io/github/17zuoye/luiti/master/landscape.svg?style=flat)](https://landscape.io/github/17zuoye/luiti/master)
[![Download](https://img.shields.io/pypi/dm/luiti.svg?style=flat)](https://pypi.python.org/pypi/luiti)
[![License](https://img.shields.io/pypi/l/luiti.svg?style=flat)](https://pypi.python.org/pypi/luiti)

As [Luigi](https://github.com/spotify/luigi)'s homepage said, it's "a
Python module that helps you build complex pipelines of batch jobs. It
handles dependency resolution, workflow management, visualization etc.
It also comes with Hadoop support built in."

Luiti is built on top of Luigi, separates all your tasks into multiple
packages, and forces one task per one Python file. Luiti task classes
can be managed by the `luiti` command, supported operations are ls, new,
generate, info, clean, run, and webui.

Luiti is born to build a layered database warehouse, corresponding to
the different packages we just mentioned. A data warehouse is consisted
of synced data sources, fact tables, dimension tables, regular or
temporary business reports.

The essence of batching processing system is to separating a large task
into small tasks, and the essence of business report is that a daily
report or a weekly report is requried, so here comes TaskDay, TaskWeek,
and more. Task classes also have a Hadoop version, such as TaskDayHadoop,
TaskWeekHadoop, and so on.

You can pass any parameters into Luigi's tasks, but Luiti recommend you
to pass only `date_value` parameter. So you can run Luiti tasks
periodically, e.g. hourly, daily, weekly, etc. luiti = luigi + time.


Document guide
------------------------
1. [Intro](#luiti)
3. [Luiti command tool](#luiti-command-tool)
2. [Luiti WebUI screenshots](#luiti-webui-screenshots)
4. [Core concepts based on time management](#core-concepts-based-on-time-management)
5. [Task specification and built-in properties and recommendation](#task-specification-and-built-in-properties-and-recommendation)
6. [Manage multiple projects in luiti](#manage-multiple-projects-in-luiti)
7. [A simple guide to Luigi](#a-simple-guide-to-luigi)
8. [A simple example in luiti](#a-simple-example-in-luiti)
9. [Installment & Development](#installment--develop-requirements)
10. [Task recommendation](#task-recommendation)
11. [Task decorators](#task-decorators)
12. [MapReduce related](#mapreduce-related)
13. [Extend luiti](#extend-luiti)
14. [FAQ](#faq)
15. [Run tests](#run-tests)
16. [Who uses Luiti?](#who-uses-luiti)

Keynote [Luiti - An Offline Task Management Framework](https://speakerdeck.com/mvj3/luiti-an-offline-task-management-framework)

Luiti command tool
------------------------
After installed package, you can use `luiti` command tool that contained
in the package.

```text
$ luiti
usage: luiti [-h] {ls,new,generate,info,clean,run,webui} ...

Luiti tasks manager.

optional arguments:
  -h, --help            show this help message and exit

subcommands:
  valid subcommands

  {ls,new,generate,info,clean,run,webui}
    ls                  list all current luiti tasks.
    new                 create a new luiti project.
    generate            generate a new luiti task python file.
    info                show a detailed task.
    clean               manage files that outputed by luiti tasks.
    run                 run a luiti task.
    webui               start a luiti DAG visualiser.
```

Luiti WebUI screenshots
------------------------
```bash
./example_webui_run.py
# or
luiti webui --project-dir your_main_luiti_package_path
```

Here's some screenshots from example_webui_run.py, just to give you an
idea of how luiti's multiple Python packages works.

1. Luiti WebUI list
![Luiti WebUI list](https://raw.githubusercontent.com/17zuoye/luiti/master/screenshots/luiti_webui_list.png)

2. Luiti WebUI show
![Luiti WebUI show](https://raw.githubusercontent.com/17zuoye/luiti/master/screenshots/luiti_webui_show.png)

3. Luiti Code show
![Luiti Code show](https://raw.githubusercontent.com/17zuoye/luiti/master/screenshots/luiti_code_show.png)


Core concepts based on time management
------------------------
### date type

#### Basic inheriting task classes:
0. TaskBase           (luigi.Task)
1. TaskHour           (TaskBase)
2. TaskDay            (TaskBase)
3. TaskWeek           (TaskBase)
4. TaskMonth          (TaskBase)
5. TaskRange          (TaskBase)

You can extend more date type by subclass `TaskBase`, and make sure the
date types are added in `TaskBase.DateTypes` too.

#### Hadoop ineriting task classes:
1. TaskDayHadoop      (luigi.hadoop.HadoopExt, TaskDay)
2. TaskWeekHadoop     (luigi.hadoop.HadoopExt, TaskWeek)
3. TaskRangeHadoop    (luigi.hadoop.HadoopExt, TaskRange)

#### Other task classes:
1. RootTask           (luigi.Task)
2. StaticFile         (luigi.Task)
3. MongoImportTask    (TaskBase)  # export json file from hdfs to mongodb.



Task specification and built-in properties and recommendation
------------------------
### Task naming conventions
1. One Task class per file.
2. Task class should be camel case ( e.g. `EnglishStudentAllExamWeek`), file name should be low case with underscore ( e.g.  `english_student_all_exam_week.py` ).
3. Task files should be under the directory of `luiti_tasks`. luiti use this convertion to linking tasks inner and outer of pacakges.
4. Task class name should be ended with date type, e.g. Day, Week, etc.  Please refer to `TaskBase.DateTypes`.


### Task builtin properties.
1. `date_value`. Required, even it's a Range type Task. This ensure that `output` will be written to a day directory.
2. `data_file`. The absolute output file path, it's a string format.
3. `data_dir`. The directory of the absolute output file path, it's a string format.
4. `root_dir`. The root path of this package. `data_file` and `data_dir` are all under it.
5. `output`. Basic Task's output class is LocalTarget, and Hadoop Task's output class is hdfs.HdfsTarget.
6. `date_str`. A datetime string, such as "20140901".
7. `date_type`. A string that generated from task class name, e.g. Day, Week, etc.
8. `date_value_by_type_in_last`. If current date type is Week, and it'll return the previous week's `date_value`.
8. `date_value_by_type_in_begin`. If current date type is Week, and it'll return Monday zero clock in the current week.
9. `date_value_by_type_in_end`. If current date type is Week, and it'll return Sunday 11:59:59 clock in the current week.
10. `pre_task_by_self`. Usually it returns previous task in the current date type. If reaches the time boundary of current date type, it returns RootTask.
11. `is_reach_the_edge`. It's semester at 17zuoye business.
12. `instances_by_date_range`. Class function, return all task intances list that belongs to current date range.
13. `task_class`. Return current task class.



Manage multiple projects in luiti
------------------------
#### The directory structure of a specific project.

We recommend you to organize every project's directory structure as the
below form, and it means it's also a normal Python package, for example:

```text
project_A                                            --- project directory
  setup.py                                           --- Python package install script
  README.markdown                                    --- project README
  project_A/                                         --- Python package install directory
  ├── __init__.py                                    --- mark current directories on disk as a Python package directories
  └── luiti_tasks                                    --- a directory name which indicates it contains several luiti tasks
      ├── __init__.py                                --- mark current directories on disk as a Python package directories
      ├── __init_luiti.py                            --- initialize luiti environment variables
      ├── exam_logs_english_app_day.py               --- an example luiti task
      ├── ..._day.py                                 --- another example luiti task
      └── templates                                  --- some libraries
            ├── __init__.py
            └── ..._template.py
```

After installing `luiti`, you can run following command line to generate
a project like above.
```bash
luiti new --project-name project_A
```

If other luiti projects needs to using this package, and you need to
install this package, to make sure luiti could find them in the
search path (`sys.path`) of Python modules.


#### How to link current Task to another Task that belongs to another pacakge?
Every luiti projects share the same structure, e.g.
`project_A/luiti_tasks/another_feature_day.py`. After config
`luigi.plug_packages("project_B", "project_C==0.0.2"])` in
`__init_luiti.py`, you can use `@luigi.ref_tasks("ArtistStreamDay')` to
indicate current Task to find `ArtistStreamDay` Task in current package
`project_A`, or related `project_B`, `project_C` packages.



A simple guide to Luigi
------------------------
Luigi's core concept is forcing you separting a big task into many small
tasks, and they're linked by **atomic** Input and Ouput. Luigi contains four
parts mainly:

1. **Output**. It must be implemented in `output` function, such as `LocalTarget` and `hdfs.HdfsTarget`.
2. **Input**. It must be implemented in `requires` function, and the
  function supposed to return some or None task instances.
3. **Parameters**. Parameters should be inherited from `luigi.Parameter`,
  e.g. `DateParameter`, etc.
4. **Execute Logic**. Use `run` function if running at local, or `mapper` and `reducer`
  if running on a distributed MapReduce YARN.

After finish the business logic implementation and test cases, You can
submit your task to the `luigid` background daemon. `luigid` will
process task dependencies automatically, this is done by checking
`output` is already `exists` (It's the Target class's function). And
luigi will guarantee that task instances are uniq in current
`luigid` background process by the task class name and parameters.

A simple example in luiti
------------------------
#### An official example from luigi.
Below code is copied from http://luigi.readthedocs.org/en/latest/example_top_artists.html

```python
import luigi
from collections import defaultdict

class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget("/data/artist_streams_%s.tsv" % self.date_interval)

    def requires(self):
        return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for input in self.input():
            with input.open('r') as in_file:
                for line in in_file:
                    timestamp, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in artist_count.iteritems():
                print >> out_file, artist, count
```

#### The same example written in luiti.

* First file: `artist_project/luiti_tasks/artist_stream_day.py`

```python
from luiti import *

class ArtistStreamDay(StaticFile):

    @cached_property
    def filepath(self):
        return TargetUtils.hdfs("/tmp/streams_%s.tsv" % self.date_str
```

* Second file: `artist_project/luiti_tasks/aggregate_artists_week.py`
```python
from luiti import *

@luigi.ref_tasks("ArtistStreamDay')
class AggregateArtistsWeek(TaskWeek):

    def requires(self):
        return [self.ArtistStreamDay(d1) for d1 in self.days_in_week]

    def output(self):
        return TargetUtils.hdfs("/data/artist_streams_%s.tsv" % self.date_str

    def run(self):
        artist_count = defaultdict(int)

        for file1 in self.input():
            for line2 in TargetUtils.line_read(file1):
                timestamp, artist, track = line.strip().split()
                artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in artist_count.iteritems():
                print >> out_file, artist, count
```

Optimizition notes:

1. luiti's task class is built in with `date_value` property, and converted
  into `Arrow` data type.
2. In ArtistStreamDay, `date_str` is transformed from `date_value`, and
  converted from a function into a instance property after the first call.
3. `@luigi.ref_tasks` bind ArtistStreamDay as AggregateArtistsWeek's
  instance property, so we can use `self.ArtistStreamDay(d1)` form to
  instantiate some task instances.
4. After AggregateArtistsWeek is inherited from `TaskWeek`, it'll has
  `self.days_in_week` property automatically.
5. `TargetUtils.line_read` replaced original function that needs two
  lines codes to complete the feature, and return a Generator directly.


#### Writing MapReduce in luiti
* MapReduce file: `artist_project/luiti_tasks/aggregate_artists_week.py`

```python
from luiti import *

@luigi.ref_tasks("ArtistStreamDay')
class AggregateArtistsWeek(TaskWeekHadoop):

    def requires(self):
        return [self.ArtistStreamDay(d1) for d1 in self.days_in_week]

    def output(self):
        return TargetUtils.hdfs("/data/weeks/artist_streams_%s.tsv" % self.date_str

    def mapper(self, line1):
        timestamp, artist, track = line.strip().split()
        yield artist, 1

    def reducer(self, artist, counts):
        yield artist, len(counts)
```

Yes, it's almost no difference to luigi, except the `self.days_in_week`
property and `@luigi.ref_tasks` decorator.


Installment & Develop requirements
------------------------
#### Installment
```bash
pip install luiti
```

Or lastest source code

```bash
git clone https://github.com/17zuoye/luiti.git
cd luiti
python setup.py install
```

#### Develop requirements
1. [Node.js](https://nodejs.org/download/) & [bower](http://bower.io/#install-bower)
2. pip requirements from setup.py
3. [tox](https://testrun.org/tox/latest/) & [nose](https://nose.readthedocs.org/)


Time library
-------------------------

The time library is [Arrow](http://crsmithdev.com/arrow/) , every Task
instance's `date_value` property is a arrow.Arrow type.

luiti will convert date paramters into local time zone automatically. If
you want to customize time, please prefer to use
 `ArrowParameter.get(*strs)` and `ArrowParameter.now()` to make sure you
 use the local time zone.



Task recommendation
-------------------------
#### Cache
We highly recommend you to use `cached_property`, like
[werkzeug](http://werkzeug.pocoo.org/docs/0.10/utils/) said, "A decorator that
converts a function into a lazy property. The function wrapped is called
the first time to retrieve the result and then that calculated result is
used the next time you access the value".

This function is heavily used in 17zuoye everyday, we use it to cache
lots of things, such as a big data dict.

```python
class AnotherBussinessDay(TaskDayHadoop):

    def requires(self):
        return [task1, task2, ...]

    def mapper(self, line1):
        k1, v1 = process(line1)
        yield k1, v1

    def reducer(self, k1, vs1):
        for v1 in vs1:
            v2 = func2(v1, self.another_dict)
            yield k1, v2

    @cached_property
    def another_dict(self):
        # lots of cpu/io
        return big_dict
```

#### Global utilities.
1. Basic utilities, such as os, re, json, defaultdict, etc.
2. Date processing utilities, they are arrow, ArrowParameter.
3. Cache utilities, `cached_property`.
4. Other utilities, such as IOUtils, DateUtils, TargetUtils, HDFSUtils, MRUtils, MathUtils,
     CommandUtils, CompressUtils.


Task decorators
------------------------
```python
# 1. Bind related tasks lazily, and can be used as instance property directly.
@luigi.ref_tasks(*tasks)

# 2. Support multiple file output in MapReduce
@luigi.multiple_text_files

# 3. Run MapReduce in local mode by only add one decorator.
@luigi.mr_local()

# 4. Check current task' data source's date range is satisfied.
@luigi.check_date_range()

# 5. Check current task can be runned in current date range.
@luigi.check_runtime_range(hour_num=[4,5,6], weekday_num=[1])

# 6. Bind other output file names except for the default `date_file`, and compacts with cleaning temporary files is the task is failed.
@luigi.persist_files(*files)

# 7. Let Task Templates under [luigi.contrib](https://github.com/spotify/luigi/tree/master/luigi/contrib) to follow with Luiti's Task convertion.
@luigi.as_a_luiti_task()

class AnotherBussinessDay(TaskDayHadoop):
    pass
```



MapReduce related
------------------------
#### Clean temporary file when a task fails.
When executing a MR job, luigi will write result to a file with
timestamp instantly. If the task successes, then rename to the name that
the task's original output file path. If the task fails, then YARN will
delete the temporary file automatically.

#### Read file in a Generator way.
1. Original way. `for line1 in TargetUtils.line_read(hdfs1)`, `line1` is an
  unicode type.
2. Read by JSON. `for json1 in TargetUtils.json_read(hdfs1)`, `json1` is
  a valid Python object.
3. Read in a K-V format. `for k1, v1 in TargetUtils.mr_read(hdfs1)`, `k1`
  is an unicode type, and `v1` is a Python object.

#### HDFS file object
We recommend to use `TargetUtils.hdfs(path1)`. This function compacts
with the MR file result data format that consists by "part-00000" file blocks.


#### MapReduce test cases

1. Add MapReduce input and output to `mrtest_input` and `mrtest_output`,
  these mimic the MapReduce processing.
2. In your test file, use `@MrTestCase` to decorator your test class,
  and add your task class to `mr_task_names` list.
3. (Optional) Add some config dict to `mrtest_attrs` to mimic properties
  that generated in production mode.
4. Run your test cases!

buy_fruit_day.py

```python
from luiti import *

class BuyFruitDay(TaskDayHadoop):

    def requries(self):
        ...

    def output(self):
        ...

    def mapper(self, line):
        ...
        yield uid, fruit

    def reducer(self, uid, fruits):
        price = sum([self.price_dict[fruit] for fruit in fruits])
        yield "", MRUtils.str_dump({"uid": uid, "price": price})

    @cache_property
    def price_dict(self):
        result = dict()
        for json1 in TargetUtils.json_read(a_fruit_price_list_file):
            result[json1["name"]] = json1["price"]
        return result

    def mrtest_input(self):
        return """
{"uid": 3, "fruit": "Apple"}
{"uid": 3, "fruit": "Apple"}
{"uid": 3, "fruit": "Banana"}
        """

    def mrtest_output(self):
        return """
{"uid": 3, "price": 7}
        """

    def mrtest_attrs(self):
        return {
          "price_dict": {
            "Apple": 3,
            "Banana": 1,
          }
        }

```

test file
```python
from luiti import MrTestCase

@MrTestCase
class TestMapReduce(unittest.TestCase):
    mr_task_names = [
            'ClassEnglishAllExamWeek',
            ...
           ]

if __name__ == '__main__':
  unittest.main()
```


Extend luiti
------------------------
Using `TaskBase`'s builtin `extend` class function to extend or overwrite
the default properties or functions, for example:

```python
TaskWeek.extend({
    'property_1' : lambda self: "property_2",
})
```

`extend` class function compacts with `function`, `property`, `cached_property`,
or any other attributes at the same time。When you want to overwrite
`property` and `cached_property`, you just need a function value, and
`extend` will automatically converted into `property` and
`cached_property` type.


FAQ
------------------------
Q: How atomic file is supported?

A: As luigi's document mentioned that "Simple class that writes to a temp file and moves it on close()
    Also cleans up the temp file if close is not invoked", so use the `self.input().open("r")` or
    `self.output().open("w")` instead of `open("some_file", "w")`.

Q: Can luigi detect the interdependent tasks?

A: It's not question inside of luigi, but it's a question about [topological sorting](https://en.wikipedia.org/wiki/Topological_sorting)
   as a general computer science topic. The task scheduler is implemented at `luigi/scheduler.py` .

Q: How to pass more parameters into luiti tasks?

A: You can create a key-value dict, `date_value` is the key, and your
customize parameters are the values.



If you have other unresolved questions, please feel free to ask
questions at [issues](https://github.com/17zuoye/luiti/issues).


Run tests
------------------------
```bash
nosetests
```



Who uses Luiti?
---------------
* [17zuoye](http://www.17zuoye.com/) Luiti born at this company.

Please let us know if your company wants to be featured on this list!


License
------------------------
MIT. David Chen @ 17zuoye.
