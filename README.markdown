Luiti
========================
Luigi是一套基于python语言构建的复杂流式批处理任务管理系统。它也仅仅是一个任务
调度系统，具体逻辑全都由 Task 自己去实现，比如分布式计算交由 Hadoop 里的 YARN 实现处理。

Luiti 是构建于 Luigi 之上的主要作用于时间管理相关的插件, 即
 Luiti = Luigi + time。


luiti 优势
------------------------
1. 按时间和业务类型等属性去多目录划分基础表, 中间表, 统计表 等。[#](#core-concepts-based-on-time-management)
2. 支持无缝多项目任务管理依赖, 兼容常规 Python 库引用机制。[#](#manage-multiple-projects-in-luiti)
3. 任务的运行时间和业务时间的满足条件检查。[#](#task-decorators)
4. 众多 MapReduce / IO 实用操作函数。[#](#mapreduce-related)
5. 基于输入输出数据的 MapReduce 测试方案。[#](#mr-test)
6. 内置支持 Task 基类扩展。[#](#extend-luiti)

luigi 预备知识
------------------------
1. 英文文档   http://luigi.readthedocs.org/en/latest/index.html
  （推荐看这个，官方详细文档, 含最新)
2. 中文介绍   http://vincentzhwg.iteye.com/blog/2063388   (Luigi
    －－基于Python语言的流式任务调度框架教程, 国内的人写的，不保
   证正确性。)


luigi 简单介绍
------------------------
luigi 的核心概念是用一系列 Task 类来管理任务，主要包含四个部分:

1. 输出。放置在 `output` 方法里。比如 LocalTarget 和 hdfs.HdfsTarget
   两种类型。
2. 输入。放置在 `requires` 方法里, 该方法返回若干 Task instances
    列表，每个 instance 都含有在 1. 里定义的 `output` 。
3. 参数。 都继承自 Parameter ，比如 DateParameter 等。
4. 执行逻辑。比如 `run` 或 `mapper` + `reducer` 方法。


在写完 Task 业务实现和测试后，提交到 luigid 后台进程即可。 luigid
会根据 `requires` 自动去处理任务依赖, 这是通过检查 `output` 是否存
在而实现的(`output` 类里有 `exists` 方法)。并根据 Task 类名 + Task
参数 保证在当前 luigid 后台进程里的唯一性。


luiti 简单示例
------------------------
#### luigi 的写法示例。以上代码 Copy 自 [luigi官方示例](http://luigi.readthedocs.org/en/latest/example_top_artists.html)
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

#### 同一个例子的 luiti 写法

* 第一个文件: `artist_project/luiti_tasks/artist_stream_day.py`

```python
from luiti import *

class ArtistStreamDay(StaticFile):

    @cached_property
    def filepath(self):
        return "/data/artist_streams_%s.tsv" % self.date_str
```

* 第二个文件: `artist_project/luiti_tasks/aggregate_artists_week.py`
```python
from luiti import *

@luigi.ref_tasks("ArtistStreamDay')
class AggregateArtistsWeek(TaskWeek):

    def requires(self):
        return [self.ArtistStreamDay(d1) for d1 in self.days_in_week]

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

优化说明:

1. luiti 的 Task 类均直接内置了 `date_value` 属性，并转为 Arrow 类型。
2. ArtistStreamDay 里的 `date_str` 由 `date_value` 转换而来，在初次调用后就被转成实例的属性了。
3. `@luigi.ref_tasks` 就自动绑定了 ArtistStreamDay 到 AggregateArtistsWeek  的实例属性了，
   所以可以用 `self.ArtistStreamDay(d1)` 形式来直接声明实例了。
4. 在 AggregateArtistsWeek 继承了 `TaskWeek` 后就自动有了 `self.days_in_week` 属性了。
5. `TargetUtils.line_read` 替换了原来两行代码需要完成的功能，直接返回一个迭代器(generator)。


#### luiti 的 MapReduce 写法
* 第一个文件: `artist_project/luiti_tasks/artist_stream_day.py`

```python
from luiti import *

class ArtistStreamDay(StaticFile):

    @cached_property
    def filepath(self):
        return TargetUtils.hdfs("/data/artist_streams_%s.tsv" % self.date_str
```

* 第二个文件: `artist_project/luiti_tasks/aggregate_artists_week.py`
```python
from luiti import *

@luigi.ref_tasks("ArtistStreamDay')
class AggregateArtistsWeek(TaskWeekHadoop):

    def requires(self):
        return [self.ArtistStreamDay(d1) for d1 in self.days_in_week]

    def mapper(self, line1):
        timestamp, artist, track = line.strip().split()
        yield artist, 1

    def reducer(self, artist, counts):
        yield artist, len(counts)
```

优化说明: 在 MapReduce 计算模式下，这种简单业务实际上比原来代码还精简。其他和原生 luigi 没多大区别。






安装
------------------------
```bash
pip install luiti
```

或者最新源码

```bash
git clone https://github.com/17zuoye/luiti.git
cd luiti
python setup.py install
```


luiti 命令行
------------------------
安装后就可以直接在当前 Shell 里使用 luiti 命令了, 比如:
```text
$ luiti
usage: luiti [-h] {ls,info,clean,run} ...

Luiti tasks manager.

optional arguments:
  -h, --help           show this help message and exit

subcommands:
  valid subcommands

  {ls,info,clean,run}
    ls                 list all current luiti tasks.
    info               show a detailed task.
    clean              manage files that outputed by luiti tasks.
    run                run a luiti task.
```

Core concepts based on time management
------------------------
### 时间类型

#### 基础继承类:
0. TaskBase           (luigi.Task)
1. TaskHour           (TaskBase)
2. TaskDay            (TaskBase)
3. TaskWeek           (TaskBase)
4. TaskMonth          (TaskBase)
5. TaskRange          (TaskBase)

所以这里是可以扩展更多时间类型的, 并确保在 `TaskBase.DateTypes` 里也加上。

#### Hadoop继承类:
1. TaskDayHadoop      (luigi.hadoop.HadoopExt, TaskDay)
2. TaskWeekHadoop     (luigi.hadoop.HadoopExt, TaskWeek)
3. TaskRangeHadoop    (luigi.hadoop.HadoopExt, TaskRange)

#### 其他类:
1. RootTask           (luiti.Task)
2. StaticFile         (luiti.Task)
3. MongoImportTask    (TaskBase) # 导出 MR 结果到 mongodb 。


### 时间库

采用的时间类库是 [Arrow](http://crsmithdev.com/arrow/) , 每一个 Task
instance 具体引用的时间 instance 都是 arrow.Arrow 类型。

在 luiti 插件里均直接转换到本地时区。如果需要自定义时间，请优先使用
 `ArrowParameter.get(*strs)` 和 `ArrowParameter.now()` 等 以保证都
 转换到本地时区。


Task 规范 和 内置属性 和 推荐做法
------------------------
### Task 命名规范
1. 一个 Task 类，一个文件。
2. Task 类为驼峰方式(比如 `EnglishStudentAllExamWeek` )，文件名为
   小写加下划线方式(比如 `english_student_all_exam_week.py` ) 。
3. Task 文件所位于的目录均为 `luiti_tasks`, 这样支持了 装饰器
   `@luigi.ref_tasks(*tasks)` 相互惰性自动引用，也支持多项目目录
   Task 引用。
4. Task 类名必须以 Day, Week 等时间类型结尾，具体参考 `TaskBase.DateTypes` 。


### Task 内置属性
1. `date_value` 。强制参数, 即使是 Range 类型的 Task 也是需要的，这样
   保证结果会 `output` 到某天的目录。另外在 `__init__` 时会被转换称
   arrow.Arrow 的本地时区类型。
2. `data_file` 。结果输出的绝对地址，字符串类型。
3. `data_dir` 。结果输出的绝对地址目录，字符串类型。
4. `root_dir` 。输出的根目录, `data_file` 和 `data_dir` 都是在其之下。
5. `output` 。基本类输出到 LocalTarget , Hadoop类型会输出到 hdfs.HdfsTarget 。
6. `date_str` 。返回 20140901 格式的时间字符串。
7. `date_type` 。从类名中获取并返回 Day, Week 等字符串。
8. `date_value_by_type_in_last` 。如果时间类型是 Week ，就返回上周一的
   arrow.Arrow 。
8. `date_value_by_type_in_begin` 。如果时间类型是 Week ，就返回当前周一的
   零点。
9. `date_value_by_type_in_end` 。如果时间类型是 Week ，就返回当前周日的
   11:59:59。
10. `pre_task_by_self` 。一般情况下返回当前时间类型的上个时间点的任务。
   如果达到了该任务类型的时间边界，就返回 RootTask 。
11. `is_reach_the_edge` 。在 17zuoye 的业务是学期边界。
12. `instances_by_date_range`。类方法。返回属于某周期里的所有当前任务实例列表。
13. `task_class`。返回当前 Task 类。


### Task 推荐做法

#### 缓存
强烈推荐使用 [Werkzeug. The Python WSGI Utility Library](http://werkzeug.pocoo.org/) 实现的 `cached_property` , 是 Python 内置的 property 的缓存版本，惰性载入耗CPU和IO
资源的字典数据。示例:

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

#### 全局实用工具
1. os, re, json, defaultdict 等基本工具。
2. arrow, ArrowParameter 时间处理工具。
3. `cached_property`, 缓存里已介绍。
4.  IOUtils, DateUtils, TargetUtils, HDFSUtils, MRUtils, MathUtils,
     CommandUtils, CompressUtils, 使用见具体实现。


Task decorators
------------------------
```python
# 1. 惰性绑定相关 Task, 直接作为 instance property 使用。
@luigi.ref_tasks(*tasks)

# 2. 检查当前日期是否满足Task依赖的时间区间。
@luigi.check_date_range()

# 3. 检查 Task 可以运行的时间点。
@luigi.check_runtime_range(hour_num=[4,5,6], weekday_num=[1])

# 4. 绑定除了默认的 `date_file` 之外的输出文件名。同时兼容了任务失败时的删除处理。
@luigi.persist_files(*files)

class AnotherBussinessDay(TaskDayHadoop):
    pass
```



MapReduce related
------------------------
#### 任务失败时的临时文件处理
执行 MR 时, luigi 会先输出到有时间戳的临时文件。如果任务成功，则重命名
到原先任务指定的名字。如果任务失败，则 YARN 会自动删除该临时文件。

#### MR 键值解析
luiti 推荐是 组合键 unicode 作为 Map Key, 而 dict (序列化为json格式) 作为 Reduce Value 。推荐使
用 `MRUtils.split_mr_kv`, 该函数会返回 [unicode, dict] 结果。

#### MR 键的组合处理
1. `MRUtils.concat_prefix_keys(*keys)` 。组合多个键。
2. `MRUtils.is_mr_line(line1)` 。判断是否是 MR 格式的行输出。
3. `MRUtils.split_prefix_keys(line_part_a)` 。用默认分隔符 分割, 返回字符串列表。
4. `MRUtils.select_prefix_keys(line_part_a, idxes=None)` 。用索引来取得组合键的
    某些部分，并支持修复因 json 序列化带来的误操作（在首尾多了 `"` 引号）。

#### MR 读入文件处理, generator 方式
1. 原始读入。 `TargetUtils.line_read(hdfs1)`。返回 unicode。
2. JSON读入。 `TargetUtils.json_read(hdfs1)`。返回 json 相关类型。
3. MR读入。   `TargetUtils.mr_read(hdfs1)`。返回 [unicode, json 相关类型] 键值对形式。

示例:
````python
for k1, v1 in MRUtils.mr_read(hdfs1):
    isinstance(k1, unicode)
    isinstance(v1, dict)
```

#### HDFS 文件对象
使用 `TargetUtils.hdfs(path1)` 。该函数同时兼容了 MR 按 `part-00000`
分文件块的数据格式。

#### MR test
1. 给继承 Hadoop 相关Task基类 的 具体业务 Task 加上 `mrtest_input` 和
    `mrtest_output` 两个方法，分别用于 MR 的文本输入和输出。
2. 在测试代码里加上如下代码，luiti 就会自动给 `mr_task_names` 里的所有 Task
   生成测试用例，然后按正常方式跑 Python 测试用例即可。
3. 还可以用 `mrtest_attrs` 生成该实例上的多个字典属性。

```python
from luiti import MrTestCase

@MrTestCase
class TestMapReduce(unittest.TestCase):
    mr_task_names = [
            'ClassEnglishAllExamWeek',
            ...
           ]

if __name__ == '__main__': unittest.main()
```


Manage multiple projects in luiti
------------------------
#### 具体单个项目的目录结构
每个项目目录结构建议为以下格式，即可以当作一个正规的 Python package 来使用， 比如:

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

在安装好 `luiti` 后，运行如下命令行即可生成上述的项目基本目录结构，
```bash
luiti new project_A
```

这个树目录其实就是可以用来安装 package 的 Python 项目, 在根的 `project_A` 目录
下运行 `python setup.py install` 即可把当前项目安装到当前 Python 环境的 package
引用路径(即 `sys.path` )下。


#### 如何关联另一个项目的某个 Task
每个项目都是类似 `project_A/luiti_tasks/another_feature_day.py` 结构，在 `__init_luiti.py` 只要
用 `luigi.plug_packages("project_B", "project_C==0.0.2"])` 后， 像 `@luigi.ref_tasks("ArtistStreamDay')`
就会现在当前 `project_A`, 和相关的 `project_B`, `project_C` 里去找 ArtistStreamDay Task 了。


Extend luiti
------------------------
使用 TaskBase 里自带 extend 类方法扩展或者覆写默认属性或方法，比如:

```python
TaskWeek.extend({
    'property_1' : lambda self: "property_2",
})
```

`extend` 类方法同时兼容了 `function`, `property`, `cached_property`,
或者其他任意类属性。在覆写 `property` 和 `cached_property`
传一个函数值即可，`extend` 会自动转化为本来的 `property` 和
`cached_property` 类型。


Run tests
------------------------
```bash
./tests/run.sh
```


License
------------------------
MIT. David Chen @ 17zuoye.
