Luiti
========================
Luigi是一套基于python语言构建的复杂流式批处理任务管理系统。

Luiti 是构建于 Luigi 之上的主要作用于时间管理相关的插件, 即
 Luiti = Luigi + time。



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
3. 参数 , 都继承自 Parameter ，比如 DateParameter 等。
4. 执行逻辑  `run` 或 `mapper` + `reducer` 方法。


在写完 Task 业务实现和测试后，提交到 luigid 后台进程即可。 luigid
会根据 `requires` 自动去处理任务依赖, 这是通过检查 `output` 是否存
在而实现的(`output` 类里有 exists 方法)。并根据 Task 类名 + Task
参数 保证在当前 luigid 后台进程里的唯一性。


简单示例
------------------------
```python
class AggregateArtists(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget("data/artist_streams_%s.tsv" % self.date_interval)

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

以上代码 Copy 自 [luigi官方示例](http://luigi.readthedocs.org/en/latest/example_top_artists.html)


基于时间管理的核心概念
------------------------

## 时间类型

基础继承类:
0. TaskBase           (luigi.Task)
1. TaskHour           (TaskBase)
2. TaskDay            (TaskBase)
3. TaskWeek           (TaskBase)
4. TaskMonth          (TaskBase)
5. TaskRange          (TaskBase)

Hadoop继承类:
1. TaskDayHadoop      (luigi.hadoop.HadoopExt, TaskDay)
2. TaskWeekHadoop     (luigi.hadoop.HadoopExt, TaskWeek)
3. TaskRangeHadoop    (luigi.hadoop.HadoopExt, TaskRange)

其他类:
1. RootTask           (RootTask)
2. StaticFile         (RootTask)


## 时间库

采用的时间类库是 [Arrow](http://crsmithdev.com/arrow/) , 每一个Task
instance 具体引用的时间 instance 都是 arrow.Arrow 类型。

在 luiti 插件里均直接转换到本地时区。如果需要自定义时间，请优先使用
 `ArrowParameter.get(*strs)` 和 `ArrowParameter.now()` 等 以保证都
 转换到本地时区。


Task 规范 和 内置属性 和 推荐做法
------------------------
## Task 命名规范
1. 一个 Task 类，一个文件。
2. Task 类为驼峰方式(比如 `EnglishStudentAllExamWeek` )，文件名为
   小写加下划线方式(比如 `english_student_all_exam_week.py` ) 。
3. Task 文件所位于的目录均为 `luiti_tasks`, 这样支持了 装饰器
   `@luigi.ref_tasks(*tasks)` 相互惰性自动引用，也支持多项目目录
   Task 引用。
4. Task 类名必须以 Day, Week 等时间类型结尾，具体参考 `TaskBase.DateTypes` 。


## Task 内置属性
1. `date_value` 。强制参数, 即使是 Range 类型的 Task 也是需要的，这样
   保证结果会 `output` 到某天的目录。另外在 `__init__` 时会被转换称
   arrow.Arrow 的本地时区类型。
2. `data_file` 。结果输出的绝对地址，字符串类型。
3. `data_dir` 。结果输出的绝对地址目录，字符串类型。
4. `root_dir` 。输出的根目录, `data_file` 和 `data_dir` 都是在其之下。


instances_by_date_range

## Task 推荐做法

Task 装饰器
------------------------
```python
# 1. 惰性绑定相关 Task, 直接作为 instance property 使用。
@luigi.ref_tasks(*tasks)

# 2. 检查当前日期是否满足Task依赖的时间区间。
@luigi.check_date_range()

# 3. 检查 Task 可以运行的时间点。
@luigi.check_runtime_range(hour_num=[4,5,6], weekday_num=[1])

# 4. 绑定除了默认的 `date_file` 之外的 输出文件名。
@luigi.persist_files(*files)
```


MapReduce 相关
------------------------
输出到有时间戳的临时文件，如果任务失败，则 YARN 会自动删除该临时文件。


实用工具库
------------------------
### MRUtils





Run tests
------------------------
```bash
./tests/run.sh
```


License
------------------------
MIT. David Chen @ 17zuoye.
