"""
API are listed at parent __ini__.py .


Example:
    class TaskDayHadoop(luigi.hadoop.HadoopExt, TaskDay):
        pass

TaskDay.__init__ will overwrite luigi.hadoop.HadoopExt's.


NOTE: luigi.hadoop.HadoopExt will overwrite TaskDay

"""
