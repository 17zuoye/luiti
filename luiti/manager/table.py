#-*-coding:utf-8-*-


from tabulate import tabulate


class Table(object):

    @staticmethod
    def print_all_tasks(result):
        """ input from Loader.load_all_tasks """
        def task_cls_inspect(cls):
            return cls.__name__

        task_headers = ["", "All Tasks"]
        task_table = [[idx1+1, task_cls_inspect(item1['task_cls'])] for idx1, item1 in enumerate(result['success'])]
        task_table.extend([["total", len(result['success'])]])

        print
        print tabulate(task_table, task_headers, tablefmt="grid")
        print

        if result['failure']:
            print
            print "[warn] failure parsed files"
            print
            for failure1 in result['failure']:
                print "[task_file] ", failure1['task_file']
                print "[err] ",       failure1['err']
                print
