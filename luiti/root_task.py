# -*- coding: utf-8 -*-

import os
import luigi
from luigi import LocalTarget

class RootTask(luigi.Task):

    def output(self):
        return LocalTarget(os.path.realpath(__file__)) # exist for ever
