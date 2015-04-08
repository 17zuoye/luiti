#-*-coding:utf-8-*-

import os
import commands

class CommandUtils:

    @staticmethod
    def execute(command_str, dry=False, verbose=True):
        if verbose:   print "[command]", command_str
        if dry:       return False

        #return commands.getstatusoutput(command_str)
        return os.system(command_str) # print logs in realtime.
