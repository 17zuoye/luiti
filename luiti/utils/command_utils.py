#-*-coding:utf-8-*-

import os

class CommandUtils:

    @staticmethod
    def execute(command_str, dry=False, verbose=True):
        if verbose:   print "[command]", command_str
        if dry:       return False

        import commands
        return commands.getstatusoutput(command_str)
