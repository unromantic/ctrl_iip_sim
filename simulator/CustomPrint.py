class CustomPrint:
    def __init__(self, name):
        self._program_name = name
        return

    def define_subname(self, subname):
        self._program_name = self._program_name + "][" + subname
        return

    def define_new_name(self, new_name):
        self._program_name = new_name
        return

    def printc(self, string):
        print("[%s] %s" % (self._program_name, string))
        return

    def get_full_name(self):
        return self._program_name
