import os
import sys

class Client:
    def exists(self, filename):
        return os.path.exists(filename)

    def ls(self, path):
        return [os.path.join(path, f) for f in os.listdir(path)]
    
    def mkdir(self, path):
        os.mkdir(path)

    def mv(self, source, target):
        os.rename(source, target)

    def open(self, filename, mode='rb'):
        return open(filename, mode)

    def remove(self, path, recursive=False):
        try:
            os.remove(path)
        except IsADirectoryError:
            if recursive:
                import shutil
                shutil.rmtree(path)
            else:
                os.rmdir(path)

    def rm(self, path, recursive=False):
        self.remove(path, recursive)