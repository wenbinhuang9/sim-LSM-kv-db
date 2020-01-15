
import os

## todo adjust architecture of the code
## change the code to stateless
class Writer():
    def __init__(self):
        self.path = "../file/active"
        self.active_file = open(self.path, 'r+')
        self.old_file_list = []

    def write(self, data):
        return self.write(data, self.active_file)

    ## append
    def write(self, data, file_handler):
        last_pos = len(file_handler)
        file_handler.seek(last_pos)
        file_handler.write(data)
        file_handler.flush()

        return last_pos

    def create_file(self, file, name):
        file_handler = open(file + name, "w+")
        return file_handler

    def read(self, offset, len):
        self.active_file.seek(offset)

        return self.active_file.read(len)




class Reader():
    def __init__(self, path, file):
        path_file = path + file
        self.seek_pos = 0
        self.file_handle = open(path_file, "r+")
        self.file_size = os.path.getsize(self.path_file)

    def hash_next(self):
        return self.last_pos != os.path.getsize(self.path)

    def read(self):
        key_len = self.__read_one(4)
        value_len = self.__read_one(4)
        key_bytes = self.__read_one(key_len)
        val_bytes = self.__read_one(value_len)

        return (key_bytes, val_bytes)

    def __read_one(self, offset):
        self.file_handle.seek(self.seek_pos)
        value_bytes = self.read(offset)

        self.seek_pos += offset

        return value_bytes




