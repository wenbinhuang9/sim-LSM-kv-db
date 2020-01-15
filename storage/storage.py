import os
import io
import struct

## todo try to make it stateless
class Writer():
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.default_file_name = "active"
        self.active_file = open(self.directory_name + "/" + self.default_file_name, 'r+')

    def write(self, data):
        return self.write_file(data, self.active_file)

    ## todo return the write file name
    def write_file(self, data, file_handler):
        file_handler.seek(0, io.SEEK_END)
        last_pos = file_handler.tell()
        file_handler.write(data)
        file_handler.flush()

        return (last_pos, self.default_file_name)

    def read(self, offset, len):
        return self.read(offset, len)

    def read(self, offset, len, file = None):
        if file == None:
            self.active_file.seek(offset)
            res = self.active_file.read(len)
            return res

        handler = open(self.directory_name + "/" + file)
        handler.seek(offset)
        res = handler.read(len)
        handler.close()
        return res

class Reader():
    ## todo does here has a concurrent problem ?
    def __init__(self, path, file):
        self.path_file = path + "/" + file
        self.seek_pos = 0
        self.file_handle = open(self.path_file, "r+")
        self.file_size = os.path.getsize(self.path_file)

    def hash_next(self):
        return self.seek_pos !=  self.file_size

    def __int_deserialized(self, obj):
        ## it always returns a tuple, we just return the first element.
        res = struct.unpack(">I", obj)
        return res[0]

    def read(self):
        key_len_bytes = self.__read_one(4)
        value_len_bytes = self.__read_one(4)
        key_len = self.__int_deserialized(key_len_bytes)
        value_len = self.__int_deserialized(value_len_bytes)
        key_bytes = self.__read_one(int(key_len))
        val_bytes = self.__read_one(int(value_len))

        return (key_bytes, val_bytes)

    def __read_one(self, offset):
        self.file_handle.seek(self.seek_pos)

        value_bytes = self.file_handle.read(offset)

        self.seek_pos += offset

        return value_bytes




