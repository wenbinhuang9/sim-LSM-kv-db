import os
import io
import struct
import time
## todo try to make it stateless

from os import walk

from core import record


class Writer():
    def __init__(self, directory_name):
        self.__default_max_file_size = 1024 * 4
        self.directory_name = directory_name
        self.current_file_name = None
        self.active_file = None
        self.file_to_handler_map = {}
        self.path_to_file_map = {}

    def get_old_segment_file_list(self):
        all_file = self.get_segment_file_list()
        return all_file[:-1]
    def get_segment_file_list(self):
        ans = []
        for (dirpath, dirnames, filenames) in walk(self.directory_name):
            ans.extend(filenames)
        ## from older to newer
        ans.sort()
        return ans

    def __open_file(self, file):
        return open(self.directory_name + "/" + file, "r+")

    def init_data(self):
        file_list = self.get_segment_file_list()

        for file in file_list:
            self.file_to_handler_map[file] = self.__open_file(file)
            self.path_to_file_map[self.directory_name + "/" + file] = file

        ans = {}
        for file in file_list:
            self.get_key_offset_from_file(file, ans)

        return ans


    def get_key_offset_from_file(self, file, ans):
        reader = Reader(self.directory_name, file)
        while reader.hash_next():
            key, val, val_len, val_offset, tstamp = reader.read()
            if val == "__tomb_stone__":
                continue

            if key not in ans:
                ans[key] = record(file, val_offset, val_len, tstamp)
            else: ## key in ans, compare time
                tstamp_in_mem = ans[key].tstamp
                if tstamp > tstamp_in_mem:
                    ans[key] = record(file, val_offset, val_len, tstamp)

    def __get_active_file_handle(self):
        if self.active_file == None:
            return self.create_active_file()

        self.active_file.seek(0, io.SEEK_END)
        last_pos = self.active_file.tell()

        if last_pos >= self.__default_max_file_size:
            return self.create_active_file()
        return self.active_file

    def create_merge_file(self):
        new_segment_name = self.__create_segment_name()
        return self.create_new_file_handler(new_segment_name)

    def create_new_file_handler(self, new_segment_name):
        new_file_handler = open(self.directory_name + "/" + new_segment_name, "w+")
        self.file_to_handler_map[new_segment_name] = new_file_handler
        self.path_to_file_map[self.directory_name + "/" + new_segment_name] = new_segment_name
        return new_file_handler

    def create_active_file(self):
        new_segment_name = self.__create_segment_name()
        self.active_file = self.create_new_file_handler(new_segment_name)
        self.current_file_name = new_segment_name

        return self.active_file

    def __create_segment_name(self):
        timestamp = int(round(time.time() * 1000))
        return "segment" + str(timestamp)


    ##todo ensure only one thread can access here.
    def write(self, data, file_handler = None):
        if file_handler == None:
            file_handler = self.__get_active_file_handle()

        file_handler.seek(0, io.SEEK_END)
        last_pos = file_handler.tell()
        file_handler.write(data)
        ## todo need flush right now?
        file_handler.flush()

        return (last_pos, self.path_to_file_map[file_handler.name])

    def read(self, offset, len, file):
        handler = self.file_to_handler_map[file]
        handler.seek(offset)
        res = handler.read(len)
        return res

    def close(self):
        for file in self.file_to_handler_map.values():
            file.close()


class Reader():
    ## todo does here has a concurrent problem ?
    def __init__(self, path, file):
        self.path_file = path + "/" + file
        self.seek_pos = 0
        self.file_handle = open(self.path_file, "r")
        self.file_size = os.path.getsize(self.path_file)

    def hash_next(self):
        return self.seek_pos !=  self.file_size

    def __int_deserialized(self, obj):
        ## it always returns a tuple, we just return the first element.

        res = struct.unpack(">I", obj)
        return res[0]
    def __long__deserialized(self, obj):
        res = struct.unpack(">Q", obj)
        return res[0]

    def read(self):
        tstamp_bytes = self.__read_one(8)
        tstamp = self.__long__deserialized(tstamp_bytes)
        key_len_bytes = self.__read_one(4)
        value_len_bytes = self.__read_one(4)
        key_len = self.__int_deserialized(key_len_bytes)
        value_len = self.__int_deserialized(value_len_bytes)
        key_bytes = self.__read_one(int(key_len))
        val_offset = self.seek_pos
        val_bytes = self.__read_one(int(value_len))

        return (key_bytes, val_bytes, value_len, val_offset, tstamp)

    def __read_one(self, offset):
        self.file_handle.seek(self.seek_pos)

        value_bytes = self.file_handle.read(offset)

        self.seek_pos += offset

        return value_bytes




