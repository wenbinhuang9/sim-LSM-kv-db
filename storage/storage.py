import os
import io
import struct
import time
from os import walk
from threading import Lock

from core import record
from new_storage import  FileHandler

class FileHandlerPool():
    def __init__(self, direcotry_name):
        self.direcotry_name = direcotry_name
        self.file_handler_map = {}

    def get(self, file):
        return self.file_handler_map[file]

    def set(self, file, format = "r+"):
        handler= FileHandler(self.direcotry_name, file, format)
        self.file_handler_map[file] = handler
        return handler

    def close(self):
        for handler in self.file_handler_map.values():
            handler.close()


class Storage():
    def __init__(self, directory_name):
        self.__default_max_file_size = 1024 * 4
        self.directory_name = directory_name
        self.active_file_handler = None
        self.file_handler_pool = FileHandlerPool(self.directory_name)
    ## todo check
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


    ## todo need? concurrent control ?
    def init_data(self):
        file_list = self.get_segment_file_list()

        for file in file_list:
            self.file_handler_pool.set(file)

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
        if self.active_file_handler == None:
            return self.create_active_file()

        if self.active_file_handler.last_pos() >= self.__default_max_file_size:
            return self.create_active_file()
        return self.active_file_handler

    def create_merge_file(self):
        new_segment_name = self.__create_segment_name()
        handler = self.file_handler_pool.set(new_segment_name, "w+")

        return handler

    def create_active_file(self):
        new_segment_name = self.__create_segment_name()
        self.active_file_handler = self.file_handler_pool.set(new_segment_name, "w+")

        return self.active_file_handler

    def __create_segment_name(self):
        timestamp = int(round(time.time() * 1000))
        return "segment" + str(timestamp)

    def write(self, data, file_handler = None):
        if file_handler == None:
            file_handler = self.__get_active_file_handle()

        last_pos = file_handler.write(data, True)

        return (last_pos, file_handler.file_name)

    def read(self, offset, len, file):
        handler = self.file_handler_pool.get(file)
        return handler.read(offset, len)

    def close(self):
        self.file_handler_pool.close()


class Reader():
    ## todo does here has a concurrent problem ?
    def __init__(self, path, file):
        self.path_file = path + "/" + file
        self.seek_pos = 0
        self.file_handle = FileHandler(path, file)
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

    def __read_one(self, len):
        value_bytes = self.file_handle.read(self.seek_pos, len)
        self.seek_pos += len
        return value_bytes




