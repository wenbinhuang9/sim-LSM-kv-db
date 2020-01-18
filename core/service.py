from core import record
from storage.storage import Writer, Reader
from index.index import index
import os
import struct
import time
tomb_stone = "__tomb_stone__"
class Serialized:
    def __int_serialized(self, obj):
        return struct.pack(">I", obj)
    def __int_deserialized(self, obj):
        ## it always returns a tuple, we just return the first element.
        res = struct.unpack(">I", obj)
        return res[0]
    def deserialized(self, obj):
        if isinstance(obj, int):
            return self.__int_deserialized()
        return obj.decode("utf-8")
    def serialized(self, obj):
        if isinstance(obj, int):
            return self.__int_serialized(obj)
        return obj.encode("utf-8")

class Service:
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.serializer = Serialized()
        self.sto = Writer(directory_name)
        self.idx = index(self.sto)
        self.init_data()

    def init_data(self):
        ans = self.sto.init_data()

        for key, data in ans.items():
            self.idx.set(key, data)

    def get(self, key):
        return self.idx.get(key)

    def set(self, key, value):
        return self.set(key, value,None, None)

    def __serilized_long(self, obj):
        return struct.pack(">Q", obj)

    def set(self, key, value, tstamp = None, file_hander = None):
        if tstamp == None:
            tstamp = long(round(time.time() * 1000))

        ##todo bug here, about the encoding and decoding bug during pack
        tstamp_bytes = self.__serilized_long(tstamp)

        encoded_key = self.serializer.serialized(key)
        encoded_value = self.serializer.serialized(value)
        encoded_data = self.serializer.serialized(key + value)
        key_len = len(encoded_key)
        val_len = len(encoded_value)
        key_len_bytes = self.serializer.serialized(key_len)
        val_len_bytes = self.serializer.serialized(val_len)

        data = tstamp_bytes + key_len_bytes + val_len_bytes + encoded_data

        offset, filename = self.sto.write(data, file_hander)

        offset += (8 + 4 + 4 + len(encoded_key))
        res = self.idx.set(key, record(filename, offset, len(encoded_value), tstamp))

        if not res:
            print ("bad happen in Service set")

        return True


    def delete(self, key):
        self.set(key, tomb_stone)
        self.idx.delete(key)

    def merge(self):
        old_file_list = self.sto.get_old_segment_file_list()

        merge_file_handler =self.sto.create_merge_file()
        for file in old_file_list:
            reader = Reader(self.directory_name, file)
            while reader.hash_next():
                key_bytes, value_bytes, value_len, val_offset, tstamp = reader.read()
                key = self.serializer.deserialized(key_bytes)
                value = self.serializer.deserialized(value_bytes)

                tstamp_in_mem = self.idx.get_key_tstamp(key)
                ## only those new data will be updated
                if tstamp == tstamp_in_mem and value != tomb_stone:
                    ## todo does here has a problem of concurrency?
                    self.set(key,value, merge_file_handler)
            os.remove(self.directory_name +"/" + file)

        return True

    def close(self):
        self.sto.close()