from storage.storage import Writer, Reader
from index.index import index

import struct

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
        return obj.encode("utf-8")
    def serialized(self, obj):
        if isinstance(obj, int):
            return self.__int_serialized(obj)
        return obj.decode("utf-8")

class Service:
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.serializer = Serialized()
        self.sto = Writer(directory_name)
        self.idx = index(self.sto)

    def get(self, key):
        return self.idx.get(key)

    def set(self, key, value):
        return self.set(key, value, None)

    def set(self, key, value, file = None):
        encoded_key = self.serializer.serialized(key)
        encoded_value = self.serializer.serialized(value)
        encoded_data = self.serializer.serialized(key + value)

        key_len = len(encoded_key)
        val_len = len(encoded_value)
        key_len_bytes = self.serializer.serialized(key_len)
        val_len_bytes = self.serializer.serialized(val_len)

        data = key_len_bytes + val_len_bytes + encoded_data
        if file == None:
            offset, filename = self.sto.write(data)
        else:
            file_hander = open(self.directory_name + "/" + file, "w+")
            ## todo optimized here
            offset, filename = self.sto.write_file(data, file_hander)
            filename = file
            file_hander.close()

        offset += (4 + 4 + len(encoded_key))
        res = self.idx.set(key, filename, offset, len(encoded_value))

        if not res:
            print ("bad happen in Service set")

        return True


    def delete(self, key):
        self.set(key, tomb_stone)
        self.idx.delete(key)

    def compact(self, file):
        reader = Reader(self.directory_name, file)
        mem = {}
        while reader.hash_next():
            key_bytes, value_bytes = reader.read()
            key = self.serializer.deserialized(key_bytes)
            value = self.serializer.deserialized(value_bytes)
            mem[key] = value

        compact_file = "compact" + file
        for key, value in mem.items():
            if value == tomb_stone:
                continue

            key_file = self.idx.get_key_file(key)
            ## important here
            if key_file == file:
                self.set(key, value, compact_file)
        return True
