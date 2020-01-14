from storage.storage import storage
from index.index import index
import struct
class Serialized():

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
class Service():

    def __init__(self):
        self.serializer = Serialized()
        self.sto = storage()
        self.idx = index()

    def get(self, key):
        return self.idx.get(key)

    def set(self, key, value):
        encoded_key = self.serializer.serialized(key)
        encoded_value = self.serializer.serialized(value)
        encoded_data = self.serializer.serialized(key + value)

        key_len = len(encoded_key)
        val_len = len(encoded_value)
        key_len_bytes = self.serializer.serialized(key_len)
        val_len_bytes = self.serializer.serialized(val_len)

        data = key_len_bytes + val_len_bytes + encoded_data

        offset = self.sto.write(data)
        offset += (4 + 4 + len(encoded_key))
        res = self.idx.set(key, None, offset, len(encoded_value))

        if not res:
            print ("bad happen in Service set")

        return True


    def delete(self, key):
        tomb_stone = "__tomb_stone__"
        self.set(key, tomb_stone)
        self.idx.delete(key)
