from core import record
from storage.storage import Storage
class index():
    def __init__(self, writer):
        self.__idx = {}
        self.sto = writer


    def get(self, key):
        if key not in self.__idx:
            return None
        data = self.__idx[key]
        return self.sto.read(data.val_offset, data.val_len,data.file_name)

    def get_key_tstamp(self, key):
        val= self.__idx[key]
        return val.tstamp

    def set(self, key, file, offset, val_len, tstamp):
        self.__idx[key] = record(file, offset, val_len,tstamp)

        return True
    def set(self, key, data):
        self.__idx[key] = data

        return True
    def delete(self, key):
        del self.__idx[key]


    def get_key_file(self, key):
        if key not  in self.__idx:
            return None
        data = self.__idx[key]

        return data.file_name
