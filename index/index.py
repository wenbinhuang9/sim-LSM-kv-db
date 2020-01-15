
from storage.storage import Writer
class index():
    def __init__(self, writer):
        self.__idx = {}
        self.sto = writer


    def get(self, key):
        if key not in self.__idx:
            return None
        file, offset, val_len = self.__idx[key]
        return self.sto.read(offset, val_len, file)


    def set(self, key, file, offset, val_len):
        self.__idx[key] = (file, offset, val_len)

        return True

    def delete(self, key):
        del self.__idx[key]


    def get_key_file(self, key):

        if key not  in self.__idx:
            return None
        file, offset, val_len = self.__idx[key]

        return file
