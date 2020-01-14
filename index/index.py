
from storage.storage import storage
class index():
    def __init__(self):
        self.__idx = {}
        self.sto = storage()


    def get(self, key):
        if key not in self.__idx:
            return None
        file, offset, val_len = self.__idx[key]
        return self.sto.read(offset, val_len)


    def set(self, key, file, offset, val_len):
        self.__idx[key] = (file, offset, val_len)

        return True

    def delete(self, key):
        del self.__idx[key]