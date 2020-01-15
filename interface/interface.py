## a state handler
from core.service import Service

class Bencaskhandler():
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.service = Service(directory_name)

    def open(self, directory_name):
        handler = Bencaskhandler(directory_name)
        return handler

    def get(self, key):
        return self.service.get(key)


    def put(self, key, val):
        return self.service.set(key, val)

    def delete(self, key):
        return self.service.delete(key)

    def merge(self):
        pass

    def close(self):
        pass
