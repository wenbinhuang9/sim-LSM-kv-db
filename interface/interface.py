## a state handler
from core.service import Service

class SimKV():
    def __init__(self, directory_name):
        self.directory_name = directory_name
        self.service = Service(directory_name)

    def open(self, directory_name):
        handler = SimKV(directory_name)
        return handler

    def set(self, key, value):
        return self.service.set(key, value)
    def get(self, key):
        return self.service.get(key)

    def delete(self, key):
        return self.service.delete(key)

    def merge(self):
        return self.service.merge()

    def close(self):
        self.service.close()
