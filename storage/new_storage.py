import io
from threading import Lock


class FileHandler():
    def __init__(self, direcotry_name, file_name, format=None):
        self.direcotry_name = direcotry_name
        self.file_name = file_name
        self.lock = Lock()
        self.format = "r+" if format == None else format
        self.handler = open(self.direcotry_name + "/" + self.file_name, self.format)

    def last_pos(self):
        self.lock.acquire()
        self.handler.seek(0, io.SEEK_END)
        last_pos = self.handler.tell()

        self.lock.release()
        return last_pos

    def read(self, offset, len):
        ##todo time out here
        self.lock.acquire()
        self.handler.seek(offset)
        value_bytes = self.handler.read(len)
        self.lock.release()
        return value_bytes

    def write(self, data, flush):
        self.lock.acquire()
        file_handler = self.handler
        file_handler.seek(0, io.SEEK_END)
        last_pos = file_handler.tell()
        file_handler.write(data)

        if flush:
            file_handler.flush()
        self.lock.release()
        return last_pos

    def close(self):
        self.handler.close()
