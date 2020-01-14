

## implement k,v (no encoding)
## getting the last seek pos?
## only written by the single thread
class storage():
    def __init__(self):
        self.path = "../file/active"
        self.old_file_list = []
        self.active_file = open(self.path, 'r+')
        self.last_pos = 0

    def write(self, data):
        self.active_file.write(data)
        self.active_file.flush()
        old_last_pos = self.last_pos
        self.last_pos += len(data)

        return old_last_pos


    def read(self, offset, len):
        self.active_file.seek(offset)

        return self.active_file.read(len)









