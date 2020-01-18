

tstamp_byte_len = 0

class record():

    def __init__(self, file_name, val_offset, val_len, tstamp):
        self.file_name = file_name
        self.val_offset = val_offset
        self.val_len = val_len
        self.tstamp = tstamp

