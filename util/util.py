
import time

def create_segment_file_name():
    timestamp = int(round(time.time() * 1000))
    return "segment" + str(timestamp)

