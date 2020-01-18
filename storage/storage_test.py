import unittest
import struct
from storage import Writer
class MyTestCase(unittest.TestCase):
    def test_storage_write(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"

        s = Writer(directory_name)

        ans = s.get_segment_file_list()

        print (ans)

    def test_s(self):
        res = struct.pack(">q", 1579240043194)

        print (len(res))

        print (isinstance(type(1579240043194), long))
if __name__ == '__main__':
    unittest.main()
