import unittest
import struct
from storage import storage
class MyTestCase(unittest.TestCase):
    def test_storage_write(self):
        s = storage()

        s.active_file.seek(0)
        bytes = s.active_file.read(4)

        res = struct.unpack(">I", bytes)
        print (type(res))
        print (res)

    def test_bytes(self):
        b = struct.pack(">I", 1024)
        data  = struct.unpack(">I", b)
        print (data)

        self.assertEqual(data[0] == 1024, True)

if __name__ == '__main__':
    unittest.main()
