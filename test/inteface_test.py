import unittest


from interface.interface import Bencaskhandler

class MyTestCase(unittest.TestCase):
    def test_batch_set(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)

        for i in range(2000):
            handler.set("hello" + str(i), "world" + str(i))

        for i in range(2000):
            key = "hello" + str(i)
            val = handler.get(key)
            self.assertEqual(val == ("world" + str(i)), True)

    def test_set_and_override(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)
        key = "hello"
        final_val = "world" + str(1999)
        for i in range(2000):
            handler.set(key , "world" + str(i))

        self.assertEqual(handler.get(key) == final_val, True)

    def test_set_and_delete(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)

        for i in range(2000):
            handler.set("hello" + str(i), "world" + str(i))

        for i in range(2000):
            key = "hello" + str(i)
            val = handler.get(key)
            self.assertEqual(val == ("world" + str(i)), True)

        for i in range(2000):
            key = "hello" + str(i)
            handler.delete(key)
            self.assertEqual(handler.get(key) == None, True)


    def test_get(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)
        for i in range(2000):
            key = "hello" + str(i)
            val = handler.get(key)

            self.assertEqual(val == ("world" + str(i)), True)

    ## todo rename
    def test_compact(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)

        key = "hello"
        value = "world"
        newvalue = value + "99"
        for i in range(100):
            new_val = value + str(i)
            handler.set(key, new_val)

        handler.merge()

        res = handler.get("hello")
        print(res)
        self.assertEqual(res == newvalue, True)

if __name__ == '__main__':
    unittest.main()
