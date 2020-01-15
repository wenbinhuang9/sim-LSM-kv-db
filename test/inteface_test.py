import unittest


from  interface.interface import Bencaskhandler
class MyTestCase(unittest.TestCase):
    def test_service(self):
        directory_name = "/Users/ben/Wenbin_GitHub/Bencask/file"
        handler = Bencaskhandler(directory_name)

        handler.set("wenbinhuang", "liu")

        res = handler.get("wenbinhuang")
        print (res)
        self.assertEqual(res == "liu", True)

        handler.delete("wenbinhuang")

        res = handler.get("wenbinhuang")
        print (res)
        self.assertEqual(res == None, True)



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
