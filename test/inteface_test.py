import threading
import unittest

from interface.interface import SimKV
from  storage.storage import get_segment_file_list
import  random

class MyTestCase(unittest.TestCase):
    def test_batch_set(self):
        directory_name = "/Users/ben/Wenbin_GitHub/simkv/file"
        handler = SimKV(directory_name)

        for i in range(2000):
            handler.set("hello" + str(i), "world" + str(i))

        for i in range(2000):
            key = "hello" + str(i)
            val = handler.get(key)
            self.assertEqual(val == ("world" + str(i)), True)

    def test_set_and_override(self):
        directory_name = "/Users/ben/Wenbin_GitHub/simkv/file"
        handler = SimKV(directory_name)
        key = "hello"
        final_val = "world" + str(1999)
        for i in range(2000):
            handler.set(key , "world" + str(i))

        self.assertEqual(handler.get(key) == final_val, True)

    def test_set_and_delete(self):
        directory_name = "/Users/ben/Wenbin_GitHub/simkv/file"
        handler = SimKV(directory_name)

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


    def test_multi_read_in_concurrency(self):
        directory_name = "/Users/ben/Wenbin_GitHub/simkv/file"
        handler = SimKV(directory_name)

        basic_key = "reader"

        def task(handler, unit_test_handler):
            for i in range(100):
                ran = random.randrange(100)
                key = "reader" + str(ran)
                val = handler.get(key)
                unit_test_handler.assertEqual(val == key, True)
                if key != val:
                    print (key + ":" + val)
        for i in range(100):
            handler.set(basic_key + str(i), basic_key + str(i))

        for i in range(100):
            self.assertEqual(handler.get(basic_key + str(i)) == basic_key + str(i), True)

        thread1 = threading.Thread(target=task, args=(handler, self))
        thread2 = threading.Thread(target=task, args=(handler,self))

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        print("Done")


    def test_get_segment_file_list(self):
        dir_list = get_segment_file_list("./file")
        correct_ans = ['segment100', 'segment102_merge', 'segment200']

        self.assertEqual(all([correct_ans[i] == dir_list[i] for i in range(len(dir_list))]), True)


    def test_merge(self):
        dir = "./dir_for_merge_test"
        handler = SimKV(dir)

        key = "hello"
        len = 5000
        for i in range(len):
            handler.set(key, "world" + str(i))

        val = handler.get(key)
        self.assertEqual(val ==  "world" + str(len - 1), True)

        handler.merge()

        val = handler.get(key)
        self.assertEqual(val == "world" + str(len - 1), True)

if __name__ == '__main__':
    unittest.main()
