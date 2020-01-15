import unittest

from core.service import Service
class MyTestCase(unittest.TestCase):
    def test_service(self):
        s = Service()

        s.set("wenbinhuang", "liu")
        res = s.get("wenbinhuang")
        print(res)
        self.assertEqual(res == "liu", True)

        s.delete("wenbinhuang")

        self.assertEqual(s.get("wenbinhuang") == None, True)

if __name__ == '__main__':
    unittest.main()
