import unittest


from service import Service
class MyTestCase(unittest.TestCase):
    def test_service(self):
        s = Service()

        s.set("wenbinhuang", "liu")
        print (s.get("wenbinhuang"))
        self.assertEqual(s.get("wenbinhuang") == "liu", True)

if __name__ == '__main__':
    unittest.main()
