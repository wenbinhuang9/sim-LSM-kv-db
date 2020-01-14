import unittest


from index import index
class MyTestCase(unittest.TestCase):
    def test_index(self):
        idx = index()

if __name__ == '__main__':
    unittest.main()
