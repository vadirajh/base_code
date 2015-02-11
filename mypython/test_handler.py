import unittest
from handler import ResourceHandler as Handler

class BasicTestCase(unittest.TestCase):
    def test_insert_resource(self):
        retval = Handler.insert_item(name="Vadi", 
				     original_price=100, status = 'online')
        print "Returned : %s" %(retval,)
        retval = Handler.insert_item(name="Raj", 
				     original_price=300, status = 'Offline')
        print "Returned : %s" %(retval,)

    def test_get_all_resource(self):
        retval = Handler.get_all_items()
        print "Received %s" %(retval,)

if __name__ == "__main__":
    unittest.main()  # run all tests
