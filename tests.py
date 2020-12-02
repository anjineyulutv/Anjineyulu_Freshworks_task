import os
import unittest
from jsonDB import JsonDB
from threading import Thread

location  = './init.db'
class DBTestCases(unittest.TestCase):
    def setUp(self):
        self.db = JsonDB(location)
        self.db.create('k', {'a': 'test'})

    def tearDown(self):
        try:
            self.db.destroy()
        except:
            pass

    def test_dbcreated(self):
        self.assertTrue(os.path.exists(location))


    def test_negative_doubleDB_ReadForbid(self):
        with self.assertRaises(InterruptedError):
            db2 = JsonDB(location)

    def test_read(self):
        self.assertEqual('test', self.db.read('k')['a'])

    def test_delete(self):
        self.db.delete('k')

        with self.assertRaises(KeyError):
            self.db.read('k')
    
    def test_inserts_with_ttl_expired(self):
        self.db.create('time', {}, -10) # deliberately expired

        with self.assertRaises(KeyError):
            self.db.read('time')

    def test_deletes_with_ttl_expired(self):
        self.db.create('time', {}, -10) # deliberately expired

        with self.assertRaises(KeyError):
            self.db.delete('time')

    def test_inserts_with_ttl(self):
        self.db.create('time', {}, 10) 

        self.assertIsNotNone(self.db.read('time'))

    def test_inserts_parallel(self):
        def inserte(val):
            try: 
                self.db.create('time', {}, val) 
            except:
                pass
        p1 = Thread(target=inserte, args=(1,))
        p2 = Thread(target=inserte, args=(2,))

        p1.start()
        p2.start()

        p1.join()
        p2.join()

        self.assertIsNotNone(self.db.read('time'))

    def test_negative_doubleInsert(self):
        with self.assertRaises(KeyError):
            self.db.create('k', {})
    
    def test_negative_NonExistentKey(self):
        with self.assertRaises(KeyError):
            self.db.read('x')

    def test_negative_NumericKey(self):
        with self.assertRaises(TypeError):
            self.db.create(1, {})

    def test_negative_NotADict(self):
        with self.assertRaises(TypeError):
            self.db.create('cx', 'string')

    def test_negative_StringTTL(self):
        with self.assertRaises(TypeError):
            self.db.create('a', {}, 'aaa')
        

if __name__ == '__main__':
    unittest.main()