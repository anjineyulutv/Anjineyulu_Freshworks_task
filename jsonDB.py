import sys
import os
import signal
import json
import time
from threading import Thread

KB = 1024      # Definition of KB
MB = KB * 1024 # Definition of MB
GB = MB * 1024 # Definition of GB

MAX_FILE_SIZE = 1 * GB
LOCK_SUFFIX = '.lock'

class JsonDB(object):
    def __init__(self, location = './data.db'):
        self.acquired_file_lock = False

        self.load(location)
        self.shutdown = None

        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler():
        if self.shutdown is not None:
            print('Database write detected... Proceding with graceful shutdown')
            self.shutdown.join()
        sys.exit(0)

    def resource_locking(self):
        if os.path.exists(self.location + LOCK_SUFFIX):
            raise InterruptedError('Resource Already active. If you think this is a error try deleting the lock file')
        
        with open(self.location + LOCK_SUFFIX, 'w') as fp: # create lockfile
            fp.write('lock')
            self.acquired_file_lock = True

    def load(self, location):
        location = os.path.expanduser(location)
        self.location = location
        if os.path.exists(location):
            self.resource_locking()
            try: 
                self.db = json.load(open(self.location, 'rt'))
                self.db_size = os.stat(location).st_size
            except ValueError:
                if os.stat(location).st_size == 0: 
                    self.db = {}
                else:
                    raise EnvironmentError(f"Unknown error with opening db at {location}")
        else:
            self.db = {}
            self.resource_locking()
            self.db_size = 0
        return True

    def write(self):
        with open(self.location, 'wt') as fp:
            self.shutdown = Thread(
                target=json.dump,
                args=(self.db, fp))
            self.shutdown.start()
            self.shutdown.join()
            self.db_size = os.stat(self.location).st_size
        

    def __create(self, key, value, ttl):
        if not isinstance(ttl, int):
            raise TypeError('TTL should be a integer')

        if not isinstance(key, str):
            raise TypeError('Key should be a string')

        if key in self.db:
            raise KeyError('Key already Present')
        
        if not isinstance(value, dict):
            raise TypeError('Value should be a Python Dict or JSON Object')

        if self.db_size >= MAX_FILE_SIZE:
            raise MemoryError(f'File Size exceeds {MAX_FILE_SIZE} GB. write failed')


    def create(self, key, value, ttl = -1):
        self.__create(key, value, ttl)
        
        expiry =  time.time() + ttl
        if ttl == -1:
            expiry = -1 
        
        self.db[key] = [json.dumps(value), expiry]
        self.write()
            

    def read(self, key):
        try:
            record = self.db[key]
            value = json.loads(record[0])
            if record[1] != -1 and record[1] <= time.time():
                self.__delete(key)
                raise KeyError
            return value
        except KeyError:
            raise KeyError('Key is not present')


    def __delete(self, key):
        del self.db[key]
        self.write()

    def delete(self, key):
        if not key in self.db: 
            raise KeyError('Key is not present')
        value = self.read(key)
        self.__delete(key)
        return value
    
    def destroy(self):
        os.remove(self.location)
        os.remove(self.location + LOCK_SUFFIX)
    
    def __del__(self):
        if self.acquired_file_lock:
            try:
                remove(self.location + LOCK_SUFFIX)
            except:
                pass


    

    

  

   