# Description

- Database is exposed as a client usable library (see below for usage)
- Client can open a file only once. lock files are generated to keep it in check.
- Graceful shutdown (in-case of SIGTERM while inserting the insertion will complete before exiting)
- Proper Validations with key as strings, value as JSON or Dictionaries
- Proper Error Messages thrown to client 
- File Size Limited to 1 GB (defined as a constant in jsonDB.py)



# Run Tests

```
python3 tests.py 
```

# Usage 

```python
from jsonDB import JsonDB
location = './database.db'

db = JsonDB(location) # location is optional

db.create('company', {'name': 'Freshworks'})

...

company = db.read('name')

...

db.delete('name')

...

# TTL keys can be given as 
db.create('companyTemp', {'name': 'Freshworks'}, 10) # will be there for 10 more seconds 
```