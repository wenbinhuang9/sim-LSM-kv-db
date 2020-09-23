# Simple key value db
This is a simpel key value databse system with the following advantages 

- low latency per item read and write
- high throughput for writing, ability to handle datasets larger than RAM without degradation 
- fast crash recovery.


## how to use 
```python
dbFile = "~/hello_word"
db = SimKV(dbFile)

key = "hello"
val = "world"
db.set(key, val)
db.get(key)
```

## reference 
 

BitCask: https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html

