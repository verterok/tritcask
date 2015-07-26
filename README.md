# Tritcask

You can never have enough key value stores that are:
* 100% python
* based on [bitcask](https://github.com/basho/bitcask)
* very fast (3x to 100x vs SQLite)
* crash friendly
* tested on millions of desktops for the Ubuntu One file sync client

## Sample usage

Create a database

```
import tritcask
db = tritcask.Tritcask("/tmp/tcask")
```

Put and get and kill some database

```
>>> db.put(0, "hello", "data")
>>> db.keys()
[(0, 'hello')]
>>> db.get(0, "hello")
'data'
>>> db.delete(0, 'hello')
>>> db.keys()
[]
```

And it's fast:
```
In [1]: time for i in xrange(10000): db.put(0, str(i), str(i))
CPU times: user 124 ms, sys: 52 ms, total: 176 ms
Wall time: 175 ms

```
