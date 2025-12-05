# Tritcask

You can never have enough local disk-based key value stores that are:
* 100% python
* based on [bitcask](https://github.com/basho/bitcask)
* very fast (3x to 100x vs SQLite)
* crash friendly
* tested on millions of desktops for the Ubuntu One file sync client

## Using Cask

`Cask` is a dict-like interface backed by a Tritcask store. It provides a familiar dictionary API for storing and retrieving Python primitive types. You can create a cask from a path using the `from_path` class method.

### Example

```python
from tritcask import Cask

# Create a cask database at the given path
cask = Cask.from_path("/tmp/shelfdb")

# You can use any combination of primitive values in the key and the value
cask["foo"] = {"hello": "world"}
cask[[1, 2]] = [1, 2, 3]

# Retrieve and use them
print(cask["foo"])  # Output: {'hello': 'world'}
print(cask[[1, 2]])  # Output: [1, 2, 3]

# Dict-like methods
print(list(cask.keys()))      # Output: ['foo', [1, 2]]
print("foo" in cask)          # Output: True

# Delete a key
del cask["foo"]
print("foo" in cask)          # Output: False
```


## Low level Tritcask

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
