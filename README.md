# Go Count Me

Go Count Me is a KMin Values database with a leveldb backend.  What this allows
you to do is store a massive amount of very large sets and fetch values for
the following operations with relatively low error:

* Cardinality
* Intersection
* Union 
* Jaccard Index

## HTTP Interface

An HTTP server gets spun up if the `gocountme` binary is run.  The server has
the following endpoints:

/get : `key` parameter designating which set to return

/delete : `key` parameter designating which set to delete

/add : `key` and `value` parameters saying which set to add the given value to.
The value is hashed with a `murmur3` hasing function.

/addhash : `key` and `hash` parameters saying which set to add the given hash to.
The hash must be a valid uint64 type.

/cardinality : `key` parameter designating which set to calculate the
cardinality of

/jaccard : two `key` parameter designating which sets to calculate the jaccard
index between.

/correlation : two or more `key` parameters to calculate the correlation matrix
of.  The return value is a list of dictionaries of the form `{"keys" : ["key1",
"key2"], "jaccard" : 0.02}`

/query : `q` which is a url encoded json specifying the desired query (more
about queries below)

## Queries

In order to do efficient lookups of complex set operations, we support a
limited query language that is based on recursively defined json objects.  The
basic json object looks like,

``` 
{ "method" : "...", "keys" : ["...", "...", ], "set" : [ {...}, {...} ] }
``` 

Here, `keys` and `set` are mutually exclusive representations of data and
`method` is what operation to perform on them.  The `keys` list is a list of
direct keys into the database while the `set` list is a set of similarly
defined dictionaries.  As a result, we can calculate a complex quantity such as:

```
Jaccard( key1 u key2, key8 n key3 )
```

with the following query,

```
{
    "method" : "jaccard",
    "set" : [
        {
            "method" : "union",
            "keys" : ["key1", "key2"],
        },
        {
            "method" : "intersection",
            "keys" : ["key8", "key3"],
        },

    ]
}
```

or the operation:

```
Card( (key1 u key2 u key3) n key5)
```

with the query,

```
{
    "method" : "cardinality",
    "set" : [
        {
            "method" : "intersection",
            "set" : [
                {
                    "method" : "union",
                    "keys" : ["key1", "key2", "key3"]
                },
                {
                    "method" : "get",
                    "keys" : ["key5"]
                },
            ]
        },
    ]
}
```

If a key doesn't exist, then it is treated as an empty set.

## Example use

First, we compile gocountme,

```
$ git clone https://github.com/mynameisfiber/gocountme.git
$ cd gocountme
$ go build
```

and then run an instance,

```
$ mkdir db
$ ./gocountme --db="./db/"
```

Now, let's load up some test data into the database,

```
$ ( 
    for key in key1 key2 key3; do 
        i=0; 
        echo -e "\n$key" 1>&2; 
        while [ $i -lt 10000 ]; do 
            echo -n "." 1>&2; 
            echo "http://localhost:8080/add?key=${key}&value=${RANDOM}"; 
            i=$(( $i + 1 )); 
        done; 
    done; 
) | xargs -n 1 -P 8 -I{} curl -s -w ' ' {} > /dev/null
```

This will add 10000 randomly generated values to the keys `key1`, `key2` and
`key3`.  Now we can start making queries!  For example, to verify how many
entries each key has we can make a call to the cardinality endpoint,

```
$ curl -s "http://localhost:8080/cardinality?key=key1"
{"status_code":200,"status_txt":"","data":9216.455393367254}

$ curl -s "http://localhost:8080/cardinality?key=key2"
{"status_code":200,"status_txt":"","data":9306.02816195663}

$ curl -s "http://localhost:8080/cardinality?key=key3"
{"status_code":200,"status_txt":"","data":9019.716257930126}
```

Which shows a ~7% relative error which is quite good since we are only storing
1024 integers per set instead of the full 10000 items.  We can also issue more
complicated queries, for example getting the jaccard index between all
combinations of the three keys,

```
$ curl -s "http://localhost:8080/correlation?key=key1&key=key2&key3"
{
  "data": [
    {
      "jaccard": 0.15625,
      "keys": [
        "key1",
        "key2"
      ]
    },
    {
      "jaccard": 0.169921875,
      "keys": [
        "key1",
        "key3"
      ]
    },
    {
      "jaccard": 0.1552734375,
      "keys": [
        "key2",
        "key3"
      ]
    }
  ],
  "status_txt": "",
  "status_code": 200
}
```

Furthermore, we can issue queries to find out more complicated questions.  For
example, to find out how many entries in keys `key1` and `key2` are the same we
can issue the command:

```
$ curl -G --data-urlencode 'q={"method":"cardinality_intersection", "keys":["key1", "key2"]}' "http://localhost:8080/query"
{"status_code":200,"status_txt":"","data":{"key":"||key1 n key2||","set":null,"result":2445.266023344539}}
```
