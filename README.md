# simplesql

> simple sql database implement

## pre-requirements

sql-parser:

```sh
cd third_party && git clone https://github.com/hyrise/sql-parser
cd sql-parser && make
cp libsqlparser.so /usr/local/lib
```

spdlog:

```sh
cd third_party && git clone https://github.com/gabime/spdlog.git
cd spdlog && mkdir build && cd build
cmake .. && make -j 4
cp libspdlog.a /usr/local/lib
```

workflow:

```sh
cd third_party && git clone https://github.com/sogou/workflow
cd workflow && make
cp _lib/libworkflow.dylib /usr/local/lib
```

## startup

build sql server and run:

```sh
$ mkdir build && cd build
$ cmake ..
$ make -j4 spsql_d

$ ./bin/spsql_d test.db
[2022-06-05 13:08:01.755] [debug] init sql db at test.db
2022-06-05 13:08:01 [Users/pedrogao/projects/cpp/simplesql/src/include/concurrency/lock_manager.h:67:LockManager] INFO  - Cycle detection thread launched
[2022-06-05 13:10:43.436] [info] req body size : 69
[2022-06-05 13:10:43.438] [debug] create table: students
```

post sql query:

```sh
$ curl  -X POST -d 'CREATE TABLE students (name VARCHAR(20), age INTEGER, grade INTEGER);' http://localhost:8888

execute successful!%
```

## thanks

[BusTub](https://github.com/cmu-db/bustub.git)
