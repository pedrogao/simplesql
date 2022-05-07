# simplesql

> simple sql database implement

## pre-requirements

sql-parser:

  ```sh
  cd third_party && git clone https://github.com/hyrise/sql-parser
  cd sql-parser && make 
  cp libsqlparser.so /usr/local/lib
  ```

workflow:

   ```sh
   cd third_party && git clone https://github.com/sogou/workflow
   cd workflow && make 
   cp _lib/libworkflow.dylib /usr/local/lib
   ```

## startup

```sh
mkdir build && cd build
cmake ..
make -j4 simple_db
./bin/simple_db
```

## thanks

[BusTub](https://github.com/cmu-db/bustub.git)