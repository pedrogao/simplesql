include_directories(../third_party/sql-parser/src)
include_directories(../third_party/workflow/_include)

add_executable(simple_db simple_db.cpp)
add_executable(simple_sql simple_sql.cpp)
add_executable(simple_srv simple_srv.cpp)

target_link_libraries(simple_db bustub_shared)
target_link_libraries(simple_sql -lsqlparser)
target_link_libraries(simple_srv -lworkflow)