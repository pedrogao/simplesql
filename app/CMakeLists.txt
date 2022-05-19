include_directories(./)

add_executable(simple_db simple_db.cpp)
add_executable(simple_sql simple_sql.cpp)
add_executable(spsql_d spsql_d.cpp)
add_executable(spsql_client spsql_client.cpp)

target_link_libraries(simple_db bustub_shared)
target_link_libraries(simple_sql bustub_shared)
target_link_libraries(spsql_d bustub_shared)
target_link_libraries(spsql_client bustub_shared)