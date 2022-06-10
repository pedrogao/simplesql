include_directories(./)

add_executable(simple_db simple_db.cpp)
add_executable(simple_sql db.cpp tuple_util.cpp simple_sql.cpp)
add_executable(spsql_d db.cpp tuple_util.cpp spsql_d.cpp)
add_executable(spsql_client spsql_client.cpp)
add_executable(simple_hd simple_hd.cpp)

target_link_libraries(simple_db bustub_shared)
target_link_libraries(simple_sql bustub_shared)
target_link_libraries(spsql_d bustub_shared)
target_link_libraries(spsql_client bustub_shared)
target_link_libraries(simple_hd z)
target_link_libraries(simple_hd bustub_shared)