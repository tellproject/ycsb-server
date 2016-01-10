project(tpcc)

find_package(Boost REQUIRED COMPONENTS system)
find_package(TellStore REQUIRED)
find_package(Crossbow REQUIRED)
find_package(Threads)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

set(SRC main.cpp)

add_executable(ycsb-server ${SRC})
target_include_directories(ycsb-server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(ycsb-server PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(ycsb-server PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(ycsb-server PUBLIC ${Boost_LIBRARIES})
target_link_libraries(ycsb-server PUBLIC crossbow_logger crossbow_allocator tellstore-client)

