cmake_minimum_required(VERSION 2.8.12)

project(dist-int-comp C CXX)
set(CMAKE_CXX_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-std=gnu99")
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=gnu++11")

add_executable(dist-int-comp-master master.c)
target_compile_definitions(dist-int-comp-master PRIVATE NDEBUG)
target_link_libraries(dist-int-comp-master pthread)

add_executable(dist-int-comp-slave slave.cc)
target_compile_definitions(dist-int-comp-slave PRIVATE _GNU_SOURCE NDEBUG)
target_link_libraries(dist-int-comp-slave m pthread)
