cmake_minimum_required(VERSION 2.8)

include_directories(thirdparty)

set(CMAKE_C_FLAGS "-std=c99")

add_executable(bredis bredis.c event_loop.c ae/ae.c)
