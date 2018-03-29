cmake_minimum_required(VERSION 2.6)
project("Sig18" C CXX)

find_package(Threads REQUIRED)

SET(EXECUTABLE_OUTPUT_PATH  "${PROJECT_BINARY_DIR}/bin")
SET(LIBRARY_OUTPUT_PATH     "${PROJECT_BINARY_DIR}/lib")

# Definitions
execute_process(COMMAND getconf LEVEL1_DCACHE_LINESIZE
    OUTPUT_VARIABLE LEVEL1_DCACHE_LINESIZE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND getconf PAGESIZE
    OUTPUT_VARIABLE PAGESIZE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
add_definitions("-DLEVEL1_DCACHE_LINESIZE=${LEVEL1_DCACHE_LINESIZE} -DPAGESIZE=${PAGESIZE}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_COMPILE_FLAGS     "-W -Wall -pedantic")
set(CMAKE_C_FLAGS           "${CMAKE_C_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} ${CMAKE_COMPILE_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")

add_custom_target(check COMMAND ctest --output-on-failure)

include_directories(src)
add_subdirectory(src)
add_subdirectory(test)
