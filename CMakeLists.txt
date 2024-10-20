cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(DDCKV LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

find_package(Boost REQUIRED)
find_package(OpenMP REQUIRED)

include_directories(src)
include_directories("/usr/include")

link_directories("/usr/lib/x86_64-linux-gnu")

find_library(ISAL_LIBRARY NAMES isal PATHS "/usr/lib")
find_library(LZ4_LIBRARY NAMES lz4 PATHS "/usr/lib")

option (ENABLE_ADDR_CACHE "+ Addr Cache" ON)
option (ENABLE_BOTH_CACHE "+ Both Cache" ON)
option (ENABLE_ASYNC_INDEX "+ Async Index" ON)

if(ENABLE_BOTH_CACHE)
    add_definitions(-DENABLE_BOTH_CACHE)
else()
    remove_definitions(-DENABLE_BOTH_CACHE)
endif()

if(ENABLE_ADDR_CACHE)
    add_definitions(-DENABLE_ADDR_CACHE)
else()
    remove_definitions(-DENABLE_ADDR_CACHE)
endif()

if(ENABLE_ASYNC_INDEX)
    add_definitions(-DENABLE_ASYNC_INDEX)
else()
    remove_definitions(-DENABLE_ASYNC_INDEX)
endif()

option (ENABLE_INDEX_SIZE_2048 "Index Size 2GB" OFF)
option (ENABLE_INDEX_SIZE_1024 "Index Size 1GB" OFF)
option (ENABLE_INDEX_SIZE_512 "Index Size 512MB" OFF)
option (ENABLE_INDEX_SIZE_256 "Index Size 256MB" OFF)
option (ENABLE_INDEX_SIZE_128 "Index Size 128MB" OFF)

remove_definitions(-DENABLE_INDEX_SIZE_2048)
remove_definitions(-DENABLE_INDEX_SIZE_1024)
remove_definitions(-DENABLE_INDEX_SIZE_512)
remove_definitions(-DENABLE_INDEX_SIZE_256)
remove_definitions(-DENABLE_INDEX_SIZE_128)

if(ENABLE_INDEX_SIZE_2048)
    add_definitions(-DENABLE_INDEX_SIZE_2048)
elseif(ENABLE_INDEX_SIZE_1024)
    add_definitions(-DENABLE_INDEX_SIZE_1024)
elseif(ENABLE_INDEX_SIZE_512)
    add_definitions(-DENABLE_INDEX_SIZE_512)
elseif(ENABLE_INDEX_SIZE_256)
    add_definitions(-DENABLE_INDEX_SIZE_256)
elseif(ENABLE_INDEX_SIZE_128)
    add_definitions(-DENABLE_INDEX_SIZE_128)
endif()

option (ENABLE_SEND_SIMPLE "Simple Checkpointing" OFF)
option (ENABLE_RPC_BLOCK "Rpc Recv Blocking Way" OFF)
option (ENABLE_RS_CODE "Using RS-Code for Erasure Coding" OFF)

if(ENABLE_SEND_SIMPLE)
    add_definitions(-DENABLE_SEND_SIMPLE)
else()
    remove_definitions(-DENABLE_SEND_SIMPLE)
endif()


if(ENABLE_RPC_BLOCK)
    add_definitions(-DENABLE_RPC_BLOCK)
else()
    remove_definitions(-DENABLE_RPC_BLOCK)
endif()

if(ENABLE_RS_CODE)
    add_definitions(-DENABLE_RS_CODE)
else()
    remove_definitions(-DENABLE_RS_CODE)
endif()

add_subdirectory(src)
add_subdirectory(major-test)
add_subdirectory(minor-test)