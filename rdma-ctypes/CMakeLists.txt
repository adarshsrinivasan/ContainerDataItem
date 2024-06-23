cmake_minimum_required (VERSION 2.6)

project (rdma-ctypes)

set(PROJECT_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/libs)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BUILD_TYPE Debug) # or debug if you need to use gdb on it.

set(CMAKE_CXX_COMPILER g++)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Ofast -ffast-math -funroll-loops -march=native")
#add_compile_options("-std=c++17")
set(CMAKE_CXX_FLAGS "-O0  -ggdb")

set(IP_ADDRESS "10.10.1.1")
set(PORT 12345)

set(CLIENT_SCRIPT ${CMAKE_SOURCE_DIR}/tests/client.sh)

# Define a custom target to run the script
add_custom_target(run-server
        COMMAND /bin/bash -c "python rdma_py/server.py -l ${IP_ADDRESS} -p ${PORT}"
        COMMENT "Starting the test server..."
)

# Define a custom target to run the script
add_custom_target(run-client
        COMMAND /bin/bash ${CLIENT_SCRIPT}
        COMMENT "Starting the test client..."
)

find_library(IBVERBS_LIBRARY ibverbs HINTS /home/atr/local/lib)
find_library(RDMACM_LIBRARY rdmacm HINTS /home/atr/local/lib)

link_libraries(pthread ${IBVERBS_LIBRARY} ${RDMACM_LIBRARY} ${HIREDIS_LIBRARY})

include_directories("${PROJECT_SOURCE_DIR}" "/home/atr/local/include/")

add_executable(server ${PROJECT_SOURCE_DIR}/utils.c ${PROJECT_SOURCE_DIR}/structs.h ${PROJECT_SOURCE_DIR}/server.c)
add_executable(client ${PROJECT_SOURCE_DIR}/utils.c ${PROJECT_SOURCE_DIR}/structs.h ${PROJECT_SOURCE_DIR}/client.c)

add_library(rdma_server_lib SHARED ${PROJECT_SOURCE_DIR}/utils.c ${PROJECT_SOURCE_DIR}/structs.h ${PROJECT_SOURCE_DIR}/server.c)
add_library(rdma_client_lib SHARED ${PROJECT_SOURCE_DIR}/utils.c ${PROJECT_SOURCE_DIR}/structs.h ${PROJECT_SOURCE_DIR}/client.c)
