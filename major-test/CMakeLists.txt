# client_perf
add_executable(client_perf client_perf.cc)

target_compile_options(client_perf
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(client_perf
    libddckv
    pthread
    ibverbs
)

# client_gc_update
add_executable(client_gc_update client_gc_update.cc)

target_compile_options(client_gc_update
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(client_gc_update
    libddckv
    pthread
    ibverbs
)

# client_degraded_search
add_executable(client_degraded_search client_degraded_search.cc)

target_compile_options(client_degraded_search
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(client_degraded_search
    libddckv
    pthread
    ibverbs
)

# client_consume
add_executable(client_consume client_consume.cc)

target_compile_options(client_consume
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(client_consume
    libddckv
    pthread
    ibverbs
)

# server
add_executable(server server.cc)

target_compile_options(server
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(server
    libddckv
    pthread
    ibverbs
)

# server_recover
add_executable(server_recover server_recover.cc)

target_compile_options(server_recover
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_link_libraries(server_recover
    libddckv
    pthread
    ibverbs
)
