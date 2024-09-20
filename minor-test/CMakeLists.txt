# dmec_test_memcpy
add_executable(dmec_test_memcpy dmec_test_memcpy.cc)

target_compile_options(dmec_test_memcpy
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
    "-O3"
)

target_link_libraries(dmec_test_memcpy
    pthread
    ibverbs
)