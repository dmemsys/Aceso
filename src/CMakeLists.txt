list(APPEND source_ddckv 
    NetworkManager.cc
    Rdma.cc
    Server.cc
    KvUtils.cc
    Hashtable.cc
    Client.cc
    AddrCache.cc
    )

add_library(libddckv STATIC ${source_ddckv})

target_compile_options(
    libddckv
    PRIVATE
    "-O3"
    "-g"
    "-fopenmp"
    ${OpenMP_CXX_FLAGS}
    "-mavx"
)

target_link_libraries(
    libddckv 
    ${Boost_LIBRARIES} 
    boost_context 
    boost_fiber 
    boost_system 
    boost_thread
    boost_coroutine
    tbb 
    "-fopenmp"
    ${OpenMP_CXX_FLAGS} 
    numa
    memcached
    cityhash
    ${ISAL_LIBRARY}
    ${LZ4_LIBRARY}
)

