SET(SIG18_SOURCES
    main.cpp
    Catalog.cpp
    QueryDescription.cpp
    QueryExecutor.cpp
    Relation.cpp
    Scheduler.cpp
    )

add_executable(main ${SIG18_SOURCES})
target_link_libraries(main ${CMAKE_THREAD_LIBS_INIT})
add_executable(harness harness.cpp)
