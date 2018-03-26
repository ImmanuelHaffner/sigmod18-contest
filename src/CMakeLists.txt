SET(SIG18_SOURCES
    main.cpp
    Analysis.cpp
    Catalog.cpp
    QueryDescription.cpp
    Relation.cpp
    Scheduler.cpp
    StringPool.cpp
    )

add_executable(main ${SIG18_SOURCES})
add_executable(harness harness.cpp)
