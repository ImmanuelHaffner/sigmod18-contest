SET(SIG18_SOURCES
    main.cpp
    Relation.cpp
    Catalog.cpp
    StringPool.cpp
    )

add_executable(main ${SIG18_SOURCES})
add_executable(harness harness.cpp)
