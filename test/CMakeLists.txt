add_executable(unittests
    test.cpp
    ${SIG18_SOURCES}
    IndexTest.cpp
    )
#target_link_libraries(dbms_test dbms impl)
add_dependencies(check unittests)
add_test(NAME unittests COMMAND unittests)
