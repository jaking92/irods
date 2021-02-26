cmake_minimum_required(VERSION ${CMAKE_VERSION})
project(unit_tests LANGUAGES C CXX)

set(IRODS_SERVER_TESTS_BUILD NO CACHE BOOL "Build C++ server tests")
set(IRODS_SERVER_TESTS_REPORTING_STYLE "junit" CACHE STRING "The style of output used for C++ server test reporting [console, compact, junit, xml]")
set(IRODS_SERVER_TESTS_REPORT_FILENAME "report.xml" CACHE STRING "The filename of the C++ server test report")
set(IRODS_SERVER_TESTS_ENABLE_ALL NO CACHE BOOL "Enables all C++ server tests")

if (NOT IRODS_SERVER_TESTS_BUILD)
    return()
endif()

# Enable CTest support.
enable_testing()

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

# Update the CMake module path so that the test compilation variables
# can be found.  Prepends the new path to the beginning of the list.
list(INSERT CMAKE_MODULE_PATH 0 ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Include helper functions and other utilities.
include(utils)

if (IRODS_SERVER_TESTS_ENABLE_ALL)
    add_definitions(-DIRODS_ENABLE_ALL_SERVER_TESTS)
endif()

# List of cmake files defined under ./cmake/test_config.
# Each file in the ./cmake/test_config directory defines variables for a specific test.
# New tests should be added to this list.
set(TEST_INCLUDE_LIST test_config/irods_atomic_apply_acl_operations
                      test_config/irods_atomic_apply_metadata_operations
                      test_config/irods_client_connection
                      test_config/irods_connection_pool
                      test_config/irods_data_object_finalize
                      test_config/irods_data_object_modify_info
                      test_config/irods_dns_cache
                      test_config/irods_dstream
                      test_config/irods_filesystem
                      test_config/irods_get_file_descriptor_info
                      test_config/irods_hostname_cache
                      test_config/irods_metadata
                      test_config/irods_parallel_transfer_engine
                      test_config/irods_query_builder
                      test_config/irods_rc_data_obj
                      test_config/irods_replica
                      test_config/irods_replica_access_table
                      test_config/irods_replica_open_and_close
                      test_config/irods_resource_administration
                      test_config/irods_scoped_client_identity
                      test_config/irods_scoped_privileged_client
                      test_config/irods_user_administration
                      test_config/irods_zone_report)

foreach(IRODS_TEST_CONFIG ${TEST_INCLUDE_LIST})
    unset_irods_test_variables()

    include(${IRODS_TEST_CONFIG})
    add_executable(${IRODS_TEST_TARGET} ${IRODS_TEST_SOURCE_FILES})
    target_include_directories(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_INCLUDE_PATH})
    target_link_libraries(${IRODS_TEST_TARGET} PRIVATE ${IRODS_TEST_LINK_LIBRARIES})

    # Make the new test available to CTest.
    add_test(NAME ${IRODS_TEST_TARGET} COMMAND ${IRODS_TEST_TARGET} -r ${IRODS_SERVER_TESTS_REPORTING_STYLE} -o ${IRODS_SERVER_TESTS_REPORT_FILENAME})
endforeach()

