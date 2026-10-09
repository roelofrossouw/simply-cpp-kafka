cmake_minimum_required(VERSION 3.22)
set(SC_MODULE kafka)
project(simply-cpp-${SC_MODULE} LANGUAGES CXX)
include(cmake/sc_bootstrap.cmake)

# Options:
option(SC_KAFKA_BUILD_TESTS "Build simply-cpp-kafka tests" ${PROJECT_IS_TOP_LEVEL})

get_sc_version()
message(STATUS "SimplyCPP ${SC_MODULE} Version: ${SC_VERSION}")

################
# Dependencies #
################
config_or_install_package(rdkafka librdkafka-dev librdkafka)
find_or_fetch_package(modern-cpp-kafka-api
        GIT_REPOSITORY https://github.com/morganstanley/modern-cpp-kafka.git
        GIT_TAG v2023.01.05)
# kafka.h takes its brokers as an sc::ip_endpoints; the demo also uses sc::getenv() and sc::explode().
find_or_install_package(sc-core simply-cpp-dev simply-cpp 1.5.7)
find_library(SC_KAFKA_RDKAFKA_LIBRARY NAMES rdkafka HINTS ${rdkafka_LIBRARY_DIRS} REQUIRED)

###########
# Objects #
###########
set(SOURCE_OBJECTS include/kafka.h)
set(SOURCE_LIBRARIES)
set(SOURCE_LINK_LIBRARIES)

add_sc_object(kafka LINK_LIBRARIES modern-cpp-kafka-api
        PUBLIC_LINK_LIBRARIES sc::sc-core ${SC_KAFKA_RDKAFKA_LIBRARY})

######################
# Exported Libraries #
######################
add_sc_libraries(DESCRIPTION "simply-cpp-kafka")

################
# Installation #
################
install_sc_module(PATH_VARS CMAKE_INSTALL_INCLUDEDIR)
install(FILES cmake/SimplyCppFunctions.cmake cmake/sc_bootstrap.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/sc-kafka
        COMPONENT development)
install(FILES tests/sc_test.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        COMPONENT development)

#########
# Tests #
#########
if (SC_KAFKA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests sc-kafka-test)
endif ()

############
# Examples #
############
add_sc_example(sc-kafka-demo)

#############
# Packaging #
#############
package_sc_module(DEPENDS librdkafka++1 DEV_DEPENDS "simply-cpp-dev (>= 1.5.5)")