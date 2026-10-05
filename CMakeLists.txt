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
find_or_install_package(rdkafka librdkafka-dev librdkafka)
find_or_fetch_package(modern-cpp-kafka-api GIT_REPOSITORY https://github.com/morganstanley/modern-cpp-kafka.git)

###########
# Objects #
###########
set(SOURCE_OBJECTS include/kafka.h)
set(SOURCE_LIBRARIES)
set(SOURCE_LINK_LIBRARIES)

add_sc_object(handler)
add_sc_object(kafka LINK_LIBRARIES modern-cpp-kafka-api PUBLIC_LINK_LIBRARIES modern-cpp-kafka-api)
add_sc_object(protobuf)

######################
# Exported Libraries #
######################
add_sc_libraries(DESCRIPTION "simply-cpp-kafka")

################
# Installation #
################
install_sc_module()

#########
# Tests #
#########
if (SC_KAFKA_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests sc-kafka-test)
endif ()

############
# Binaries #
############

#add_executable(sc-demo-db apps/sc-demo-db.cpp)
#target_link_libraries(sc-demo-db PRIVATE sc::sc-db-shared)
#install(TARGETS sc-demo-db RUNTIME DESTINATION bin COMPONENT runtime)

#############
# Packaging #
#############
package_sc_module()