
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was sc-kafkaConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

# The shared build helpers - get_sc_version(), add_sc_object(), add_sc_test(),
# find_or_install_package() - so every simply-cpp module configures the same way
# instead of keeping its own copy.
include("${CMAKE_CURRENT_LIST_DIR}/SimplyCppFunctions.cmake")

# Where sc_test.h was installed. add_sc_test() puts this on the include path.
set_and_check(SC_TEST_INCLUDE_DIR "${PACKAGE_PREFIX_DIR}/include")

config_or_install_package(rdkafka librdkafka-dev librdkafka)

include("${CMAKE_CURRENT_LIST_DIR}/sc-kafkaTargets.cmake")
