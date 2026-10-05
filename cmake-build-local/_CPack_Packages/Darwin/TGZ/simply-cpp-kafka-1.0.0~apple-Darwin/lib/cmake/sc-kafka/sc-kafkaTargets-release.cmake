#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "sc::sc-kafka-shared" for configuration "Release"
set_property(TARGET sc::sc-kafka-shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(sc::sc-kafka-shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libsc-kafka-shared.1.0.0.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libsc-kafka-shared.1.dylib"
  )

list(APPEND _cmake_import_check_targets sc::sc-kafka-shared )
list(APPEND _cmake_import_check_files_for_sc::sc-kafka-shared "${_IMPORT_PREFIX}/lib/libsc-kafka-shared.1.0.0.dylib" )

# Import target "sc::sc-kafka" for configuration "Release"
set_property(TARGET sc::sc-kafka APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(sc::sc-kafka PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libsc-kafka.a"
  )

list(APPEND _cmake_import_check_targets sc::sc-kafka )
list(APPEND _cmake_import_check_files_for_sc::sc-kafka "${_IMPORT_PREFIX}/lib/libsc-kafka.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
