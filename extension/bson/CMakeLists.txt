cmake_minimum_required(VERSION 2.8.12...3.29)

project(BSONExtension)

include_directories(include ../json/include ../../third_party/yyjson/include)

set(BSON_EXTENSION_FILES
    bson_extension.cpp
    bson_common.cpp
    bson_functions.cpp
    bson_functions/bson_valid.cpp
    bson_functions/bson_exists.cpp
    bson_functions/bson_type.cpp
    bson_functions/bson_extract.cpp
    bson_functions/bson_extract_string.cpp
    bson_functions/json_to_bson.cpp)

build_static_extension(bson ${BSON_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(bson ${PARAMETERS} ${BSON_EXTENSION_FILES})
target_link_libraries(bson_loadable_extension duckdb_yyjson)

install(
  TARGETS bson_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
