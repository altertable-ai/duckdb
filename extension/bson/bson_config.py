import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/bson/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/bson/bson_extension.cpp', 'extension/bson/bson_common.cpp', 'extension/bson/bson_functions.cpp', 'extension/bson/bson_functions/bson_valid.cpp', 'extension/bson/bson_functions/bson_exists.cpp', 'extension/bson/bson_functions/bson_type.cpp', 'extension/bson/bson_functions/bson_extract.cpp', 'extension/bson/bson_functions/bson_extract_string.cpp', 'extension/bson/bson_functions/json_to_bson.cpp']]
