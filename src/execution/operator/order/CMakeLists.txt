add_library_unity(duckdb_operator_order OBJECT physical_order.cpp
                  physical_streaming_top_n_per_group.cpp physical_top_n.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_operator_order>
    PARENT_SCOPE)
