//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bson_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

// Include yyjson for JSON parsing (for JSON to BSON conversion)
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//! BSON type codes from the BSON specification
enum class BSONType : uint8_t {
	DOUBLE = 0x01,
	STRING = 0x02,
	DOCUMENT = 0x03,
	ARRAY = 0x04,
	BINARY = 0x05,
	UNDEFINED = 0x06, // Deprecated
	OBJECT_ID = 0x07,
	BOOLEAN = 0x08,
	DATE_TIME = 0x09,
	NULL_VALUE = 0x0A,
	REGEX = 0x0B,
	DB_POINTER = 0x0C, // Deprecated
	JAVASCRIPT = 0x0D,
	SYMBOL = 0x0E, // Deprecated
	JAVASCRIPT_WITH_SCOPE = 0x0F,
	INT32 = 0x10,
	TIMESTAMP = 0x11,
	INT64 = 0x12,
	DECIMAL128 = 0x13,
	MIN_KEY = 0xFF,
	MAX_KEY = 0x7F
};

//! BSON element: a type code, a key (cstring), and a value
struct BSONElement {
	BSONType type;
	const char *key;
	idx_t key_len;
	const uint8_t *value;
	idx_t value_len;
};

//! Common BSON functionality
struct BSONCommon {
public:
	//! BSON type strings
	static constexpr const char *TYPE_STRING_DOUBLE = "double";
	static constexpr const char *TYPE_STRING_STRING = "string";
	static constexpr const char *TYPE_STRING_DOCUMENT = "document";
	static constexpr const char *TYPE_STRING_ARRAY = "array";
	static constexpr const char *TYPE_STRING_BINARY = "binary";
	static constexpr const char *TYPE_STRING_UNDEFINED = "undefined";
	static constexpr const char *TYPE_STRING_OBJECT_ID = "objectid";
	static constexpr const char *TYPE_STRING_BOOLEAN = "boolean";
	static constexpr const char *TYPE_STRING_DATE_TIME = "datetime";
	static constexpr const char *TYPE_STRING_NULL = "null";
	static constexpr const char *TYPE_STRING_REGEX = "regex";
	static constexpr const char *TYPE_STRING_DB_POINTER = "dbpointer";
	static constexpr const char *TYPE_STRING_JAVASCRIPT = "javascript";
	static constexpr const char *TYPE_STRING_SYMBOL = "symbol";
	static constexpr const char *TYPE_STRING_JAVASCRIPT_WITH_SCOPE = "javascriptwithscope";
	static constexpr const char *TYPE_STRING_INT32 = "int32";
	static constexpr const char *TYPE_STRING_TIMESTAMP = "timestamp";
	static constexpr const char *TYPE_STRING_INT64 = "int64";
	static constexpr const char *TYPE_STRING_DECIMAL128 = "decimal128";
	static constexpr const char *TYPE_STRING_MIN_KEY = "minkey";
	static constexpr const char *TYPE_STRING_MAX_KEY = "maxkey";

	//! Convert BSON type to string
	static inline const char *TypeToString(BSONType type) {
		switch (type) {
		case BSONType::DOUBLE:
			return TYPE_STRING_DOUBLE;
		case BSONType::STRING:
			return TYPE_STRING_STRING;
		case BSONType::DOCUMENT:
			return TYPE_STRING_DOCUMENT;
		case BSONType::ARRAY:
			return TYPE_STRING_ARRAY;
		case BSONType::BINARY:
			return TYPE_STRING_BINARY;
		case BSONType::UNDEFINED:
			return TYPE_STRING_UNDEFINED;
		case BSONType::OBJECT_ID:
			return TYPE_STRING_OBJECT_ID;
		case BSONType::BOOLEAN:
			return TYPE_STRING_BOOLEAN;
		case BSONType::DATE_TIME:
			return TYPE_STRING_DATE_TIME;
		case BSONType::NULL_VALUE:
			return TYPE_STRING_NULL;
		case BSONType::REGEX:
			return TYPE_STRING_REGEX;
		case BSONType::DB_POINTER:
			return TYPE_STRING_DB_POINTER;
		case BSONType::JAVASCRIPT:
			return TYPE_STRING_JAVASCRIPT;
		case BSONType::SYMBOL:
			return TYPE_STRING_SYMBOL;
		case BSONType::JAVASCRIPT_WITH_SCOPE:
			return TYPE_STRING_JAVASCRIPT_WITH_SCOPE;
		case BSONType::INT32:
			return TYPE_STRING_INT32;
		case BSONType::TIMESTAMP:
			return TYPE_STRING_TIMESTAMP;
		case BSONType::INT64:
			return TYPE_STRING_INT64;
		case BSONType::DECIMAL128:
			return TYPE_STRING_DECIMAL128;
		case BSONType::MIN_KEY:
			return TYPE_STRING_MIN_KEY;
		case BSONType::MAX_KEY:
			return TYPE_STRING_MAX_KEY;
		default:
			throw InternalException("Unknown BSON type");
		}
	}

	//! Validate a BSON document
	static bool ValidateDocument(const uint8_t *data, idx_t size);

	//! Get the size of a BSON value
	static idx_t GetValueSize(BSONType type, const uint8_t *value, idx_t remaining);

	//! Parse JSONPath-like path for BSON traversal
	enum class PathType : uint8_t {
		REGULAR = 0,
		WILDCARD = 1 // For future support
	};

	//! Path segment: either object key or array index
	struct PathSegment {
		bool is_array_index;
		union {
			string key;  // Object key
			idx_t index; // Array index
		};

		PathSegment() : is_array_index(false), key() {
		}
		~PathSegment() {
			if (!is_array_index) {
				key.~basic_string();
			}
		}
		PathSegment(const PathSegment &other) : is_array_index(other.is_array_index) {
			if (is_array_index) {
				index = other.index;
			} else {
				new (&key) string(other.key);
			}
		}
		PathSegment &operator=(const PathSegment &other) {
			if (this != &other) {
				this->~PathSegment();
				new (this) PathSegment(other);
			}
			return *this;
		}
	};

	//! Parse a JSONPath into segments
	static PathType ParsePath(const char *path, idx_t len, vector<PathSegment> &segments);

	//! Traverse BSON document following path segments
	static bool TraversePath(const uint8_t *doc_data, idx_t doc_size, const vector<PathSegment> &segments,
	                         BSONElement &result);

	//! Find element in BSON document by key
	static bool FindElement(const uint8_t *doc_data, idx_t doc_size, const char *key, idx_t key_len,
	                        BSONElement &result);

	//! Get element at array index
	static bool GetArrayElement(const uint8_t *array_data, idx_t array_size, idx_t index, BSONElement &result);

	//! Read int32 from little-endian bytes
	static inline int32_t ReadInt32(const uint8_t *data) {
		int32_t value;
		memcpy(&value, data, sizeof(int32_t));
		return value;
	}

	//! Read int64 from little-endian bytes
	static inline int64_t ReadInt64(const uint8_t *data) {
		int64_t value;
		memcpy(&value, data, sizeof(int64_t));
		return value;
	}

	//! Read double from little-endian bytes
	static inline double ReadDouble(const uint8_t *data) {
		double value;
		memcpy(&value, data, sizeof(double));
		return value;
	}
};

} // namespace duckdb
