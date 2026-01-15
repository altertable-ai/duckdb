#include "bson_common.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

bool BSONCommon::ValidateDocument(const uint8_t *data, idx_t size) {
	// Minimum BSON document: 5 bytes (4-byte length + 1-byte terminator)
	if (size < 5) {
		return false;
	}

	// Read document length (first 4 bytes, little-endian int32)
	int32_t doc_len = ReadInt32(data);
	if (doc_len < 5 || (idx_t)doc_len > size) {
		return false;
	}

	// Check terminator (last byte must be 0x00)
	if (data[doc_len - 1] != 0x00) {
		return false;
	}

	// Validate elements
	idx_t pos = 4;                     // Skip length field
	while (pos < (idx_t)doc_len - 1) { // -1 for terminator
		if (pos >= size) {
			return false;
		}

		// Read type byte
		auto type = static_cast<BSONType>(data[pos++]);

		// Skip element name (null-terminated cstring)
		while (pos < (idx_t)doc_len && data[pos] != 0x00) {
			pos++;
		}
		if (pos >= (idx_t)doc_len) {
			return false;
		}
		pos++; // Skip null terminator

		// Get value size and skip it
		idx_t remaining = doc_len - pos;
		idx_t value_size = GetValueSize(type, data + pos, remaining);
		if (value_size == 0 || value_size > remaining) {
			return false;
		}
		pos += value_size;
	}

	return pos == (idx_t)doc_len - 1;
}

idx_t BSONCommon::GetValueSize(BSONType type, const uint8_t *value, idx_t remaining) {
	if (remaining < 1) {
		return 0;
	}

	switch (type) {
	case BSONType::DOUBLE:
		return 8;
	case BSONType::STRING: {
		if (remaining < 4) {
			return 0;
		}
		int32_t str_len = ReadInt32(value);
		if (str_len < 1 || str_len > (int32_t)remaining - 4) {
			return 0;
		}
		return 4 + str_len; // 4-byte length + string + null terminator
	}
	case BSONType::DOCUMENT:
	case BSONType::ARRAY: {
		if (remaining < 4) {
			return 0;
		}
		int32_t doc_len = ReadInt32(value);
		if (doc_len < 5 || doc_len > (int32_t)remaining) {
			return 0;
		}
		return doc_len;
	}
	case BSONType::BINARY: {
		if (remaining < 5) {
			return 0;
		}
		int32_t bin_len = ReadInt32(value);
		if (bin_len < 0 || bin_len > (int32_t)remaining - 5) {
			return 0;
		}
		return 5 + bin_len; // 4-byte length + 1-byte subtype + data
	}
	case BSONType::UNDEFINED:
		return 0; // Deprecated, no value
	case BSONType::OBJECT_ID:
		return 12; // 12-byte ObjectId
	case BSONType::BOOLEAN:
		return 1;
	case BSONType::DATE_TIME:
		return 8; // 8-byte int64 (milliseconds since epoch)
	case BSONType::NULL_VALUE:
		return 0; // No value
	case BSONType::REGEX: {
		// Two null-terminated cstrings: pattern and options
		idx_t pos = 0;
		// Skip pattern
		while (pos < remaining && value[pos] != 0x00) {
			pos++;
		}
		if (pos >= remaining) {
			return 0;
		}
		pos++; // Skip null terminator
		// Skip options
		while (pos < remaining && value[pos] != 0x00) {
			pos++;
		}
		if (pos >= remaining) {
			return 0;
		}
		return pos + 1;
	}
	case BSONType::DB_POINTER: {
		if (remaining < 4) {
			return 0;
		}
		int32_t str_len = ReadInt32(value);
		if (str_len < 1 || str_len > (int32_t)remaining - 16) {
			return 0;
		}
		return 4 + str_len + 12; // string + 12-byte ObjectId
	}
	case BSONType::JAVASCRIPT: {
		if (remaining < 4) {
			return 0;
		}
		int32_t str_len = ReadInt32(value);
		if (str_len < 1 || str_len > (int32_t)remaining - 4) {
			return 0;
		}
		return 4 + str_len;
	}
	case BSONType::SYMBOL: {
		if (remaining < 4) {
			return 0;
		}
		int32_t str_len = ReadInt32(value);
		if (str_len < 1 || str_len > (int32_t)remaining - 4) {
			return 0;
		}
		return 4 + str_len;
	}
	case BSONType::JAVASCRIPT_WITH_SCOPE: {
		if (remaining < 4) {
			return 0;
		}
		int32_t total_len = ReadInt32(value);
		if (total_len < 14 || total_len > (int32_t)remaining) {
			return 0;
		}
		return total_len;
	}
	case BSONType::INT32:
		return 4;
	case BSONType::TIMESTAMP:
		return 8;
	case BSONType::INT64:
		return 8;
	case BSONType::DECIMAL128:
		return 16;
	case BSONType::MIN_KEY:
	case BSONType::MAX_KEY:
		return 0;
	default:
		return 0;
	}
}

BSONCommon::PathType BSONCommon::ParsePath(const char *path, idx_t len, vector<PathSegment> &segments) {
	// Simple JSONPath parser: $.key1[0].key2 format
	// Returns REGULAR for now, WILDCARD support can be added later

	if (len == 0) {
		return PathType::REGULAR;
	}

	// Path should start with '$'
	if (path[0] != '$') {
		throw InvalidInputException("BSON path must start with '$'");
	}

	idx_t pos = 1;
	while (pos < len) {
		char c = path[pos];

		if (c == '.') {
			// Object key access
			pos++;
			if (pos >= len) {
				throw InvalidInputException("BSON path ends with '.'");
			}

			// Check for quoted key
			bool quoted = (path[pos] == '"');
			if (quoted) {
				pos++;
			}

			idx_t key_start = pos;
			while (pos < len) {
				if (quoted) {
					if (path[pos] == '"') {
						break;
					}
				} else {
					if (path[pos] == '.' || path[pos] == '[') {
						break;
					}
				}
				pos++;
			}

			if (pos == key_start) {
				throw InvalidInputException("Empty key in BSON path");
			}

			PathSegment seg;
			seg.is_array_index = false;
			new (&seg.key) string(path + key_start, pos - key_start);
			segments.push_back(seg);

			if (quoted) {
				if (pos >= len || path[pos] != '"') {
					throw InvalidInputException("Unclosed quoted key in BSON path");
				}
				pos++;
			}
		} else if (c == '[') {
			// Array index access
			pos++;
			if (pos >= len) {
				throw InvalidInputException("BSON path ends with '['");
			}

			idx_t index_start = pos;
			while (pos < len && path[pos] >= '0' && path[pos] <= '9') {
				pos++;
			}

			if (pos == index_start || pos >= len || path[pos] != ']') {
				throw InvalidInputException("Invalid array index in BSON path");
			}

			string index_str(path + index_start, pos - index_start);
			PathSegment seg;
			seg.is_array_index = true;
			seg.index = std::stoull(index_str);
			segments.push_back(seg);

			pos++; // Skip ']'
		} else {
			throw InvalidInputException("Unexpected character in BSON path");
		}
	}

	return PathType::REGULAR;
}

bool BSONCommon::FindElement(const uint8_t *doc_data, idx_t doc_size, const char *key, idx_t key_len,
                             BSONElement &result) {
	if (doc_size < 5) {
		return false;
	}

	int32_t doc_len = ReadInt32(doc_data);
	if (doc_len < 5 || (idx_t)doc_len > doc_size) {
		return false;
	}

	idx_t pos = 4; // Skip length field
	while (pos < (idx_t)doc_len - 1) {
		// Read type
		result.type = static_cast<BSONType>(doc_data[pos++]);

		// Read key (null-terminated cstring)
		result.key = (const char *)(doc_data + pos);
		idx_t elem_key_len = 0;
		while (pos < (idx_t)doc_len && doc_data[pos] != 0x00) {
			elem_key_len++;
			pos++;
		}
		if (pos >= (idx_t)doc_len) {
			return false;
		}
		pos++; // Skip null terminator
		result.key_len = elem_key_len;

		// Get value
		result.value = doc_data + pos;
		idx_t remaining = doc_len - pos;
		result.value_len = GetValueSize(result.type, result.value, remaining);
		if (result.value_len == 0 || result.value_len > remaining) {
			return false;
		}

		// Check if this is the key we're looking for
		if (elem_key_len == key_len && memcmp(result.key, key, key_len) == 0) {
			return true;
		}

		pos += result.value_len;
	}

	return false;
}

bool BSONCommon::GetArrayElement(const uint8_t *array_data, idx_t array_size, idx_t index, BSONElement &result) {
	// BSON arrays are encoded as documents with keys "0", "1", "2", etc.
	string index_str = std::to_string(index);
	return FindElement(array_data, array_size, index_str.c_str(), index_str.length(), result);
}

bool BSONCommon::TraversePath(const uint8_t *doc_data, idx_t doc_size, const vector<PathSegment> &segments,
                              BSONElement &result) {
	const uint8_t *current_data = doc_data;
	idx_t current_size = doc_size;

	for (const auto &segment : segments) {
		if (segment.is_array_index) {
			// Array access
			if (!GetArrayElement(current_data, current_size, segment.index, result)) {
				return false;
			}
		} else {
			// Object key access
			if (!FindElement(current_data, current_size, segment.key.c_str(), segment.key.length(), result)) {
				return false;
			}
		}

		// If not at the last segment, navigate into document/array
		if (&segment != &segments.back()) {
			if (result.type != BSONType::DOCUMENT && result.type != BSONType::ARRAY) {
				return false;
			}
			current_data = result.value;
			current_size = result.value_len;
		}
	}

	return true;
}

} // namespace duckdb
