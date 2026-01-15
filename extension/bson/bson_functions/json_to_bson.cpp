#include "bson_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

// Helper to write BSON elements
static idx_t WriteBSONString(uint8_t *dest, const char *str, idx_t len) {
	// BSON string: 4-byte length (including null) + string + null terminator
	int32_t bson_len = len + 1;
	memcpy(dest, &bson_len, 4);
	memcpy(dest + 4, str, len);
	dest[4 + len] = 0x00;
	return 4 + len + 1;
}

static idx_t WriteBSONInt32(uint8_t *dest, int32_t value) {
	memcpy(dest, &value, 4);
	return 4;
}

static idx_t WriteBSONInt64(uint8_t *dest, int64_t value) {
	memcpy(dest, &value, 8);
	return 8;
}

static idx_t WriteBSONDouble(uint8_t *dest, double value) {
	memcpy(dest, &value, 8);
	return 8;
}

static idx_t WriteBSONBool(uint8_t *dest, bool value) {
	dest[0] = value ? 0x01 : 0x00;
	return 1;
}

// Forward declaration for recursive conversion
static idx_t JSONToBSONValue(yyjson_val *val, uint8_t *dest, idx_t max_size);

static idx_t JSONToBSONDocument(yyjson_val *obj, uint8_t *dest, idx_t max_size) {
	if (max_size < 5) {
		throw InvalidInputException("Buffer too small for BSON document");
	}

	// Reserve space for document length
	uint8_t *length_pos = dest;
	uint8_t *current = dest + 4;
	idx_t remaining = max_size - 4;

	// Iterate through object properties
	yyjson_obj_iter iter;
	yyjson_obj_iter_init(obj, &iter);
	yyjson_val *key_val;
	while ((key_val = yyjson_obj_iter_next(&iter))) {
		yyjson_val *val = yyjson_obj_iter_get_val(key_val);
		const char *key = yyjson_get_str(key_val);
		size_t key_len = yyjson_get_len(key_val);

		if (remaining < key_len + 3) {
			throw InvalidInputException("Buffer too small for BSON element");
		}

		// Determine BSON type and write type byte
		yyjson_type type = yyjson_get_type(val);
		switch (type) {
		case YYJSON_TYPE_NULL:
			*current++ = 0x0A; // null type
			memcpy(current, key, key_len);
			current += key_len;
			*current++ = 0x00;
			remaining -= (key_len + 2);
			break;

		case YYJSON_TYPE_BOOL:
			*current++ = 0x08; // boolean type
			memcpy(current, key, key_len);
			current += key_len;
			*current++ = 0x00;
			current += WriteBSONBool(current, yyjson_get_bool(val));
			remaining -= (key_len + 3);
			break;

		case YYJSON_TYPE_NUM: {
			yyjson_subtype subtype = yyjson_get_subtype(val);
			if (subtype == YYJSON_SUBTYPE_REAL) {
				*current++ = 0x01; // double type
				memcpy(current, key, key_len);
				current += key_len;
				*current++ = 0x00;
				current += WriteBSONDouble(current, yyjson_get_real(val));
				remaining -= (key_len + 10);
			} else if (subtype == YYJSON_SUBTYPE_SINT) {
				int64_t num = yyjson_get_sint(val);
				if (num >= INT32_MIN && num <= INT32_MAX) {
					*current++ = 0x10; // int32 type
					memcpy(current, key, key_len);
					current += key_len;
					*current++ = 0x00;
					current += WriteBSONInt32(current, (int32_t)num);
					remaining -= (key_len + 6);
				} else {
					*current++ = 0x12; // int64 type
					memcpy(current, key, key_len);
					current += key_len;
					*current++ = 0x00;
					current += WriteBSONInt64(current, num);
					remaining -= (key_len + 10);
				}
			} else {               // UINT
				*current++ = 0x12; // int64 type
				memcpy(current, key, key_len);
				current += key_len;
				*current++ = 0x00;
				current += WriteBSONInt64(current, (int64_t)yyjson_get_uint(val));
				remaining -= (key_len + 10);
			}
			break;
		}

		case YYJSON_TYPE_STR: {
			*current++ = 0x02; // string type
			memcpy(current, key, key_len);
			current += key_len;
			*current++ = 0x00;
			const char *str = yyjson_get_str(val);
			size_t str_len = yyjson_get_len(val);
			current += WriteBSONString(current, str, str_len);
			remaining -= (key_len + str_len + 7);
			break;
		}

		case YYJSON_TYPE_ARR: {
			*current++ = 0x04; // array type
			memcpy(current, key, key_len);
			current += key_len;
			*current++ = 0x00;
			idx_t written = JSONToBSONDocument(val, current, remaining);
			current += written;
			remaining -= (key_len + written + 2);
			break;
		}

		case YYJSON_TYPE_OBJ: {
			*current++ = 0x03; // document type
			memcpy(current, key, key_len);
			current += key_len;
			*current++ = 0x00;
			idx_t written = JSONToBSONDocument(val, current, remaining);
			current += written;
			remaining -= (key_len + written + 2);
			break;
		}

		default:
			throw InvalidInputException("Unsupported JSON type in conversion to BSON");
		}
	}

	// Write terminator
	if (remaining < 1) {
		throw InvalidInputException("Buffer too small for BSON terminator");
	}
	*current++ = 0x00;

	// Write document length
	int32_t doc_len = (int32_t)(current - dest);
	memcpy(length_pos, &doc_len, 4);

	return doc_len;
}

static bool CastJSONToBSON(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool all_succeeded = true;

	// Parse JSON strings and convert to BSON
	UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
	    source, result, count, [&](string_t json_str, ValidityMask &mask, idx_t idx) {
		    // Parse JSON using yyjson
		    yyjson_doc *doc = yyjson_read(json_str.GetData(), json_str.GetSize(), 0);
		    if (!doc) {
			    mask.SetInvalid(idx);
			    string error = "Invalid JSON in JSON to BSON cast";
			    HandleCastError::AssignError(error, parameters);
			    all_succeeded = false;
			    return string_t();
		    }

		    yyjson_val *root = yyjson_doc_get_root(doc);
		    if (!yyjson_is_obj(root) && !yyjson_is_arr(root)) {
			    yyjson_doc_free(doc);
			    mask.SetInvalid(idx);
			    string error = "JSON root must be object or array for BSON conversion";
			    HandleCastError::AssignError(error, parameters);
			    all_succeeded = false;
			    return string_t();
		    }

		    // Allocate buffer for BSON (estimate: JSON size * 2 should be plenty)
		    idx_t bson_size = json_str.GetSize() * 2 + 1024;
		    auto bson_data = unique_ptr<uint8_t[]>(new uint8_t[bson_size]);

		    // Convert to BSON
		    idx_t written;
		    try {
			    written = JSONToBSONDocument(root, bson_data.get(), bson_size);
		    } catch (const Exception &e) {
			    yyjson_doc_free(doc);
			    mask.SetInvalid(idx);
			    string error = string("Failed to convert JSON to BSON: ") + e.what();
			    HandleCastError::AssignError(error, parameters);
			    all_succeeded = false;
			    return string_t();
		    }

		    yyjson_doc_free(doc);

		    // Create BLOB result
		    return StringVector::AddStringOrBlob(result, (const char *)bson_data.get(), written);
	    });

	return all_succeeded;
}

ScalarFunctionSet BSONFunctions::GetJSONToBSONFunction() {
	ScalarFunctionSet set("json_to_bson");

	// JSON string to BSON
	set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BSON(),
	                               [](DataChunk &args, ExpressionState &state, Vector &result) {
		                               CastParameters params(false, nullptr);
		                               CastJSONToBSON(args.data[0], result, args.size(), params);
	                               }));

	// JSON type to BSON
	set.AddFunction(ScalarFunction({LogicalType::JSON()}, LogicalType::BSON(),
	                               [](DataChunk &args, ExpressionState &state, Vector &result) {
		                               CastParameters params(false, nullptr);
		                               CastJSONToBSON(args.data[0], result, args.size(), params);
	                               }));

	return set;
}

void BSONFunctions::RegisterJSONToBSONCast(ExtensionLoader &loader) {
	// Register cast from JSON to BSON
	BoundCastInfo json_to_bson_info(CastJSONToBSON);
	loader.RegisterCastFunction(LogicalType::JSON(), LogicalType::BSON(), std::move(json_to_bson_info), 200);

	// Also allow casting from VARCHAR (JSON strings) to BSON
	BoundCastInfo varchar_to_bson_info(CastJSONToBSON);
	loader.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::BSON(), std::move(varchar_to_bson_info), 150);
}

} // namespace duckdb
