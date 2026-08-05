#include "reader/variant_column_reader.hpp"
#include "reader/variant/variant_binary_decoder.hpp"
#include "reader/variant/variant_shredded_conversion.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Variant Column Reader
//===--------------------------------------------------------------------===//
VariantColumnReader::VariantColumnReader(ClientContext &context, const ParquetReader &reader,
                                         const ParquetColumnSchema &schema,
                                         vector<unique_ptr<ColumnReader>> child_readers_p, bool projected_mode_p,
                                         vector<string> extract_path_p)
    : ColumnReader(reader, schema), context(context), child_readers(std::move(child_readers_p)),
      extract_path(std::move(extract_path_p)), projected_mode(projected_mode_p) {
	D_ASSERT(Type().InternalType() == PhysicalType::STRUCT);

	optional_idx metadata_idx;
	for (idx_t i = 0; i < schema.children.size(); i++) {
		auto &child_name = schema.children[i].name;
		if (child_name == "metadata") {
			metadata_idx = i;
		} else if (child_name == "value") {
			value_reader_idx = i;
		} else if (child_name == "typed_value") {
			typed_value_reader_idx = i;
		}
	}
	if (!metadata_idx.IsValid()) {
		throw InternalException("The Variant column must have a 'metadata' child");
	}
	metadata_reader_idx = metadata_idx.GetIndex();
	if (!child_readers[metadata_reader_idx]) {
		throw InternalException("The Variant column requires a 'metadata' reader");
	}
	if (!projected_mode) {
		if (!value_reader_idx.IsValid() || !child_readers[value_reader_idx.GetIndex()]) {
			throw InternalException("The Variant column must have 'metadata' and 'value' as children");
		}
	} else if (value_reader_idx.IsValid() && child_readers[value_reader_idx.GetIndex()]) {
		throw InternalException("Projected Variant reads must omit the root 'value' reader");
	}
}

ColumnReader &VariantColumnReader::GetChildReader(idx_t child_idx) {
	if (!child_readers[child_idx]) {
		throw InternalException("VariantColumnReader::GetChildReader(%d) - but this child reader is not set",
		                        child_idx);
	}
	return *child_readers[child_idx].get();
}

void VariantColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                         TProtocol &protocol_p) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

static LogicalType GetIntermediateGroupType(optional_ptr<ColumnReader> typed_value) {
	child_list_t<LogicalType> children;
	children.emplace_back("value", LogicalType::BLOB);
	if (typed_value) {
		children.emplace_back("typed_value", typed_value->Type());
	}
	return LogicalType::STRUCT(std::move(children));
}

idx_t VariantColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	if (pending_skips > 0) {
		throw InternalException("VariantColumnReader cannot have pending skips");
	}
	optional_ptr<ColumnReader> typed_value_reader;
	if (typed_value_reader_idx.IsValid()) {
		typed_value_reader = child_readers[typed_value_reader_idx.GetIndex()].get();
	}

	// If the child reader values are all valid, "define_out" may not be initialized at all
	// So, we just initialize them to all be valid beforehand
	std::fill_n(define_out, num_values, MaxDefine());

	Vector metadata_intermediate(LogicalType::BLOB, num_values);
	Vector intermediate_group(GetIntermediateGroupType(typed_value_reader), num_values);
	auto &group_entries = StructVector::GetEntries(intermediate_group);
	auto &value_intermediate = *group_entries[0];

	auto metadata_values =
	    child_readers[metadata_reader_idx]->Read(num_values, define_out, repeat_out, metadata_intermediate);

	idx_t value_values;
	if (value_reader_idx.IsValid() && child_readers[value_reader_idx.GetIndex()]) {
		value_values =
		    child_readers[value_reader_idx.GetIndex()]->Read(num_values, define_out, repeat_out, value_intermediate);
		if (metadata_values != value_values) {
			throw InvalidInputException(
			    "The Variant column did not contain the same amount of values for 'metadata' and 'value'");
		}
	} else {
		// Projected reads omit the root value column; treat every row as missing untyped leftovers.
		value_intermediate.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(value_intermediate, true);
		value_values = metadata_values;
	}

	if (typed_value_reader) {
		auto typed_values = typed_value_reader->Read(num_values, define_out, repeat_out, *group_entries[1]);
		if (typed_values != value_values) {
			throw InvalidInputException(
			    "The shredded Variant column did not contain the same amount of values for 'typed_value' and 'value'");
		}
	}
	const vector<string> *path_ptr = extract_path.empty() ? nullptr : &extract_path;
	auto intermediate = VariantShreddedConversion::Convert(metadata_intermediate, intermediate_group, 0, num_values,
	                                                       num_values, path_ptr);
	VariantValue::ToVARIANT(intermediate, result);

	return value_values;
}

void VariantColumnReader::Skip(idx_t num_values) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->Skip(num_values);
	}
}

void VariantColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->RegisterPrefetch(transport, allow_merge);
	}
}

uint64_t VariantColumnReader::TotalCompressedSize() {
	uint64_t size = 0;
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		size += child->TotalCompressedSize();
	}
	return size;
}

idx_t VariantColumnReader::GroupRowsAvailable() {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		return child->GroupRowsAvailable();
	}
	throw InternalException("No projected columns in struct?");
}

bool VariantColumnReader::TypedValueLayoutToType(const LogicalType &typed_value, LogicalType &output) {
	if (!typed_value.IsNested()) {
		output = typed_value;
		return true;
	}
	auto type_id = typed_value.id();
	if (type_id == LogicalTypeId::STRUCT) {
		//! OBJECT (...)
		auto &object_fields = StructType::GetChildTypes(typed_value);
		child_list_t<LogicalType> children;
		for (auto &object_field : object_fields) {
			auto &name = object_field.first;
			auto &field = object_field.second;
			//! <name>: {
			//! 	value: BLOB,
			//! 	typed_value: <type>
			//! }
			auto &field_children = StructType::GetChildTypes(field);
			idx_t index = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < field_children.size(); i++) {
				if (field_children[i].first == "typed_value") {
					index = i;
					break;
				}
			}
			if (index == DConstants::INVALID_INDEX) {
				//! FIXME: we might be able to just omit this field from the OBJECT, instead of flat-out failing the
				//! conversion No 'typed_value' field, so we can't assign a structured type to this field at all
				return false;
			}
			LogicalType child_type;
			if (!TypedValueLayoutToType(field_children[index].second, child_type)) {
				return false;
			}
			children.emplace_back(name, child_type);
		}
		output = LogicalType::STRUCT(std::move(children));
		return true;
	}
	if (type_id == LogicalTypeId::LIST) {
		//! ARRAY
		auto &element = ListType::GetChildType(typed_value);
		//! element: {
		//! 	value: BLOB,
		//! 	typed_value: <type>
		//! }
		auto &element_children = StructType::GetChildTypes(element);
		idx_t index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < element_children.size(); i++) {
			if (element_children[i].first == "typed_value") {
				index = i;
				break;
			}
		}
		if (index == DConstants::INVALID_INDEX) {
			//! This *might* be allowed by the spec, it's hard to reason about..
			return false;
		}
		LogicalType child_type;
		if (!TypedValueLayoutToType(element_children[index].second, child_type)) {
			return false;
		}
		output = LogicalType::LIST(child_type);
		return true;
	}
	throw InvalidInputException("VARIANT typed value has to be a primitive/struct/list, not %s",
	                            typed_value.ToString());
}

} // namespace duckdb
