#include "duckdb/execution/operator/order/physical_streaming_top_n_per_group.hpp"

#include "duckdb/common/types/value.hpp"

namespace duckdb {

PhysicalStreamingTopNPerGroup::PhysicalStreamingTopNPerGroup(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                           vector<idx_t> group_column_indices_p, idx_t limit_p,
                                                           bool include_row_number_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::STREAMING_TOP_N_PER_GROUP, std::move(types),
                       estimated_cardinality),
      group_column_indices(std::move(group_column_indices_p)), limit(limit_p), include_row_number(include_row_number_p) {
}

namespace {

class StreamingTopNPerGroupState : public OperatorState {
public:
	explicit StreamingTopNPerGroupState(const PhysicalStreamingTopNPerGroup &op, Allocator &) {
		last_group_keys.resize(op.group_column_indices.size());
	}

	vector<Value> last_group_keys;
	bool initialized = false;
	idx_t emitted_in_current_group = 0;
};

static bool GroupKeysDiffer(const DataChunk &input, idx_t row_idx, const StreamingTopNPerGroupState &state,
                            const vector<idx_t> &group_column_indices) {
	for (idx_t g = 0; g < group_column_indices.size(); g++) {
		const auto col = group_column_indices[g];
		auto v_in = input.data[col].GetValue(row_idx);
		if (!Value::NotDistinctFrom(v_in, state.last_group_keys[g])) {
			return true;
		}
	}
	return false;
}

static void CopyGroupKeysFromRow(DataChunk &input, idx_t row_idx, StreamingTopNPerGroupState &state,
                                 const vector<idx_t> &group_column_indices) {
	for (idx_t g = 0; g < group_column_indices.size(); g++) {
		const auto col = group_column_indices[g];
		state.last_group_keys[g] = input.data[col].GetValue(row_idx);
	}
}

} // namespace

unique_ptr<OperatorState> PhysicalStreamingTopNPerGroup::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<StreamingTopNPerGroupState>(*this, BufferAllocator::Get(context.client));
}

OperatorResultType PhysicalStreamingTopNPerGroup::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                            GlobalOperatorState &, OperatorState &state_p) const {
	auto &state = state_p.Cast<StreamingTopNPerGroupState>();
	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t sel_count = 0;
	vector<int64_t> row_numbers;
	if (include_row_number) {
		row_numbers.resize(STANDARD_VECTOR_SIZE);
	}

	for (idx_t i = 0; i < input.size(); i++) {
		const bool new_group =
		    !state.initialized || GroupKeysDiffer(input, i, state, group_column_indices);
		if (new_group) {
			state.emitted_in_current_group = 0;
			CopyGroupKeysFromRow(input, i, state, group_column_indices);
			state.initialized = true;
		}
		if (state.emitted_in_current_group < limit) {
			state.emitted_in_current_group++;
			sel.set_index(sel_count, UnsafeNumericCast<sel_t>(i));
			if (include_row_number) {
				row_numbers[sel_count] = UnsafeNumericCast<int64_t>(state.emitted_in_current_group);
			}
			sel_count++;
		}
	}

	chunk.Reset();
	chunk.SetCardinality(sel_count);
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		chunk.data[c].Slice(input.data[c], sel, sel_count);
	}
	if (include_row_number) {
		auto &target = chunk.data[input.ColumnCount()];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto data = FlatVector::GetData<int64_t>(target);
		for (idx_t j = 0; j < sel_count; j++) {
			data[j] = row_numbers[j];
		}
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

OrderPreservationType PhysicalStreamingTopNPerGroup::OperatorOrder() const {
	return OrderPreservationType::FIXED_ORDER;
}

bool PhysicalStreamingTopNPerGroup::ParallelOperator() const {
	return false;
}

InsertionOrderPreservingMap<string> PhysicalStreamingTopNPerGroup::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Limit"] = to_string(limit);
	string groups;
	for (idx_t i = 0; i < group_column_indices.size(); i++) {
		if (i > 0) {
			groups += ",";
		}
		groups += to_string(group_column_indices[i]);
	}
	result["Groups"] = std::move(groups);
	return result;
}

} // namespace duckdb
