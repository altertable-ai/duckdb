#include "include/icu-zone-offsets.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "unicode/basictz.h"
#include "unicode/tzrule.h"
#include "unicode/tztrans.h"

namespace duckdb {

static int64_t RuleOffset(const icu::TimeZoneRule &rule) {
	return (int64_t(rule.getRawOffset()) + rule.getDSTSavings()) * Interval::MICROS_PER_MSEC;
}

static bool IsUnderADay(int64_t offset) {
	return offset > -Interval::MICROS_PER_DAY && offset < Interval::MICROS_PER_DAY;
}

shared_ptr<const ZoneOffsets> ZoneOffsets::Build(const icu::TimeZone &zone) {
	const auto basic_zone = dynamic_cast<const icu::BasicTimeZone *>(&zone);
	if (!basic_zone) {
		return nullptr;
	}

	//	Recurring rules produce transitions forever, so stop at a year well past scan-scale data.
	const int64_t coverage_end = int64_t(Date::FromDate(2250, 1, 1).days) * Interval::MICROS_PER_DAY;
	//	Early enough to precede every tzdata transition, late enough for exact µs conversion.
	UDate search_from = -1.0e15;

	icu::TimeZoneTransition transition;
	bool has_next = basic_zone->getNextTransition(search_from, false, transition);
	int64_t offset;
	if (has_next) {
		offset = RuleOffset(*transition.getFrom());
	} else {
		int32_t raw = 0;
		int32_t dst = 0;
		UErrorCode status = U_ZERO_ERROR;
		basic_zone->getOffset(0, false, raw, dst, status);
		if (U_FAILURE(status)) {
			return nullptr;
		}
		offset = (int64_t(raw) + dst) * Interval::MICROS_PER_MSEC;
	}
	if (!IsUnderADay(offset)) {
		return nullptr;
	}

	auto result = make_shared_ptr<ZoneOffsets>();
	auto &segments = result->segments;
	segments.push_back(Segment {NumericLimits<int64_t>::Minimum(), NumericLimits<int64_t>::Minimum(), offset});
	for (; has_next; has_next = basic_zone->getNextTransition(search_from, false, transition)) {
		search_from = transition.getTime();
		const auto utc = int64_t(search_from) * Interval::MICROS_PER_MSEC;
		const auto offset_after = RuleOffset(*transition.getTo());
		const bool past_coverage = utc >= coverage_end;
		if (!past_coverage && offset_after == segments.back().offset) {
			continue;
		}
		if (!IsUnderADay(offset_after)) {
			return nullptr;
		}
		//	A fall-back longer than the time since the previous transition would make wall times non-monotonic.
		if (utc + offset_after <= segments.back().start_local) {
			return nullptr;
		}
		segments.push_back(Segment {utc, utc + offset_after, offset_after});
		if (past_coverage) {
			return std::move(result);
		}
	}
	segments.push_back(Segment {NumericLimits<int64_t>::Maximum(), NumericLimits<int64_t>::Maximum(), 0});
	return std::move(result);
}

} // namespace duckdb
