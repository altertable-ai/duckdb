#include "include/icu-datetrunc.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-zone-offsets.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <cstring>

namespace duckdb {

struct ICUDateTrunc : public ICUDateFunc {
	static void PreserveOffsets(icu::Calendar *calendar) {
		//	We have to extract _everything_ before setting anything
		//	Otherwise ICU will clear the fStamp fields
		//	This also means we must call this method first.

		//	Force reuse of offsets when reassembling truncated sub-hour times.
		const auto zone_offset = ExtractField(calendar, UCAL_ZONE_OFFSET);
		const auto dst_offset = ExtractField(calendar, UCAL_DST_OFFSET);

		calendar->set(UCAL_ZONE_OFFSET, zone_offset);
		calendar->set(UCAL_DST_OFFSET, dst_offset);
	}

	static void TruncMicrosecondInternal(icu::Calendar *calendar, uint64_t &micros) {
	}

	static void TruncMicrosecond(icu::Calendar *calendar, uint64_t &micros) {
		PreserveOffsets(calendar);
		TruncMicrosecondInternal(calendar, micros);
	}

	static void TruncMillisecondInternal(icu::Calendar *calendar, uint64_t &micros) {
		TruncMicrosecondInternal(calendar, micros);
		micros = 0;
	}

	static void TruncMillisecond(icu::Calendar *calendar, uint64_t &micros) {
		PreserveOffsets(calendar);
		TruncMillisecondInternal(calendar, micros);
	}

	static void TruncSecondInternal(icu::Calendar *calendar, uint64_t &micros) {
		TruncMillisecondInternal(calendar, micros);
		calendar->set(UCAL_MILLISECOND, 0);
	}

	static void TruncSecond(icu::Calendar *calendar, uint64_t &micros) {
		PreserveOffsets(calendar);
		TruncSecondInternal(calendar, micros);
	}

	static void TruncMinuteInternal(icu::Calendar *calendar, uint64_t &micros) {
		TruncSecondInternal(calendar, micros);
		calendar->set(UCAL_SECOND, 0);
	}

	static void TruncMinute(icu::Calendar *calendar, uint64_t &micros) {
		PreserveOffsets(calendar);
		TruncMinuteInternal(calendar, micros);
	}

	static void TruncHour(icu::Calendar *calendar, uint64_t &micros) {
		TruncMinuteInternal(calendar, micros);
		calendar->set(UCAL_MINUTE, 0);
	}

	static void TruncDay(icu::Calendar *calendar, uint64_t &micros) {
		TruncHour(calendar, micros);
		calendar->set(UCAL_HOUR_OF_DAY, 0);
	}

	static void TruncWeek(icu::Calendar *calendar, uint64_t &micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		TruncDay(calendar, micros);
		calendar->set(UCAL_DAY_OF_WEEK, UCAL_MONDAY);
	}

	static void TruncMonth(icu::Calendar *calendar, uint64_t &micros) {
		TruncDay(calendar, micros);
		calendar->set(UCAL_DATE, 1);
	}

	static void TruncQuarter(icu::Calendar *calendar, uint64_t &micros) {
		TruncMonth(calendar, micros);
		auto mm = ExtractField(calendar, UCAL_MONTH);
		calendar->set(UCAL_MONTH, (mm / 3) * 3);
	}

	static void TruncYear(icu::Calendar *calendar, uint64_t &micros) {
		TruncMonth(calendar, micros);
		calendar->set(UCAL_MONTH, UCAL_JANUARY);
	}

	static void TruncISOYear(icu::Calendar *calendar, uint64_t &micros) {
		TruncWeek(calendar, micros);
		calendar->set(UCAL_WEEK_OF_YEAR, 1);
	}

	static void TruncDecade(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 10;
		calendar->set(UCAL_YEAR, yyyy * 10);
	}

	static void TruncCentury(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 100;
		calendar->set(UCAL_YEAR, yyyy * 100);
	}

	static void TruncMillenium(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 1000;
		calendar->set(UCAL_YEAR, yyyy * 1000);
	}

	static void TruncEra(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto era = ExtractField(calendar, UCAL_ERA);
		calendar->set(UCAL_YEAR, 0);
		calendar->set(UCAL_ERA, era);
	}

	static timestamp_t TruncWithICU(icu::Calendar *calendar, part_trunc_t truncator, timestamp_t input) {
		auto micros = SetTime(calendar, input);
		truncator(calendar, micros);
		return GetTimeUnsafe(calendar, micros);
	}

	//! The length in µs of the parts that always span the same amount of wall time, or 0.
	static int64_t FixedPartLength(DatePartSpecifier part) {
		switch (part) {
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DOW:
		case DatePartSpecifier::ISODOW:
		case DatePartSpecifier::DOY:
		case DatePartSpecifier::JULIAN_DAY:
			return Interval::MICROS_PER_DAY;
		case DatePartSpecifier::HOUR:
			return Interval::MICROS_PER_HOUR;
		case DatePartSpecifier::MINUTE:
			return Interval::MICROS_PER_MINUTE;
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::EPOCH:
			return Interval::MICROS_PER_SEC;
		case DatePartSpecifier::MILLISECONDS:
			return Interval::MICROS_PER_MSEC;
		case DatePartSpecifier::MICROSECONDS:
			return 1;
		default:
			return 0;
		}
	}

	//! A constant part of fixed length, truncated with arithmetic on µs instead of ICU calls.
	struct ArithmeticTruncData : public BindData {
		ArithmeticTruncData(ClientContext &context, DatePartSpecifier part, int64_t unit_p)
		    : BindData(context), truncator(TruncationFactory(part)), unit(unit_p) {
		}

		//! Truncates the instants the arithmetic cannot handle
		part_trunc_t truncator;
		int64_t unit;
		//! Unset below a minute: offsets are whole seconds, so the instant truncates like its wall time.
		shared_ptr<const ZoneOffsets> offsets;

		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<ArithmeticTruncData>(*this);
		}
	};

	//! The per-execution state of an arithmetic truncation.
	struct ArithmeticTruncator {
		explicit ArithmeticTruncator(const ArithmeticTruncData &data) : unit(data.unit), offsets(data.offsets.get()) {
		}

		const int64_t unit;
		const ZoneOffsets *const offsets;
		ZoneOffsets::Cursor instant_cursor;
		ZoneOffsets::Cursor wall_cursor;

		//! Offsets and units are under a day, so truncation moves an instant by less than three days.
		static constexpr int64_t MIN_SAFE = NumericLimits<int64_t>::Minimum() + 3 * Interval::MICROS_PER_DAY;
		static constexpr int64_t MAX_SAFE = NumericLimits<int64_t>::Maximum() - 3 * Interval::MICROS_PER_DAY;

		static int64_t Floor(int64_t value, int64_t unit) {
			const auto remainder = value % unit;
			return value - (remainder < 0 ? remainder + unit : remainder);
		}

		bool TryTrunc(int64_t utc, int64_t &result) {
			if (utc < MIN_SAFE || utc > MAX_SAFE) {
				return false;
			}
			if (!offsets) {
				result = Floor(utc, unit);
				return true;
			}
			int64_t offset;
			if (!offsets->OffsetAtInstant(utc, instant_cursor, offset)) {
				return false;
			}
			const auto floored = Floor(utc + offset, unit);
			//	Like ICU, keep the instant's offset below an hour (PreserveOffsets) and re-resolve it from an hour up.
			if (unit >= Interval::MICROS_PER_HOUR && !offsets->OffsetAtWallTime(floored, wall_cursor, offset)) {
				return false;
			}
			result = floored - offset;
			return true;
		}
	};

	template <typename T>
	static void ArithmeticTruncFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &info = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<ArithmeticTruncData>();
		ArithmeticTruncator truncator(info);
		CalendarPtr calendar;
		UnaryExecutor::Execute<T, timestamp_t>(args.data[1], result, args.size(), [&](T input) {
			if (!Timestamp::IsFinite(input)) {
				return input;
			}
			int64_t truncated;
			if (truncator.TryTrunc(input.value, truncated)) {
				return timestamp_t(truncated);
			}
			if (!calendar) {
				calendar.reset(info.calendar->clone());
			}
			return TruncWithICU(calendar.get(), info.truncator, input);
		});
	}

	template <typename T>
	static unique_ptr<FunctionData> BindDateTrunc(ClientContext &context, ScalarFunction &bound_function,
	                                              vector<unique_ptr<Expression>> &arguments) {
		if (!arguments[0]->IsFoldable()) {
			return Bind(context, bound_function, arguments);
		}
		const auto part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		DatePartSpecifier part;
		if (part_value.IsNull() || !TryGetDatePartSpecifier(part_value.ToString(), part) || !FixedPartLength(part)) {
			return Bind(context, bound_function, arguments);
		}

		auto data = make_uniq<ArithmeticTruncData>(context, part, FixedPartLength(part));
		if (data->unit >= Interval::MICROS_PER_MINUTE) {
			//	Only the offsets are modelled, so other calendars stay on ICU.
			if (std::strcmp(data->calendar->getType(), "gregorian") == 0) {
				data->offsets = ZoneOffsets::Build(data->calendar->getTimeZone());
			}
			if (!data->offsets) {
				return Bind(context, bound_function, arguments);
			}
		}
		bound_function.SetFunctionCallback(ArithmeticTruncFunction<T>);
		return std::move(data);
	}

	template <typename T>
	static void ICUDateTruncFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(part_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				const auto specifier = ConstantVector::GetData<string_t>(part_arg)->GetString();
				auto truncator = TruncationFactory(GetDatePartSpecifier(specifier));
				UnaryExecutor::Execute<T, timestamp_t>(date_arg, result, args.size(), [&](T input) {
					if (Timestamp::IsFinite(input)) {
						return TruncWithICU(calendar.get(), truncator, input);
					} else {
						return input;
					}
				});
			}
		} else {
			BinaryExecutor::Execute<string_t, T, timestamp_t>(
			    part_arg, date_arg, result, args.size(), [&](string_t specifier, T input) {
				    if (Timestamp::IsFinite(input)) {
					    auto truncator = TruncationFactory(GetDatePartSpecifier(specifier.GetString()));
					    return TruncWithICU(calendar.get(), truncator, input);
				    } else {
					    return input;
				    }
			    });
		}
	}

	template <typename TA>
	static ScalarFunction GetDateTruncFunction(const LogicalTypeId &type) {
		return ScalarFunction({LogicalType::VARCHAR, type}, LogicalType::TIMESTAMP_TZ, ICUDateTruncFunction<TA>,
		                      BindDateTrunc<TA>);
	}

	static void AddBinaryTimestampFunction(const string &name, ExtensionLoader &loader) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateTruncFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		loader.RegisterFunction(set);
	}
};

ICUDateFunc::part_trunc_t ICUDateFunc::TruncationFactory(DatePartSpecifier type) {
	switch (type) {
	case DatePartSpecifier::ERA:
		return ICUDateTrunc::TruncEra;
	case DatePartSpecifier::MILLENNIUM:
		return ICUDateTrunc::TruncMillenium;
	case DatePartSpecifier::CENTURY:
		return ICUDateTrunc::TruncCentury;
	case DatePartSpecifier::DECADE:
		return ICUDateTrunc::TruncDecade;
	case DatePartSpecifier::YEAR:
		return ICUDateTrunc::TruncYear;
	case DatePartSpecifier::QUARTER:
		return ICUDateTrunc::TruncQuarter;
	case DatePartSpecifier::MONTH:
		return ICUDateTrunc::TruncMonth;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return ICUDateTrunc::TruncWeek;
	case DatePartSpecifier::ISOYEAR:
		return ICUDateTrunc::TruncISOYear;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return ICUDateTrunc::TruncDay;
	case DatePartSpecifier::HOUR:
		return ICUDateTrunc::TruncHour;
	case DatePartSpecifier::MINUTE:
		return ICUDateTrunc::TruncMinute;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return ICUDateTrunc::TruncSecond;
	case DatePartSpecifier::MILLISECONDS:
		return ICUDateTrunc::TruncMillisecond;
	case DatePartSpecifier::MICROSECONDS:
		return ICUDateTrunc::TruncMicrosecond;
	default:
		throw NotImplementedException("Specifier type not implemented for ICU DATETRUNC");
	}
}

timestamp_t ICUDateFunc::CurrentMidnight(icu::Calendar *calendar, ExpressionState &state) {
	const auto current_timestamp = MetaTransaction::Get(state.GetContext()).start_timestamp;
	auto current_micros = SetTime(calendar, current_timestamp);
	ICUDateTrunc::TruncDay(calendar, current_micros);
	return GetTime(calendar);
}

void RegisterICUDateTruncFunctions(ExtensionLoader &loader) {
	ICUDateTrunc::AddBinaryTimestampFunction("date_trunc", loader);
	ICUDateTrunc::AddBinaryTimestampFunction("datetrunc", loader);
}

} // namespace duckdb
