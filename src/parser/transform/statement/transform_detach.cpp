#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/enums/attachment_scope.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

unique_ptr<DetachStatement> Transformer::TransformDetach(duckdb_libpgquery::PGDetachStmt &stmt) {
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = stmt.db_name;
	info->if_not_found = TransformOnEntryNotFound(stmt.missing_ok);

	if (stmt.options) {
		duckdb_libpgquery::PGListCell *cell;
		for_each_cell(cell, stmt.options->head) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			auto option_name = StringUtil::Lower(def_elem->defname);
			if (option_name != "scope") {
				throw ParserException("Unrecognized option for DETACH: \"%s\"", def_elem->defname);
			}
			if (!def_elem->arg) {
				throw ParserException("DETACH SCOPE option requires a value (GLOBAL or SESSION)");
			}
			auto expr = TransformExpression(def_elem->arg);
			string scope_str;
			if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
				scope_str = StringUtil::Upper(expr->Cast<ConstantExpression>().value.ToString());
			} else if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
				// Unquoted SESSION/GLOBAL are parsed as column references.
				auto &colref = expr->Cast<ColumnRefExpression>();
				scope_str = StringUtil::Upper(colref.GetColumnName());
			} else {
				throw ParserException("DETACH SCOPE option must be a constant (GLOBAL or SESSION)");
			}
			if (scope_str == "SESSION") {
				info->scope = AttachmentScope::SESSION;
			} else if (scope_str == "GLOBAL") {
				info->scope = AttachmentScope::GLOBAL;
			} else {
				throw ParserException("Unrecognized ATTACH/DETACH scope \"%s\" - expected GLOBAL or SESSION", scope_str);
			}
		}
	}

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
