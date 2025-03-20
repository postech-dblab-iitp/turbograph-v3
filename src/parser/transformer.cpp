#include "parser/transformer.hpp"
#include "common/exception.hpp"
#include "parser/statement/explain_statement.hpp"
#include "common/string_util.hpp"

namespace duckdb {

std::vector<std::shared_ptr<CypherStatement>> Transformer::transform() {
    std::vector<std::shared_ptr<CypherStatement>> statements;
    for (auto& oc_Statement : root.oC_Cypher()) {
        auto statement = transformStatement(*oc_Statement->oC_Statement());
        if (oc_Statement->oC_AnyCypherOption()) {
            auto cypherOption = oc_Statement->oC_AnyCypherOption();
            auto explainType = ExplainType::EXPLAIN_STANDARD;
            if (cypherOption->oC_Explain()) {
                explainType = ExplainType::EXPLAIN_ANALYZE;
            }
            statements.push_back(
                std::make_unique<ExplainStatement>(std::move(statement), explainType));
            continue;
        }
        statements.push_back(std::move(statement));
    }
    return statements;
}

std::unique_ptr<CypherStatement> Transformer::transformStatement(CypherParser::OC_StatementContext& ctx) {
    if (ctx.oC_Query()) {
        return transformQuery(*ctx.oC_Query());
    } else {
        throw ParserException("Unsupported Cypher statement");
    }
}

std::unique_ptr<ParsedExpression> Transformer::transformWhere(CypherParser::OC_WhereContext& ctx) {
    return transformExpression(*ctx.oC_Expression());
}

std::string Transformer::transformVariable(CypherParser::OC_VariableContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSchemaName(CypherParser::OC_SchemaNameContext& ctx) {
    return transformSymbolicName(*ctx.oC_SymbolicName());
}

std::string Transformer::transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx) {
    if (ctx.EscapedSymbolicName()) {
        std::string escapedSymbolName = ctx.EscapedSymbolicName()->getText();
        // escapedSymbolName symbol will be of form "`Some.Value`". Therefore, we need to sanitize
        // it such that we don't store the symbol with escape character.
        return escapedSymbolName.substr(1, escapedSymbolName.size() - 2);
    } else {
        return ctx.getText();
    }
}

std::string Transformer::transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral) {
    auto str = stringLiteral.getText();
    return StringUtil::RemoveEscapedCharacters(str);
}

}