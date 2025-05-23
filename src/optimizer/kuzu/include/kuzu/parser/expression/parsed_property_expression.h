#pragma once



namespace kuzu {
namespace parser {

class ParsedPropertyExpression : public ParsedExpression {
public:
    ParsedPropertyExpression(
        ExpressionType type, string propertyName, unique_ptr<ParsedExpression> child, string raw)
        : ParsedExpression{type, std::move(child), std::move(raw)}, propertyName{
                                                                        std::move(propertyName)} {}

    inline string getPropertyName() const { return propertyName; }

private:
    string propertyName;
};

} // namespace parser
} // namespace kuzu
