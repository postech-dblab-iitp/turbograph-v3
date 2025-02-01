#pragma once

#include "kuzu/parser/query/return_with_clause/projection_body.h"

namespace kuzu {
namespace parser {

class ReturnClause {

public:
    ReturnClause(unique_ptr<ProjectionBody> projectionBody)
        : projectionBody{move(projectionBody)} {}

    virtual ~ReturnClause() = default;

    inline ProjectionBody* getProjectionBody() const { return projectionBody.get(); }

private:
    unique_ptr<ProjectionBody> projectionBody;
};

} // namespace parser
} // namespace kuzu
