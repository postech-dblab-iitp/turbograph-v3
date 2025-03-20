#pragma once

#include "parser/query/return_with_clause/projection_body.hpp"

namespace duckdb {

class ReturnClause {
public:
    explicit ReturnClause(std::unique_ptr<ProjectionBody> projectionBody)
        : projectionBody{std::move(projectionBody)} {}
        
    virtual ~ReturnClause() = default;

    inline const ProjectionBody* getProjectionBody() const { return projectionBody.get(); }

    virtual std::string ToString() const {
        return "RETURN " + projectionBody->ToString();
    }

private:
    std::unique_ptr<ProjectionBody> projectionBody;
};

}