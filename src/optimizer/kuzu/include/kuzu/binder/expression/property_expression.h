#pragma once

#include "kuzu/common/configs.h"
#include "kuzu/binder/expression/expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
   public:
    PropertyExpression(
        DataType dataType, const string &propertyName, uint64_t propertyID,
        Expression &nodeOrRel,
        unordered_map<table_id_t, property_id_t>* propertyIDPerTable)
        : Expression{PROPERTY, std::move(dataType),
                     nodeOrRel.getUniqueName() + "." + propertyName},
        //   propertyName{propertyName},
          propertyID{propertyID},
          variableName{nodeOrRel.getUniqueName()},
          variableRawName{nodeOrRel.getRawName()},
          propertyIDPerTable{propertyIDPerTable},
          nodeOrRelExpr(&nodeOrRel)
    {
        rawName = nodeOrRel.getRawName() + "." + propertyName;
    }

    PropertyExpression(const PropertyExpression &other)
        : Expression{PROPERTY, other.dataType, other.uniqueName},
        //   propertyName{other.propertyName},
          propertyID{other.propertyID},
          variableName{other.variableName},
          variableRawName{other.variableRawName},
          propertyIDPerTable{other.propertyIDPerTable}
    {
        rawName = other.rawName;
    }

    // inline string getPropertyName() const { return propertyName; }

    inline uint64_t getPropertyID() const { return propertyID; }

    inline string getVariableName() const
    {
        return variableName;
    }  // returns uniquename of node

    inline string getVariableRawName() const
    {
        return variableRawName;
    }  // returns rawname of node

    inline bool hasPropertyID(table_id_t tableID) const
    {
        return propertyIDPerTable->find(tableID) != propertyIDPerTable->end();
    }
    inline property_id_t getPropertyID(table_id_t tableID) const
    {
        assert(propertyIDPerTable->find(tableID) != propertyIDPerTable->end());
        return propertyIDPerTable->at(tableID);
    }
    void addPropertyID(table_id_t tableID, property_id_t propID)
    {
        if (propertyIDPerTable->find(tableID) != propertyIDPerTable->end()) {
            return;
        }
        propertyIDPerTable->insert(std::make_pair(tableID, propID));
    }

    unordered_map<table_id_t, property_id_t> *getPropertyIDPerTable()
    {
        return propertyIDPerTable;
    }

    inline bool isInternalID() const
    {
        return propertyID == INTERNAL_ID_PROPERTY_KEY_ID;
    }

    inline unique_ptr<Expression> copy() const override
    {
        return make_unique<PropertyExpression>(*this);
    }

    inline void setUsedForFilter() { used_for_filter = true; }
    bool isUsedForFilter() const { return used_for_filter; }

    Expression *getNodeOrRelExpr() { return nodeOrRelExpr; }

   private:
    // string propertyName;
    uint64_t propertyID;
    // reference to a node/rel table
    string variableName;
    string variableRawName;
    bool used_for_filter = false;
    Expression *nodeOrRelExpr;  // parent
    unordered_map<table_id_t, property_id_t>* propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
