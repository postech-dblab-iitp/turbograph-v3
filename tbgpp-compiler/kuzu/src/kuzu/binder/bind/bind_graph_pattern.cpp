#include <set>

#include "binder/binder.h"
#include "catalog_wrapper.hpp"
#include "common/tuple.hpp"
#include "common/type_utils.h"
#include "common/boost_typedefs.hpp"
#include "database.hpp"

namespace kuzu {
namespace binder {

// A graph pattern contains node/rel and a set of key-value pairs associated with the variable. We
// bind node/rel as query graph and key-value pairs as a separate collection. This collection is
// interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will merge key-value pairs
// with other predicates specified in WHERE clause.
pair<unique_ptr<QueryGraphCollection>, unique_ptr<PropertyKeyValCollection>>
Binder::bindGraphPattern(const vector<unique_ptr<PatternElement>> &graphPattern)
{
    auto propertyCollection = make_unique<
        PropertyKeyValCollection>();  // filters for properties appearing in node/edge pattern (a -> (k,value), ...)
    auto queryGraphCollection = make_unique<QueryGraphCollection>();
    for (auto &patternElement : graphPattern) {
        queryGraphCollection->addAndMergeQueryGraphIfConnected(
            bindPatternElement(*patternElement, *propertyCollection));
    }
    return make_pair(std::move(queryGraphCollection),
                     std::move(propertyCollection));
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement &patternElement, PropertyKeyValCollection &collection)
{
    auto queryGraph = make_unique<QueryGraph>();
    queryGraph->setQueryGraphType(
        (QueryGraphType)patternElement.getPatternType());

    // bind partition IDs first
    auto leftNode = bindQueryNode(*patternElement.getFirstNodePattern(),
                                  *queryGraph, collection);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode = bindQueryNode(*patternElementChain->getNodePattern(),
                                       *queryGraph, collection);
        bindQueryRel(*patternElementChain->getRelPattern(), leftNode, rightNode,
                     *queryGraph, collection);
        leftNode = rightNode;
    }

    // iterate queryGraph & remove unnecessary partitions
    // TODO

    // bind schema informations
    uint64_t num_schema_combinations = 1;
    auto firstQueryNode = queryGraph->getQueryNode(0);
    num_schema_combinations *= bindQueryNodeSchema(
        firstQueryNode, *patternElement.getFirstNodePattern(), *queryGraph,
        collection, patternElement.getNumPatternElementChains() > 0);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        num_schema_combinations *= bindQueryNodeSchema(
            queryGraph->getQueryNode(i + 1),
            *patternElementChain->getNodePattern(), *queryGraph, collection, true);
        num_schema_combinations *= bindQueryRelSchema(
            queryGraph->getQueryRel(i), *patternElementChain->getRelPattern(),
            *queryGraph, collection);
    }

    // TODO how to trigger DSI?
    if (num_schema_combinations >= 2) {
        auto firstQueryNode = queryGraph->getQueryNode(0);
        firstQueryNode->setDSITarget();
        for (auto i = 0u; i < patternElement.getNumPatternElementChains();
             ++i) {
            queryGraph->getQueryNode(i + 1)->setDSITarget();
            queryGraph->getQueryRel(i)->setDSITarget();
        }
    }

    if (queryGraph->getQueryGraphType() != QueryGraphType::NONE) {
        string pathName = patternElement.getPathName();
        queryGraph->setQueryPath(pathName);

        if (variablesInScope.find(pathName) != variablesInScope.end()) {
            auto prevVariable = variablesInScope.at(pathName);
            ExpressionBinder::validateExpectedDataType(*prevVariable, PATH);
            throw BinderException("Bind path " + pathName +
                                  " to path with same name is not supported.");
        }
        else {
            if (!pathName.empty()) {
                variablesInScope.insert({pathName, queryGraph->getQueryPath()});
            }
        }
    }

    return queryGraph;
}

// E.g. MATCH (:person)-[:studyAt]->(:person) ...
// static void validateNodeRelConnectivity(const Catalog& catalog_, const RelExpression& rel,
//     const NodeExpression& srcNode, const NodeExpression& dstNode) {
//     set<pair<table_id_t, table_id_t>> srcDstTableIDs;
//     for (auto relTableID : rel.getTableIDs()) {
//         for (auto [srcTableID, dstTableID] :
//             catalog_.getReadOnlyVersion()->getRelTableSchema(relTableID)->srcDstTableIDs) {
//             srcDstTableIDs.insert({srcTableID, dstTableID});
//         }
//     }
//     for (auto srcTableID : srcNode.getTableIDs()) {
//         for (auto dstTableID : dstNode.getTableIDs()) {
//             if (srcDstTableIDs.contains(make_pair(srcTableID, dstTableID))) {
//                 return;
//             }
//         }
//     }
//     throw BinderException("Nodes " + srcNode.getRawName() + " and " + dstNode.getRawName() +
//                           " are not connected through rel " + rel.getRawName() + ".");
// }

static vector<std::pair<std::string, vector<Property>>>
getPropertyNameAndSchemasPairs(
    vector<std::string> propertyNames,
    unordered_map<std::string, vector<Property>> propertyNamesToSchemas)
{
    vector<std::pair<std::string, vector<Property>>>
        propertyNameAndSchemasPairs;
    for (auto &propertyName : propertyNames) {
        auto propertySchemas = propertyNamesToSchemas.at(propertyName);
        propertyNameAndSchemasPairs.emplace_back(propertyName,
                                                 std::move(propertySchemas));
    }
    return propertyNameAndSchemasPairs;
}

static vector<std::pair<std::string, vector<Property>>>
getRelPropertyNameAndPropertiesPairs(
    const vector<RelTableSchema *> &relTableSchemas)
{
    vector<std::string>
        propertyNames;  // preserve order as specified in catalog.
    unordered_map<std::string, vector<Property>> propertyNamesToSchemas;
    for (auto &relTableSchema : relTableSchemas) {
        for (auto &property : relTableSchema->properties) {
            if (!(propertyNamesToSchemas.find(property.name) !=
                  propertyNamesToSchemas.end())) {
                propertyNames.push_back(property.name);
                propertyNamesToSchemas.insert(
                    {property.name, vector<Property>{}});
            }
            propertyNamesToSchemas.at(property.name).push_back(property);
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames,
                                          propertyNamesToSchemas);
}

static vector<std::pair<std::string, vector<Property>>>
getNodePropertyNameAndPropertiesPairs(
    const vector<NodeTableSchema *> &nodeTableSchemas)
{
    vector<std::string>
        propertyNames;  // preserve order as specified in catalog.
    unordered_map<std::string, vector<Property>> propertyNamesToSchemas;
    for (auto &nodeTableSchema : nodeTableSchemas) {
        for (auto &property : nodeTableSchema->properties) {
            if (!(propertyNamesToSchemas.find(property.name) !=
                  propertyNamesToSchemas.end())) {
                // if name not found
                propertyNames.push_back(property.name);
                propertyNamesToSchemas.insert(
                    {property.name, vector<Property>{}});
            }
            propertyNamesToSchemas.at(property.name).push_back(property);
        }
    }
    return getPropertyNameAndSchemasPairs(propertyNames,
                                          propertyNamesToSchemas);
}

void Binder::bindQueryRel(const RelPattern &relPattern,
                          const shared_ptr<NodeExpression> &leftNode,
                          const shared_ptr<NodeExpression> &rightNode,
                          QueryGraph &queryGraph,
                          PropertyKeyValCollection &collection)
{
    auto parsedName = relPattern.getVariableName();
    if (variablesInScope.find(parsedName) != variablesInScope.end()) {
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, REL);
        throw BinderException(
            "Bind relationship " + parsedName +
            " to relationship with same name is not supported.");
    }

    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    if (srcNode->getUniqueName() == dstNode->getUniqueName()) {
        throw BinderException("Self-loop rel " + parsedName +
                              " is not supported.");
    }
    vector<uint64_t> partitionIDs, tableIDs;
    bindRelPartitionIDs(relPattern.getLabelOrTypeNames(), srcNode, dstNode,
                        partitionIDs);

    // bind variable length
    auto boundPair = bindVariableLengthRelBound(relPattern);
    auto &lowerBound = boundPair.first;
    auto &upperBound = boundPair.second;
    bool isVariableLength = lowerBound != upperBound ? true : false;
    auto queryRel = make_shared<RelExpression>(
        getUniqueExpressionName(parsedName), partitionIDs, tableIDs, srcNode,
        dstNode, lowerBound, upperBound);
    queryRel->setAlias(parsedName);
    if (parsedName == "") {
        // S62 empty rel cannot have raw name
        queryRel->setRawName("annon_" + queryRel->getUniqueName());
    }
    else {
        queryRel->setRawName(parsedName);
    }

    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryRel});
    }
    queryGraph.addQueryRel(queryRel);
}

uint64_t Binder::bindQueryRelSchema(shared_ptr<RelExpression> queryRel,
                                    const RelPattern &relPattern,
                                    QueryGraph &queryGraph,
                                    PropertyKeyValCollection &collection)
{

    if (!queryRel->isSchemainfoBound()) {
        D_ASSERT(client != nullptr);

        vector<uint64_t> tableIDs;
        uint64_t univTableID;
        bindRelTableIDsFromPartitions(queryRel->getPartitionIDs(), tableIDs,
                                      univTableID);
        queryRel->pushBackTableIDs(tableIDs);
        univTableID = tableIDs[0];  // rel has no univ schema

        bool isVariableLength =
            queryRel->getLowerBound() != queryRel->getUpperBound() ? true
                                                                   : false;

        duckdb::string_vector *universal_schema;
        duckdb::idx_t_vector *universal_schema_ids;
        duckdb::LogicalTypeId_vector *universal_types_id;
        duckdb::PropertyToPropertySchemaPairVecUnorderedMap *property_schema_index;
        client->db->GetCatalogWrapper().GetPropertyKeyToPropertySchemaMap(
            *client, tableIDs, &property_schema_index, &universal_schema,
            &universal_schema_ids, &universal_types_id,
            queryRel->getPartitionIDs());
        {
            string propertyName = "_id";
            vector<Property> prop_id;
            for (auto &table_id : tableIDs) {
                prop_id.push_back(Property::constructNodeProperty(
                    PropertyNameDataType(propertyName, DataTypeID::NODE_ID), 0,
                    table_id));
                // TODO for variable length, id is not nodeid type. it is list!!
            }
            auto prop_idexpr =
                expressionBinder.createPropertyExpression(*queryRel, prop_id);
            queryRel->addPropertyExpression(propertyName,
                                            std::move(prop_idexpr));
        }

        // for each property, create property expression
        // for variable length join, cannot create property
        for (uint64_t i = 0; i < universal_schema->size(); i++) {
            // auto it = pkey_to_ps_map.find(universal_schema[i]);
            std::string propertyName = std::string(universal_schema->at(i));
            duckdb::idx_t property_key_id = universal_schema_ids->at(i);
            duckdb::LogicalTypeId property_key_type = universal_types_id->at(i);
            vector<Property> prop_id;
            if (isVariableLength && !(propertyName == "_sid" ||
                                      propertyName == "_tid")) {
                // when variable length, only fetch _sid and _tid, propery cannot be fetched
                continue;
            }
            auto it = property_schema_index->find(property_key_id);
            for (auto &tid_and_cid_pair : it->second) {
                // uint8_t duckdb_typeid = (uint8_t)std::get<2>(tid_and_cid_pair);
                DataTypeID kuzu_typeid = (DataTypeID)property_key_type;
                prop_id.push_back(Property::constructNodeProperty(
                    PropertyNameDataType(propertyName, kuzu_typeid),
                    tid_and_cid_pair.second + 1,
                    tid_and_cid_pair.first));
            }
            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryRel, prop_id, property_key_id);
            queryRel->addPropertyExpression(propertyName,
                                            std::move(prop_idexpr));
        }
        queryRel->markAllColumnsAsUsed();
        queryRel->setSchemainfoBound(true);
    }

    return queryRel->getTableIDs().size();
}

pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern &relPattern)
{
    auto lowerBound =
        min(TypeUtils::convertToUint32(relPattern.getLowerBound().c_str()),
            VAR_LENGTH_EXTEND_MAX_DEPTH);
    uint32_t upperBound;
    if (relPattern.getUpperBound() == "inf") {
        upperBound = -1;  // -1 reserved for inifnite loop
    }
    else {
        upperBound =
            min(TypeUtils::convertToUint32(relPattern.getUpperBound().c_str()),
                VAR_LENGTH_EXTEND_MAX_DEPTH);
        if (lowerBound > upperBound) {
            throw BinderException("Lower bound of rel " +
                                  relPattern.getVariableName() +
                                  " is greater than upperBound.");
        }
    }

    return make_pair(lowerBound, upperBound);
}

shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern &nodePattern, QueryGraph &queryGraph,
    PropertyKeyValCollection &collection)
{
    auto parsedName = nodePattern.getVariableName();
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.find(parsedName) !=
        variablesInScope.end()) {  // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto idProperty = queryNode->getPropertyExpression("_id"); // this may add unnessary _id
        // E.g. MATCH (a:person) MATCH (a:organisation)
        // We bind to single node a with both labels
        if (!nodePattern.getLabelOrTypeNames().empty()) {
            // S62 change table ids to relations
            assert(
                false);  // S62 logic is strange - may crash when considering schema.
            // auto otherTableIDs = std::move(bindNodeTableIDs(nodePattern.getLabelOrTypeNames()));
            // queryNode->addTableIDs(otherTableIDs);
        }
    }
    else {
        queryNode = createQueryNode(nodePattern);
    }

    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

uint64_t Binder::bindQueryNodeSchema(shared_ptr<NodeExpression> queryNode,
                                     const NodePattern &nodePattern,
                                     QueryGraph &queryGraph,
                                     PropertyKeyValCollection &collection,
                                     bool hasEdgeConnection)
{

    if (!queryNode->isSchemainfoBound()) {
        D_ASSERT(client != nullptr);

        vector<uint64_t> tableIDs;
        uint64_t univTableID;
        bindNodeTableIDsFromPartitions(queryNode->getPartitionIDs(), tableIDs,
                                       univTableID);
        if (tableIDs.size() == 1)
            univTableID = tableIDs[0];
        queryNode->pushBackTableIDs(tableIDs);
        queryNode->setUnivTableID(univTableID);

        // set tableIds
        queryNode->setInternalIDProperty(
            expressionBinder.createInternalNodeIDExpression(*queryNode));

        unordered_map<string,
                      vector<tuple<uint64_t, uint64_t, duckdb::LogicalTypeId>>>
            pkey_to_ps_map;
        duckdb::string_vector *universal_schema;
        duckdb::idx_t_vector *universal_schema_ids;
        duckdb::LogicalTypeId_vector *universal_types_id;
        duckdb::PropertyToPropertySchemaPairVecUnorderedMap *property_schema_index;
        client->db->GetCatalogWrapper().GetPropertyKeyToPropertySchemaMap(
            *client, tableIDs, &property_schema_index, &universal_schema,
            &universal_schema_ids, &universal_types_id,
            queryNode->getPartitionIDs());
        {
            string propertyName = "_id";
            vector<Property> prop_id;
            for (auto &table_id : tableIDs) {
                prop_id.push_back(Property::constructNodeProperty(
                    PropertyNameDataType(propertyName, DataTypeID::NODE_ID), 0,
                    table_id));
            }
            auto prop_idexpr =
                expressionBinder.createPropertyExpression(*queryNode, prop_id);
            queryNode->addPropertyExpression(propertyName,
                                             std::move(prop_idexpr));
            if (hasEdgeConnection)
                queryNode->markAllColumnsAsUsed();
        }

        // for each property, create property expression
        for (uint64_t i = 0; i < universal_schema->size(); i++) {
            std::string propertyName = std::string(universal_schema->at(i));
            duckdb::idx_t property_key_id = universal_schema_ids->at(i);
            duckdb::LogicalTypeId property_key_type = universal_types_id->at(i);
            auto it = property_schema_index->find(property_key_id);
            vector<Property> prop_id;
            for (auto &table_id_and_column_idx_pair : it->second) {
                DataTypeID kuzu_typeid = (DataTypeID)property_key_type;
                prop_id.push_back(Property::constructNodeProperty(
                    PropertyNameDataType(propertyName, kuzu_typeid),
                    table_id_and_column_idx_pair.second + 1,
                    table_id_and_column_idx_pair.first));
            }
            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryNode, prop_id, property_key_id);
            queryNode->addPropertyExpression(propertyName,
                                             std::move(prop_idexpr));
        }
        queryNode->setSchemainfoBound(true);
    }

    // bind for e.g. (a:P {prop: val}) -> why necessary?
    for (auto i = 0u; i < nodePattern.getNumPropertyKeyValPairs(); ++i) {
        const auto &propertyName = nodePattern.getProperty(i).first;
        const auto &rhs = nodePattern.getProperty(i).second;
        // refer binder and bind node property expression
        auto boundLhs = expressionBinder.bindNodePropertyExpression(
            *queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(
            boundRhs, boundLhs->dataType);
        collection.addPropertyKeyValPair(*queryNode,
                                         make_pair(boundLhs, boundRhs));
    }

    return queryNode->getTableIDs().size();
}

shared_ptr<NodeExpression> Binder::createQueryNode(
    const NodePattern &nodePattern)
{
    auto parsedName = nodePattern.getVariableName();

    vector<uint64_t> partitionIDs, tableIDs;
    bindNodePartitionIDs(nodePattern.getLabelOrTypeNames(), partitionIDs);
    auto queryNode = make_shared<NodeExpression>(
        getUniqueExpressionName(parsedName), partitionIDs, tableIDs);
    queryNode->setAlias(parsedName);
    if (parsedName == "") {
        // annon node cannot have raw name
        queryNode->setRawName("annon_" + queryNode->getUniqueName());
    }
    else {
        queryNode->setRawName(parsedName);
    }

    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryNode});
    }
    return queryNode;
}

void Binder::bindNodePartitionIDs(const vector<string> &tableNames,
                                  vector<uint64_t> &partitionIDs)
{
    D_ASSERT(client != nullptr);
    client->db->GetCatalogWrapper().GetPartitionIDs(
        *client, tableNames, partitionIDs, duckdb::GraphComponentType::VERTEX);
}

void Binder::bindNodeTableIDsFromPartitions(vector<uint64_t> &partitionIDs,
                                            vector<uint64_t> &tableIDs,
                                            uint64_t &univTableID)
{
    D_ASSERT(client != nullptr);
    client->db->GetCatalogWrapper().GetSubPartitionIDsFromPartitions(
        *client, partitionIDs, tableIDs, univTableID,
        duckdb::GraphComponentType::VERTEX);
}

// S62 access catalog and  change to mdids
void Binder::bindNodeTableIDs(const vector<string> &tableNames,
                              vector<uint64_t> &partitionIDs,
                              vector<uint64_t> &tableIDs)
{
    D_ASSERT(false);  // TODO 231120 deprecated
    // D_ASSERT(client != nullptr);
    // // TODO tablenames should be vector of vector considering the union over labelsets
    //     // e.g. (A:B | C:D) => [[A,B], [C,D]]

    // // syntax is strange. each tablename is considered intersection.
    // client->db->GetCatalogWrapper().GetSubPartitionIDs(*client, tableNames, partitionIDs, tableIDs, duckdb::GraphComponentType::VERTEX);
}

// S62 access catalog and  change to mdids
void Binder::bindRelTableIDs(const vector<string> &tableNames,
                             const shared_ptr<NodeExpression> &srcNode,
                             const shared_ptr<NodeExpression> &dstNode,
                             vector<uint64_t> &partitionIDs,
                             vector<uint64_t> &tableIDs)
{
    D_ASSERT(client != nullptr);

    // if empty, return all edges
    // otherwise, union table of all edges
    // this is an union semantics
    vector<uint64_t> dstPartitionIDs;
    if (tableNames.size() == 0) {
        // get edges that connected with srcNode
        client->db->GetCatalogWrapper().GetConnectedEdgeSubPartitionIDs(
            *client, srcNode->getPartitionIDs(), partitionIDs, tableIDs,
            dstPartitionIDs);
    }
    else {
        client->db->GetCatalogWrapper().GetSubPartitionIDs(
            *client, tableNames, partitionIDs, tableIDs,
            duckdb::GraphComponentType::EDGE);
    }
}

void Binder::bindRelTableIDsFromPartitions(vector<uint64_t> &partitionIDs,
                                           vector<uint64_t> &tableIDs,
                                           uint64_t &univTableID)
{
    D_ASSERT(client != nullptr);
    client->db->GetCatalogWrapper().GetSubPartitionIDsFromPartitions(
        *client, partitionIDs, tableIDs, univTableID,
        duckdb::GraphComponentType::EDGE);
}

void Binder::bindRelPartitionIDs(const vector<string> &tableNames,
                                 const shared_ptr<NodeExpression> &srcNode,
                                 const shared_ptr<NodeExpression> &dstNode,
                                 vector<uint64_t> &partitionIDs)
{
    D_ASSERT(client != nullptr);
    vector<uint64_t> srcPartitionIDs, dstPartitionIDs;
    if (tableNames.size() == 0) {
        // get edges that connected with srcNode
        client->db->GetCatalogWrapper().GetConnectedEdgeSubPartitionIDs(
            *client, srcNode->getPartitionIDs(), partitionIDs, dstPartitionIDs);

        // prune unnecessary partition IDs
        vector<uint64_t> new_dstPartitionIDs;
        auto &cur_dstPartitionIDs = dstNode->getPartitionIDs();
        std::set_intersection(cur_dstPartitionIDs.begin(),
                              cur_dstPartitionIDs.end(),
                              dstPartitionIDs.begin(), dstPartitionIDs.end(),
                              std::back_inserter(new_dstPartitionIDs));
        std::swap(new_dstPartitionIDs, dstNode->getPartitionIDs());
    }
    else {
        client->db->GetCatalogWrapper().GetEdgeAndConnectedSrcDstPartitionIDs(
            *client, tableNames, partitionIDs, srcPartitionIDs, dstPartitionIDs,
            duckdb::GraphComponentType::EDGE);

        // prune unnecessary partition IDs
        vector<uint64_t> new_srcPartitionIDs;
        auto &cur_srcPartitionIDs = srcNode->getPartitionIDs();
        std::set_intersection(cur_srcPartitionIDs.begin(),
                              cur_srcPartitionIDs.end(),
                              srcPartitionIDs.begin(), srcPartitionIDs.end(),
                              std::back_inserter(new_srcPartitionIDs));
        std::swap(new_srcPartitionIDs, srcNode->getPartitionIDs());

        vector<uint64_t> new_dstPartitionIDs;
        auto &cur_dstPartitionIDs = dstNode->getPartitionIDs();
        std::set_intersection(cur_dstPartitionIDs.begin(),
                              cur_dstPartitionIDs.end(),
                              dstPartitionIDs.begin(), dstPartitionIDs.end(),
                              std::back_inserter(new_dstPartitionIDs));
        std::swap(new_dstPartitionIDs, dstNode->getPartitionIDs());
    }
}

}  // namespace binder
}  // namespace kuzu
