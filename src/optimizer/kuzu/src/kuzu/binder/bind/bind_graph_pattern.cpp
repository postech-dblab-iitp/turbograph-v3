#include <set>

#include "kuzu/binder/binder.h"
#include "catalog/catalog_wrapper.hpp"
#include "common/tuple.hpp"
#include "kuzu/common/type_utils.h"
#include "common/boost_typedefs.hpp"

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

    if (queryGraph->getQueryGraphType() != QueryGraphType::NONE) {
        string pathName = patternElement.getPathName();
        queryGraph->setQueryPath(pathName);

        if (variablesInScope.find(pathName) != variablesInScope.end()) {
            auto prevVariable = variablesInScope.at(pathName);
            ExpressionBinder::validateExpectedDataType(*prevVariable, DataTypeID::PATH);
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
        ExpressionBinder::validateExpectedDataType(*prevVariable, DataTypeID::REL);
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
    vector<uint64_t> partitionIDs;
    bindRelPartitionIDs(relPattern.getLabelOrTypeNames(), srcNode, dstNode,
                        partitionIDs);

    // bind variable length
    auto boundPair = bindVariableLengthRelBound(relPattern);
    auto &lowerBound = boundPair.first;
    auto &upperBound = boundPair.second;
    bool isVariableLength = lowerBound != upperBound ? true : false;
    auto queryRel = make_shared<RelExpression>(
        getUniqueExpressionName(parsedName), partitionIDs, srcNode,
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
        vector<size_t> numTablesPerPartition;
        client->db->GetCatalogWrapper().GetSubPartitionIDsFromPartitions(
            *client, queryRel->getPartitionIDs(), tableIDs, numTablesPerPartition,
            duckdb::GraphComponentType::EDGE, true);

        queryRel->addTableIDs(tableIDs, numTablesPerPartition);

        bool isVariableLength =
            queryRel->getLowerBound() != queryRel->getUpperBound() ? true
                                                                   : false;

        vector<duckdb::idx_t> universal_schema_ids;
        vector<duckdb::LogicalTypeId> universal_types_id;
        unordered_map<duckdb::idx_t, vector<std::pair<uint64_t, uint64_t>>> property_schema_index;

        duckdb::GraphCatalogEntry *graph_catalog_entry =
            (duckdb::GraphCatalogEntry *)client->db->GetCatalog().GetEntry(
                *client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
                DEFAULT_GRAPH);

        client->db->GetCatalogWrapper().GetPropertyKeyToPropertySchemaMap(
            *client, property_schema_index, universal_schema_ids,
            universal_types_id, queryRel->getPartitionIDs());
        {
            string propertyName = "_id";
            unordered_map<table_id_t, property_id_t> propertyIDPerTable;
            propertyIDPerTable.reserve(tableIDs.size());
            
            Property anchorProperty;
            anchorProperty.name = std::to_string(0);
            anchorProperty.dataType = DataType(DataTypeID::NODE_ID);
            
            for (auto &table_id : tableIDs) {
                propertyIDPerTable.insert({table_id, 0});
            }
            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryRel, anchorProperty, propertyIDPerTable);
            uint64_t propertyKeyID = graph_catalog_entry->GetPropertyKeyID(*client, propertyName);
            queryRel->addPropertyExpression(propertyKeyID,
                                            std::move(prop_idexpr));
        }

        // for each property, create property expression
        // for variable length join, cannot create property
        for (uint64_t i = 0; i < universal_schema_ids.size(); i++) {
            duckdb::idx_t property_key_id = universal_schema_ids.at(i);
            duckdb::LogicalTypeId property_key_type = universal_types_id.at(i);
            // vector<Property> prop_id;
            unordered_map<table_id_t, property_id_t> propertyIDPerTable;
            propertyIDPerTable.reserve(tableIDs.size());

            Property anchorProperty;
            anchorProperty.name = std::to_string(property_key_id);
            anchorProperty.dataType = DataType((DataTypeID)property_key_type);

            std::string propertyName = graph_catalog_entry->GetPropertyName(*client, property_key_id);
            if (isVariableLength && !(propertyName == "_sid" ||
                                      propertyName == "_tid")) {
                // when variable length, only fetch _sid and _tid, propery cannot be fetched
                continue;
            }
            auto it = property_schema_index.find(property_key_id);
            for (auto &tid_and_cid_pair : it->second) {
                // uint8_t duckdb_typeid = (uint8_t)std::get<2>(tid_and_cid_pair);
                DataTypeID kuzu_typeid = (DataTypeID)property_key_type;
                D_ASSERT(anchorProperty.dataType.typeID == kuzu_typeid);
                // prop_id.push_back(Property::constructNodeProperty(
                //     PropertyNameDataType(propertyName, kuzu_typeid),
                //     tid_and_cid_pair.second + 1,
                //     tid_and_cid_pair.first));
                propertyIDPerTable.insert(
                    {tid_and_cid_pair.first, tid_and_cid_pair.second + 1});
            }
            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryRel, anchorProperty, propertyIDPerTable, property_key_id);
            // uint64_t propertyKeyID = graph_catalog_entry->GetPropertyKeyID(*client, propertyName);
            queryRel->addPropertyExpression(property_key_id,
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
        ExpressionBinder::validateExpectedDataType(*prevVariable, DataTypeID::NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto idProperty = queryNode->getPropertyExpression(INTERNAL_ID_PROPERTY_KEY_ID); // this may add unnessary _id
        // E.g. MATCH (a:person) MATCH (a:organisation)
        // We bind to single node a with both labels
        if (!nodePattern.getLabelOrTypeNames().empty()) {
            // S62 change table ids to relations
            assert(
                false);  // S62 logic is strange - may crash when considering schema.
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
        vector<size_t> numTablesPerPartition;
        client->db->GetCatalogWrapper().GetSubPartitionIDsFromPartitions(
            *client, queryNode->getPartitionIDs(), tableIDs, numTablesPerPartition,
            duckdb::GraphComponentType::VERTEX, true);

        queryNode->addTableIDs(tableIDs, numTablesPerPartition);
        queryNode->setInternalIDProperty(
            expressionBinder.createInternalNodeIDExpression(*queryNode));

        unordered_map<string,
                      vector<tuple<uint64_t, uint64_t, duckdb::LogicalTypeId>>>
            pkey_to_ps_map;
        vector<duckdb::idx_t> universal_schema_ids; 
        vector<duckdb::LogicalTypeId> universal_types_id;
        unordered_map<duckdb::idx_t, vector<std::pair<uint64_t, uint64_t>>> property_schema_index;
        duckdb::GraphCatalogEntry *graph_catalog_entry =
            (duckdb::GraphCatalogEntry *)client->db->GetCatalog().GetEntry(
                *client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
                DEFAULT_GRAPH);

        client->db->GetCatalogWrapper().GetPropertyKeyToPropertySchemaMap(
            *client, property_schema_index, universal_schema_ids,
            universal_types_id, queryNode->getPartitionIDs());
        {
            string propertyName = "_id";
            unordered_map<table_id_t, property_id_t> propertyIDPerTable;
            propertyIDPerTable.reserve(tableIDs.size());

            Property anchorProperty;
            anchorProperty.name = std::to_string(0);
            anchorProperty.dataType = DataType(DataTypeID::NODE_ID);
            
            for (auto &table_id : tableIDs) {
                propertyIDPerTable.insert({table_id, 0});
            }
            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryNode, anchorProperty, propertyIDPerTable);
            uint64_t propertyKeyID = graph_catalog_entry->GetPropertyKeyID(*client, propertyName);
            queryNode->addPropertyExpression(propertyKeyID,
                                             std::move(prop_idexpr));
            if (hasEdgeConnection)
                queryNode->markAllColumnsAsUsed();
        }

        // for each property, create property expression
        for (uint64_t i = 0; i < universal_schema_ids.size(); i++) {
            duckdb::idx_t property_key_id = universal_schema_ids.at(i);
            duckdb::LogicalTypeId property_key_type = universal_types_id.at(i);
            auto it = property_schema_index.find(property_key_id);
            unordered_map<table_id_t, property_id_t> propertyIDPerTable;
            propertyIDPerTable.reserve(tableIDs.size());

            Property anchorProperty;
            anchorProperty.name = std::to_string(property_key_id);
            anchorProperty.dataType = DataType((DataTypeID)property_key_type);

            for (auto &table_id_and_column_idx_pair : it->second) {
                DataTypeID kuzu_typeid = (DataTypeID)property_key_type;
                D_ASSERT(anchorProperty.dataType.typeID == kuzu_typeid);
                propertyIDPerTable.insert(
                    {table_id_and_column_idx_pair.first,
                     table_id_and_column_idx_pair.second + 1});
            }

            auto prop_idexpr = expressionBinder.createPropertyExpression(
                *queryNode, anchorProperty, propertyIDPerTable, property_key_id);
            queryNode->addPropertyExpression(property_key_id,
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

    vector<uint64_t> partitionIDs;
    bindNodePartitionIDs(nodePattern.getLabelOrTypeNames(), partitionIDs);
    auto queryNode = make_shared<NodeExpression>(
        getUniqueExpressionName(parsedName), partitionIDs);
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
