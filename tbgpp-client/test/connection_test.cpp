#include <iostream>
#include "connection/s62_connection.hpp"

int main(int argc, char** argv) {
    std::string workspace("/data");
    duckdb::S62Connection conn(workspace);

    std::cout << "\n<<<Node metadata>>>" << std::endl;
    
    duckdb::NodeMetadataList node_metadata_list;
    conn.GetNodesMetadata(node_metadata_list);

    // Print node metadata
    for (auto& node_metadata: node_metadata_list) {
        std::cout << "Label name: " << node_metadata.label_name << std::endl;
        std::cout << "Property names: ";
        for (auto& property_name: node_metadata.property_names) {
            std::cout << property_name << " ";
        }
        std::cout << std::endl;
        std::cout << "Property types: ";
        for (auto& property_type: node_metadata.property_types) {
            std::cout << property_type << " ";
        }
        std::cout << std::endl << std::endl;
    }

    std::cout << "<<<Edge metadata>>>" << std::endl;

    duckdb::EdgeMetadataList edge_metadata_list;
    conn.GetEdgesMetadata(edge_metadata_list);

    // Print edge metadata
    for (auto& edge_metadata: edge_metadata_list) {
        std::cout << "Type name: " << edge_metadata.type_name << std::endl;
        std::cout << "Property names: ";
        for (auto& property_name: edge_metadata.property_names) {
            std::cout << property_name << " ";
        }
        std::cout << std::endl;
        std::cout << "Property types: ";
        for (auto& property_type: edge_metadata.property_types) {
            std::cout << property_type << " ";
        }
        std::cout << std::endl << std::endl;
    }

    std::cout << "<<<Query result set metadata>>>" << std::endl;

    duckdb::QueryResultSetMetadata result_set_metadata;
    std::string query("MATCH (n:Person {id: 65})-[r:IS_LOCATED_IN]->(p:Place) \
		   RETURN \
		   	n.firstName AS firstName, \
			n.lastName AS lastName, \
			n.birthday AS birthday, \
			n.locationIP AS locationIP, \
			n.browserUsed AS browserUsed, \
			p.id AS cityId, \
			n.gender AS gender, \
			n.creationDate AS creationDate;");
    conn.GetQueryResultsMetadata(query, result_set_metadata);

    // Print column names
    std::cout << "Column names: ";
    for (auto& column_name: result_set_metadata.property_names) {
        std::cout << column_name << " ";
    }
    std::cout << std::endl;

    // Pritn column types
    std::cout << "Column types: ";
    for (auto& column_type: result_set_metadata.property_types) {
        std::cout << column_type << " ";
    }
    std::cout << std::endl << std::endl;;


    return 0;
}