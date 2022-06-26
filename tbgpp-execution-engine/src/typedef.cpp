#include "typedef.hpp"
#include <iostream>
#include <algorithm>
#include <map>

void LabelSet::insert(std::string input) {
	this->data.insert(input);
}

bool LabelSet::isSupersetOf(const LabelSet& elem) {

	// if size bigger, always false
	if( elem.data.size() > this->data.size() ) return false;
	// if same or small, check if all members exist.
	for( const auto& item: elem.data ) {
		if( this->data.find(item) == this->data.end() ) {
			return false;
		}
	}
	return true;
}

bool LabelSet::contains(const std::string st) {
	return this->data.find(st) != this->data.end();
}

std::ostream& operator<<(std::ostream& os, const LabelSet& obj) {
	os << "LabelSet(";
	for(auto item: obj.data) {
		os << item << ",";
	}
	os << ")";
    return os;
}

bool operator==(const LabelSet lhs, const LabelSet rhs) {
	// This is a comparison between 'unordered' sets, thus different ordering will return true.
	return lhs.data == rhs.data;
}

std::vector<duckdb::LogicalType> CypherSchema::getTypes() const {
	std::vector<duckdb::LogicalType> result;
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::DATA:
			case CypherValueType::ID:
			case CypherValueType::ADJLIST: {
				result.push_back( std::get<2>(attr) );
				break;
			}
			case CypherValueType::NODE:
			case CypherValueType::EDGE:
			case CypherValueType::PATH: {
				// recursive seek
				std::vector<duckdb::LogicalType> innerResult = nestedAttrs.find(name)->second.getTypes();
				for( auto& r: innerResult ) {
					result.push_back(r);
				}
				break;
			}
			default: {
				assert( 0 && "wrong type");
			}	
		}
	}
	return result;
}

void CypherSchema::addNode(std::string name, LoadAdjListOption adjOption) {
	
	CypherSchema nodeSchema;
	nodeSchema.attrs.push_back(
		std::make_tuple("_id", CypherValueType::ID, duckdb::LogicalType::UBIGINT)
	);
	switch(adjOption) {
		case LoadAdjListOption::NONE:
			break;
		case LoadAdjListOption::BOTH: {
			// outgoing first 
			nodeSchema.attrs.push_back(
				std::make_tuple("_adj_out", CypherValueType::ADJLIST, duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT))
			);
			nodeSchema.attrs.push_back(
				std::make_tuple("_adj_in", CypherValueType::ADJLIST, duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT))
			);
			
			break;
		}
		case LoadAdjListOption::INCOMING: {
			nodeSchema.attrs.push_back(
				std::make_tuple("_adj_in", CypherValueType::ADJLIST, duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT))
			);
			break;
		}
		case LoadAdjListOption::OUTGOING: {
			nodeSchema.attrs.push_back(
				std::make_tuple("_adj_out", CypherValueType::ADJLIST, duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT))
			);
			break;
		}
	}
	// set node info on attrs
	attrs.push_back( std::make_tuple(name, CypherValueType::NODE, duckdb::LogicalType::INVALID) );
	// set node details
	nestedAttrs[name] = nodeSchema;
}

void CypherSchema::addPropertyIntoNode(std::string nodeName, std::string propName, duckdb::LogicalType type) {
	nestedAttrs[nodeName].attrs.push_back(
		std::make_tuple(propName, CypherValueType::DATA, type)
	);
}

void CypherSchema::addColumn(std::string attrName, duckdb::LogicalType type) {
	attrs.push_back( std::make_tuple(attrName, CypherValueType::DATA, type) );
}

int CypherSchema::getNodeColIdx(std::string name) const {

	// TODO FIXME fix immediately!!! write here.
	// TODO DINNER

	return 0;

}

std::string CypherSchema::toString() {

	std::string result;
	result += "(";
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		result += name;
		result += ":";
		// Type (recurse)
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::DATA: {
				duckdb::LogicalType dataType = std::get<2>(attr);
				result += dataType.ToString();
				break;
			}
			case CypherValueType::ID: {
				result += "ID";
				break;
			}
			case CypherValueType::ADJLIST: {
				result += "ADJLIST";
				break;
			}
			case CypherValueType::NODE: {
				result += "NODE";
				result += nestedAttrs[name].toString();
				break;
			}
			case CypherValueType::EDGE: {
				result += "EDGE";
				result += nestedAttrs[name].toString();
				break;
			}
			case CypherValueType::PATH: {
				assert(0);
				result += "PATH"; break;
			}
		}
		result += ", ";
	}
	result.pop_back(); result.pop_back(); // delete last comma
	result += ")";
	return result;
}