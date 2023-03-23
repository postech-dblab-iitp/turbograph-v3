#pragma once

#include "gpos/memory/CMemoryPool.h"

#include "kuzu/common/types/types.h"
#include "kuzu/common/types/literal.h"

#include "common/assert.hpp"
#include "common/types.hpp"
#include "types/value.hpp"
#include "common/types/string_type.hpp"

#include <cstdlib>


namespace s62 {

class DatumSerDes {

// The two functions should be exactly opposite.
public:

	// kuzu literal -> in mp
	// TODO may need to apply templates if necessary
	static void SerializeKUZULiteralIntoOrcaByteArray(uint32_t type_id, kuzu::common::Literal* kuzu_literal, void*& out_mem_ptr, uint64_t& out_length) {

		DataType kuzu_type = kuzu_literal->dataType;
		D_ASSERT( type_id == LOGICAL_TYPE_BASE_ID + (OID)kuzu_type.typeID);
		
		switch(kuzu_type.typeID) {
			case DataTypeID::INT64: {
				out_length = 8;
				int64_t* mem_ptr = (int64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.int64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::UBIGINT: {
				out_length = 8;
				uint64_t* mem_ptr = (uint64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.uint64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::STRING: {
				out_length = strlen(kuzu_literal->strVal.c_str())+1;	// add null term
				char* mem_ptr = (char*) malloc(out_length);
				memcpy(mem_ptr, (char*)kuzu_literal->strVal.c_str(), out_length);
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			default:
				D_ASSERT(false);
		}
		return;
	}

	// datum mp -> duckdb value
		// TODO api should not copy return value
	static duckdb::Value DeserializeOrcaByteArrayIntoDuckDBValue(uint32_t type_id, const void* mem_ptr, uint64_t length) {

		duckdb::LogicalTypeId duckdb_type = (duckdb::LogicalTypeId) (type_id - LOGICAL_TYPE_BASE_ID);

		// TODO deallocation is responsible here
		switch(duckdb_type) {
			case duckdb::LogicalType::UBIGINT: {
				D_ASSERT(length == 8);
				uint64_t value = *((uint64_t*)mem_ptr);
				return duckdb::Value::UBIGINT(value);
			}
			case duckdb::LogicalType::BIGINT: {
				D_ASSERT(length == 8);
				int64_t value = *((int64_t*)mem_ptr);
				return duckdb::Value::UBIGINT((uint64_t)value);	// TODO to BIGINT
			}
			case duckdb::LogicalType::VARCHAR: {
				string str_value((char*)mem_ptr);
				duckdb::string_t value(str_value);
				return duckdb::Value(value);
			}
			default:
				D_ASSERT(false);
		}
		

		
	}


};


}