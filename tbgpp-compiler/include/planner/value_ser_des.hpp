#pragma once

#include "gpos/memory/CMemoryPool.h"

#include "kuzu/common/types/types.h"
#include "kuzu/common/types/literal.h"

#include "common/assert.hpp"

#include "common/assert.h"
#include "types/value.hpp"

#include <cstdlib>


namespace s62 {

class DatumSerDes {

// The two functions should be exactly opposite.
public:

	// kuzu literal -> in mp
	// TODO may need to apply templates if necessary
	static void SerializeKUZULiteralIntoOrcaByteArray(uint32_t type_id, kuzu::common::Literal* kuzu_literal, void*& out_mem_ptr, uint64_t& out_length) {

		DataType kuzu_type = kuzu_literal->dataType;
		D_ASSERT( type_id ==LOGICAL_TYPE_BASE_ID + (OID)kuzu_type.typeID);
		
		switch(kuzu_type.typeID) {
			case DataTypeID::INT64: {
				out_length = 8;
				int64_t* mem_ptr = (int64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.int64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			default:
				D_ASSERT(false);
		}
		return;
	}

	// datum mp -> duckdb value
	static void DeserializeOrcaByteArrayIntoDuckDBValue(void* mem_ptr, uint64_t length, duckdb::Value out_value) {

	}


};


}