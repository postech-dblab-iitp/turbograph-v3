#pragma once

#include "gpos/memory/CMemoryPool.h"

#include "kuzu/common/types/types.h"
#include "kuzu/common/types/literal.h"

#include "common/assert.hpp"
#include "common/types.hpp"
#include "common/types/value.hpp"
#include "common/types/string_type.hpp"

#include <cstdlib>


namespace s62 {

class DatumSerDes {

// The two functions should be exactly opposite.
public:

	// kuzu literal -> in mp
	// TODO may need to apply templates if necessary
	static void SerializeKUZULiteralIntoOrcaByteArray(uint32_t type_id, kuzu::common::Literal* kuzu_literal, void*& out_mem_ptr, uint64_t& out_length) {

		// DataType kuzu_type = kuzu_literal->dataType;
		// D_ASSERT( type_id == LOGICAL_TYPE_BASE_ID + (OID)kuzu_type.typeID);
		DataTypeID data_type_id = DataTypeID((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);

		switch (data_type_id) {
			case DataTypeID::INTEGER: {
				out_length = 4;
				int32_t* mem_ptr = (int32_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.int32Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::INT64: {
				out_length = 8;
				int64_t* mem_ptr = (int64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.int64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::UINTEGER: {
				out_length = 4;
				uint32_t* mem_ptr = (uint32_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.uint32Val;
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
			case DataTypeID::HUGEINT: {
				// jhha: currently, think hugeint as int64
				out_length = 8;
				int64_t* mem_ptr = (int64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.uint64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::DOUBLE: {
				out_length = 8;
				double* mem_ptr = (double*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.doubleVal;
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
			case DataTypeID::BOOLEAN: {
				out_length = 1;
				int8_t val = kuzu_literal->val.booleanVal ? 1 : 0;
				char *mem_ptr = (char*) malloc(out_length);
				(*mem_ptr) = val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::DATE: {
				out_length = 4;
				int32_t val = kuzu_literal->val.dateVal.days;
				int32_t *mem_ptr = (int32_t *)malloc(out_length);
				(*mem_ptr) = val;
				out_mem_ptr = (void *)mem_ptr;
				break;
			}
			case DataTypeID::DECIMAL: { // TODO decimal temporary
				out_length = 8;
				int64_t* mem_ptr = (int64_t*) malloc(out_length);
				(*mem_ptr) = kuzu_literal->val.int64Val;
				out_mem_ptr = (void*)mem_ptr;
				break;
			}
			case DataTypeID::LIST: {
				switch (kuzu_literal->dataType.childType->typeID) {
				case DataTypeID::STRING: {
					uint32_t num_strings = kuzu_literal->listVal.size();
					out_length += sizeof(uint32_t);
					for (auto i = 0; i < num_strings; i++) {
						out_length += sizeof(uint32_t);
						out_length += kuzu_literal->listVal[i].strVal.size();
					}
					char *mem_ptr = (char *)malloc(out_length);

					uint32_t accm_bytes = 0;
					memcpy(mem_ptr, &num_strings, sizeof(uint32_t));
					accm_bytes += sizeof(uint32_t);
					for (auto i = 0; i < num_strings; i++) {
						uint32_t str_size = kuzu_literal->listVal[i].strVal.size();
						memcpy(mem_ptr + accm_bytes, &str_size, sizeof(uint32_t));
						accm_bytes += sizeof(uint32_t);
						memcpy(mem_ptr + accm_bytes, (char *)kuzu_literal->listVal[i].strVal.c_str(), str_size);
						accm_bytes += str_size;
					}
					out_mem_ptr = (void *)mem_ptr;
					break;
				}
				default:
					D_ASSERT(false);
				}
				break;
			}
			default:
				D_ASSERT(false);
		}
		return;
	}

	// datum mp -> duckdb value
		// TODO api should not copy return value
	static duckdb::Value DeserializeOrcaByteArrayIntoDuckDBValue(uint32_t type_id, int32_t type_modifier, const void* mem_ptr, uint64_t length) {
		duckdb::LogicalTypeId duckdb_type = (duckdb::LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);

		// TODO deallocation is responsible here
		switch (duckdb_type) {
			case duckdb::LogicalTypeId::INTEGER: {
				D_ASSERT(length == 4 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::INTEGER);	// TODO to BIGINT
				} else {
					int32_t value = *((int32_t*)mem_ptr);
					return duckdb::Value::INTEGER((int32_t)value);	// TODO to BIGINT
				}
			}
			case duckdb::LogicalTypeId::BIGINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					// return duckdb::Value(duckdb::LogicalType::UBIGINT);	// TODO to BIGINT
					return duckdb::Value(duckdb::LogicalType::BIGINT);	// TODO to BIGINT
				} else {
					int64_t value = *((int64_t*)mem_ptr);
					// return duckdb::Value::UBIGINT((uint64_t)value);	// TODO to BIGINT
					return duckdb::Value::BIGINT((uint64_t)value);	// TODO to BIGINT
				}
			}
			case duckdb::LogicalTypeId::UINTEGER: {
				D_ASSERT(length == 4 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::UINTEGER);
				} else {
					uint32_t value = *((uint32_t*)mem_ptr);
					return duckdb::Value::UINTEGER((uint32_t)value);
				}
			}
			case duckdb::LogicalTypeId::UBIGINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::UBIGINT);
				} else {
					uint64_t value = *((uint64_t*)mem_ptr);
					return duckdb::Value::UBIGINT(value);
				}
			}
			case duckdb::LogicalTypeId::DOUBLE: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::DOUBLE);
				} else {
					double value = *((double*)mem_ptr);
					return duckdb::Value::DOUBLE(value);
				}
			}
			case duckdb::LogicalTypeId::HUGEINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::HUGEINT);
				} else {
					uint64_t value = *((int64_t*)mem_ptr);
					return duckdb::Value::HUGEINT(value);
				}
			}
			case duckdb::LogicalTypeId::VARCHAR: {
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::VARCHAR);
				} else {
					string str_value((char*)mem_ptr);
					duckdb::string_t value(str_value);
					return duckdb::Value(value);
				}
			}
			case duckdb::LogicalTypeId::ID: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::ID);
				} else {
					uint64_t value = *((uint64_t*)mem_ptr);
					return duckdb::Value::ID(value);
				}
			}
			case duckdb::LogicalTypeId::BOOLEAN: {
				int8_t value = *((int8_t*)mem_ptr);
				return duckdb::Value::BOOLEAN(value);
			}
			case duckdb::LogicalTypeId::DATE: {
				int32_t value = *((int32_t *)mem_ptr);
				return duckdb::Value::DATE(duckdb::date_t(value));
			}
			case duckdb::LogicalTypeId::DECIMAL: { // TODO decimal temporary
				int64_t value = *((int64_t *)mem_ptr);
				return duckdb::Value::DECIMAL(value, 12, 2);
			}
			case duckdb::LogicalTypeId::PATH: {
				return duckdb::Value::LIST(duckdb::LogicalTypeId::UBIGINT, {});
			}
			case duckdb::LogicalTypeId::LIST: {
				if (length == 0) {
					D_ASSERT(type_modifier >= std::numeric_limits<uint8_t>::min() && type_modifier <= std::numeric_limits<uint8_t>::max());
					duckdb::LogicalTypeId child_type_id = (duckdb::LogicalTypeId)type_modifier;
					return duckdb::Value::LIST(child_type_id, {});
				} else {
					D_ASSERT(type_modifier >= std::numeric_limits<uint8_t>::min() && type_modifier <= std::numeric_limits<uint8_t>::max());
					duckdb::LogicalTypeId child_type_id = (duckdb::LogicalTypeId)type_modifier;
					char *str_ptr = (char *)mem_ptr;
					switch(child_type_id) {
					case duckdb::LogicalTypeId::VARCHAR: {
						uint32_t num_strings = *((uint32_t *)str_ptr);
						vector<duckdb::Value> list_values;
						uint32_t accm_bytes = sizeof(uint32_t);
						for (auto i = 0; i < num_strings; i++) {
							uint32_t str_size = *((uint32_t*)(str_ptr + accm_bytes));
							accm_bytes += sizeof(uint32_t);
							string str_value((char*)(str_ptr + accm_bytes), str_size);
							duckdb::string_t value(str_value);
							list_values.push_back(duckdb::Value(value));
							accm_bytes += str_size;
						}
						return duckdb::Value::LIST(duckdb::LogicalTypeId::VARCHAR, list_values);
					}
					default:
						D_ASSERT(false);
					}
				}
			}
			default:
				D_ASSERT(false);
		}
		

		
	}


};


}