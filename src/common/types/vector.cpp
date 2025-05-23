#include "common/types/vector.hpp"

#include "common/algorithm.hpp"
#include "common/assert.hpp"
#include "common/exception.hpp"
#include "common/operator/comparison_operators.hpp"
#include "common/pair.hpp"
//#include "common/printer.hpp"
//#include "common/serializer.hpp"
#include "common/types/null_value.hpp"
#include "common/types/sel_cache.hpp"
#include "common/types/vector_cache.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/typedef.hpp"
//#include "storage/buffer/buffer_handle.hpp"

#include <cstring> // strlen() on Solaris
#include "icecream.hpp"

namespace duckdb {

Vector::Vector(LogicalType type_p, bool create_data, bool zero_data, idx_t capacity)
    : vector_type(VectorType::FLAT_VECTOR), type(move(type_p)), data(nullptr), capacity(capacity) {
	if (create_data) {
		Initialize(zero_data, capacity);
	}
}

Vector::Vector(LogicalType type_p, idx_t capacity) : Vector(move(type_p), true, false, capacity) {
	this->capacity = capacity;
}

Vector::Vector(LogicalType type_p, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(move(type_p)), data(dataptr) {
	D_ASSERT(!(dataptr && type.id() == LogicalTypeId::INVALID));
	// if (dataptr && type.id() == LogicalTypeId::INVALID) {
	// 	throw InternalException("Cannot create a vector of type INVALID!");
	// }
}

Vector::Vector(const VectorCache &cache, idx_t capacity) : type(cache.GetType()), capacity(capacity) {
	ResetFromCache(cache);
}

Vector::Vector(Vector &other) : type(other.type) {
	Reference(other);
}

Vector::Vector(Vector &other, const SelectionVector &sel, idx_t count) : type(other.type) {
	Slice(other, sel, count);
}

Vector::Vector(Vector &other, idx_t offset) : type(other.type) {
	Slice(other, offset);
}

// Vector::Vector(const Value &value) : type(value.type()) {
Vector::Vector(const Value &value) : type(value.type()), capacity(STANDARD_VECTOR_SIZE) {
	Reference(value);
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(move(other.type)), data(other.data), validity(move(other.validity)),
      buffer(move(other.buffer)), auxiliary(move(other.auxiliary)), capacity(other.capacity) {
}

void Vector::Reference(const Value &value) {
	D_ASSERT( (GetType().id()==LogicalTypeId::ID) || (value.type().id()==LogicalTypeId::ID) || ( GetType().id() == value.type().id() ) );
		// TODO bypassing same logical value checks on Logicaltypd::ID

	this->vector_type = VectorType::CONSTANT_VECTOR;
	buffer = VectorBuffer::CreateConstantVector(value.type());
	auto internal_type = value.type().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_unique<VectorStructBuffer>();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_vectors = struct_buffer->GetChildren();
		auto &value_children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto vector = make_unique<Vector>(value.IsNull() ? Value(child_types[i].second) : value_children[i]);
			child_vectors.push_back(move(vector));
		}
		auxiliary = move(struct_buffer);
		if (value.IsNull()) {
			SetValue(0, value);
		}
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_unique<VectorListBuffer>(value.type());
		auxiliary = move(list_buffer);
		data = buffer->GetData();
		SetValue(0, value);
	} else {
		auxiliary.reset();
		data = buffer->GetData();
		// std::memset(data, 0, STANDARD_VECTOR_SIZE * GetTypeIdSize(value.type().InternalType())); // TODO super overhead
		SetValue(0, value);
	}
}

void Vector::Reference(Vector &other) {
	// D_ASSERT(other.GetType() == GetType());
	// D_ASSERT(other.GetType().InternalType() == GetType().InternalType()); // S62 maybe Reference can be processed when physical type is same..
	D_ASSERT(other.GetType().InternalType() == GetType().InternalType()
		|| other.GetType() == LogicalType::SQLNULL);
	Reinterpret(other);
}

void Vector::ReferenceAndSetType(Vector &other) {
	type = other.GetType();
	Reference(other);
}

void Vector::Reinterpret(Vector &other) {
	vector_type = other.vector_type;
	AssignSharedPointer(buffer, other.buffer);
	AssignSharedPointer(auxiliary, other.auxiliary);
	data = other.data;
	validity = other.validity;
	capacity = other.capacity;
	is_valid = other.is_valid;
	rowcol_idx = other.rowcol_idx;
}

void Vector::ResetFromCache(const VectorCache &cache) {
	cache.ResetFromCache(*this);
}

void Vector::Slice(Vector &other, idx_t offset) {
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR);

	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		Vector new_vector(GetType());
		auto &entries = StructVector::GetEntries(new_vector);
		auto &other_entries = StructVector::GetEntries(other);
		D_ASSERT(entries.size() == other_entries.size());
		for (idx_t i = 0; i < entries.size(); i++) {
			entries[i]->Slice(*other_entries[i], offset);
		}
		if (offset > 0) {
			new_vector.validity.Slice(other.validity, offset);
		} else {
			new_vector.validity = other.validity;
		}
		Reference(new_vector);
	} else {
		Reference(other);
		if (offset > 0) {
			data = data + GetTypeIdSize(internal_type) * offset;
			validity.Slice(other.validity, offset);
		}
	}
}

void Vector::Slice(Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// already a dictionary, slice the current dictionary
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_buffer<DictionaryBuffer>(move(sliced_dictionary));
		return;
	}
	Vector child_vector(*this);
	auto child_ref = make_buffer<VectorChildBuffer>(move(child_vector));
	auto dict_buffer = make_buffer<DictionaryBuffer>(sel);
	child_ref->data.is_valid = this->is_valid;
	child_ref->data.rowcol_idx = this->rowcol_idx; // TODO right?
	vector_type = VectorType::DICTIONARY_VECTOR;
	buffer = move(dict_buffer);
	auxiliary = move(child_ref);
	child_vector.is_valid = this->is_valid;
	child_vector.rowcol_idx = this->rowcol_idx; // TODO right?
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto target_data = current_sel.data();
		auto entry = cache.cache.find(target_data);
		if (entry != cache.cache.end()) {
			// cached entry exists: use that
			this->buffer = make_buffer<DictionaryBuffer>(((DictionaryBuffer &)*entry->second).GetSelVector());
			vector_type = VectorType::DICTIONARY_VECTOR;
		} else {
			Slice(sel, count);
			cache.cache[target_data] = this->buffer;
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(bool zero_data, idx_t capacity) {
	this->capacity = capacity;
	auxiliary.reset();
	validity.Reset();
	auto &type = GetType();
	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_unique<VectorStructBuffer>(type, capacity);
		auxiliary = move(struct_buffer);
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_unique<VectorListBuffer>(type, capacity);
		auxiliary = move(list_buffer);
	}
	auto type_size = GetTypeIdSize(internal_type);
	if (type_size > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, capacity);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, capacity * type_size);
		}
	}
}

void Vector::CreateRowColumn(const VectorCache &cache, idx_t capacity)
{
	this->capacity = capacity;
	this->is_valid = true;
	cache.ResetFromCacheForRowCol(*this);
}

void Vector::AssignRowMajorStore(buffer_ptr<VectorBuffer> buffer)
{
	AssignSharedPointer(auxiliary, buffer);
}

buffer_ptr<VectorBuffer> Vector::GetRowMajorStoreBufferPtr()
{
	D_ASSERT(auxiliary->GetBufferType() == VectorBufferType::ROWSTORE_BUFFER);
	return auxiliary;
}

char *Vector::GetRowMajorStore()
{
	D_ASSERT(auxiliary->GetBufferType() == VectorBufferType::ROWSTORE_BUFFER);
	auto &rowstore_buffer = (VectorRowStoreBuffer &)*auxiliary;
	return rowstore_buffer.GetRowData();
}

struct DataArrays {
	Vector &vec;
	data_ptr_t data;
	VectorBuffer *buffer;
	idx_t type_size;
	bool is_nested;
	DataArrays(Vector &vec, data_ptr_t data, VectorBuffer *buffer, idx_t type_size, bool is_nested)
	    : vec(vec), data(data), buffer(buffer), type_size(type_size), is_nested(is_nested) {};
};

void FindChildren(std::vector<DataArrays> &to_resize, VectorBuffer &auxiliary) {
	if (auxiliary.GetBufferType() == VectorBufferType::LIST_BUFFER) {
		auto &buffer = (VectorListBuffer &)auxiliary;
		auto &child = buffer.GetChild();
		auto data = child.GetData();
		if (!data) {
			//! Nested type
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  true);
			to_resize.emplace_back(arrays);
			FindChildren(to_resize, *child.GetAuxiliary());
		} else {
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  false);
			to_resize.emplace_back(arrays);
		}
	} else if (auxiliary.GetBufferType() == VectorBufferType::STRUCT_BUFFER) {
		auto &buffer = (VectorStructBuffer &)auxiliary;
		auto &children = buffer.GetChildren();
		for (auto &child : children) {
			auto data = child->GetData();
			if (!data) {
				//! Nested type
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), true);
				to_resize.emplace_back(arrays);
				FindChildren(to_resize, *child->GetAuxiliary());
			} else {
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), false);
				to_resize.emplace_back(arrays);
			}
		}
	}
}
void Vector::Resize(idx_t cur_size, idx_t new_size) {
	std::vector<DataArrays> to_resize;
	if (!buffer) {
		buffer = make_unique<VectorBuffer>(0);
	}
	if (!data) {
		//! this is a nested structure
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), true);
		to_resize.emplace_back(arrays);
		FindChildren(to_resize, *auxiliary);
	} else {
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), false);
		to_resize.emplace_back(arrays);
	}
	for (auto &data_to_resize : to_resize) {
		if (!data_to_resize.is_nested) {
			auto new_data = unique_ptr<data_t[]>(new data_t[new_size * data_to_resize.type_size]);
			memcpy(new_data.get(), data_to_resize.data, cur_size * data_to_resize.type_size * sizeof(data_t));
			data_to_resize.buffer->SetData(move(new_data));
			data_to_resize.vec.data = data_to_resize.buffer->GetData();
		}
		data_to_resize.vec.validity.Resize(cur_size, new_size);
	}
}

void Vector::SetValue(idx_t index, const Value &val) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), val);
	}
	if (val.type().InternalType() != GetType().InternalType()) {
		SetValue(index, val.CastAs(GetType()));
		return;
	}

	validity.EnsureWritable();
	validity.Set(index, !val.IsNull());
	if (val.IsNull() && GetType().InternalType() != PhysicalType::STRUCT) {
		// for structs we still need to set the child-entries to NULL
		// so we do not bail out yet
		return;
	}

	switch (GetType().InternalType()) {
	case PhysicalType::BOOL:
		((bool *)data)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		((int8_t *)data)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		((int16_t *)data)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		((int32_t *)data)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		((int64_t *)data)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		((hugeint_t *)data)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		((uint8_t *)data)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		((uint16_t *)data)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		((uint32_t *)data)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		((uint64_t *)data)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::FLOAT:
		((float *)data)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		((double *)data)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		((interval_t *)data)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		((string_t *)data)[index] = StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		break;
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		auto &val_children = StructValue::GetChildren(val);
		D_ASSERT(val.IsNull() || children.size() == val_children.size());
		for (size_t i = 0; i < children.size(); i++) {
			auto &vec_child = children[i];
			if (!val.IsNull()) {
				auto &struct_child = val_children[i];
				vec_child->SetValue(index, struct_child);
			} else {
				vec_child->SetValue(index, Value());
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		auto &val_children = ListValue::GetChildren(val);
		if (!val_children.empty()) {
			for (idx_t i = 0; i < val_children.size(); i++) {
				ListVector::PushBack(*this, val_children[i]);
			}
		}

		//! now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val_children.size();
		entry.offset = offset;
		break;
	}
	default:
		throw InternalException("Unimplemented type for Vector::SetValue");
	}
}

void Vector::SimpleSetValue(idx_t index, const Value &val) {
	switch (GetType().InternalType()) {
	case PhysicalType::BOOL:
		((bool *)data)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		((int8_t *)data)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		((int16_t *)data)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		((int32_t *)data)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		((int64_t *)data)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		((hugeint_t *)data)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		((uint8_t *)data)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		((uint16_t *)data)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		((uint32_t *)data)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		((uint64_t *)data)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::FLOAT:
		((float *)data)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		((double *)data)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		((interval_t *)data)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		((string_t *)data)[index] = StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		break;
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		auto &val_children = StructValue::GetChildren(val);
		D_ASSERT(val.IsNull() || children.size() == val_children.size());
		for (size_t i = 0; i < children.size(); i++) {
			auto &vec_child = children[i];
			if (!val.IsNull()) {
				auto &struct_child = val_children[i];
				vec_child->SetValue(index, struct_child);
			} else {
				vec_child->SetValue(index, Value());
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		auto &val_children = ListValue::GetChildren(val);
		if (!val_children.empty()) {
			for (idx_t i = 0; i < val_children.size(); i++) {
				ListVector::PushBack(*this, val_children[i]);
			}
		}
		//! now set the pointer
		auto &entry = ((list_entry_t *)data)[index];
		entry.length = val_children.size();
		entry.offset = offset;
		break;
	}
	default:
		throw InternalException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValue(idx_t index) const {
	if (!is_valid) {
		return Value(GetType());
	}

	switch (GetVectorType()) {
	case VectorType::CONSTANT_VECTOR:
		index = 0;
		break;
	case VectorType::FLAT_VECTOR:
		break;
		// dictionary: apply dictionary and forward to child
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.GetValue(sel_vector.get_index(index));
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		return Value::Numeric(GetType(), start + increment * index);
	}
	case VectorType::ROW_VECTOR: {
		return GetRowColValue(index);
	}
	default:
		throw InternalException("Unimplemented vector type for Vector::GetValue");
	}

	if (!validity.RowIsValid(index)) {
		return Value(GetType());
	}

	switch (GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(((bool *)data)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(((int8_t *)data)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(((int16_t *)data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(((int32_t *)data)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(((date_t *)data)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(((dtime_t *)data)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(((dtime_t *)data)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(((int64_t *)data)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(((uint8_t *)data)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(((uint16_t *)data)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(((uint32_t *)data)[index]);
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::ID:
	case LogicalTypeId::ADJLISTCOLUMN:
		return Value::UBIGINT(((uint64_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(((timestamp_t *)data)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(((timestamp_t *)data)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(((hugeint_t *)data)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(((hugeint_t *)data)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(GetType());
		auto scale = DecimalType::GetScale(GetType());
		switch (GetType().InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(((int16_t *)data)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(((int32_t *)data)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(((int64_t *)data)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(((hugeint_t *)data)[index], width, scale);
		default:
			throw InternalException("Widths bigger than 38 are not supported");
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(((uint8_t *)data)[index], GetType());
		case PhysicalType::UINT16:
			return Value::ENUM(((uint16_t *)data)[index], GetType());
		case PhysicalType::UINT32:
			return Value::ENUM(((uint32_t *)data)[index], GetType());
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::HASH:
		return Value::HASH(((hash_t *)data)[index]);
	case LogicalTypeId::POINTER:
		return Value::POINTER(((uintptr_t *)data)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(((float *)data)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(((double *)data)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(((interval_t *)data)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = ((string_t *)data)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::JSON: {
		auto str = ((string_t *)data)[index];
		return Value::JSON(str.GetString());
	}
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::BLOB: {
		auto str = ((string_t *)data)[index];
		return Value::BLOB((const_data_ptr_t)str.GetDataUnsafe(), str.GetSize());
	}
	case LogicalTypeId::MAP: {
		auto &child_entries = StructVector::GetEntries(*this);
		Value key = child_entries[0]->GetValue(index);
		Value value = child_entries[1]->GetValue(index);
		return Value::MAP(move(key), move(value));
	}
	case LogicalTypeId::STRUCT: {
		// we can derive the value schema from the vector schema
		auto &child_entries = StructVector::GetEntries(*this);
		child_list_t<Value> children;
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			auto &struct_child = child_entries[child_idx];
			children.push_back(
			    make_pair(StructType::GetChildName(GetType(), child_idx), struct_child->GetValue(index)));
		}
		return Value::STRUCT(move(children));
	}
	case LogicalTypeId::LIST: {
		auto offlen = ((list_entry_t *)data)[index];
		auto &child_vec = ListVector::GetEntry(*this);
		vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::LIST(ListType::GetChildType(GetType()), move(children));
	}
	case LogicalTypeId::FORWARD_ADJLIST:
	case LogicalTypeId::BACKWARD_ADJLIST: {
		D_ASSERT(index >= 0);
		auto start_offset = index == 0 ? STANDARD_VECTOR_SIZE : ((idx_t *)data)[index-1];
		auto end_offset = ((idx_t *)data)[index];
		vector<Value> children;
		for (idx_t i = start_offset; i < end_offset; i++) {
			children.push_back(Value::UBIGINT(((uint64_t *)data)[i]));
		}
		return Value::LIST(LogicalType::UBIGINT, move(children));
	}
	case LogicalTypeId::SQLNULL: { // S62 added
		return Value("NULL");
	}
	default:
		throw InternalException("Unimplemented type for value access");
	}
}

Value Vector::GetRowColValue(idx_t index) const {
	D_ASSERT(auxiliary->GetBufferType() == VectorBufferType::ROWSTORE_BUFFER);
	D_ASSERT(rowcol_idx >= 0);
	auto &row_buffer = (VectorRowStoreBuffer &)*auxiliary;
	char *row_data = row_buffer.GetRowData();
	rowcol_t *rowcol = (rowcol_t *)data;
	auto base_offset = rowcol[index].offset;
	PartialSchema *schema_ptr = (PartialSchema *)rowcol[index].schema_ptr;
	LogicalType type = GetType();

	if (schema_ptr->hasIthCol(rowcol_idx)) {
		auto offset = schema_ptr->getIthColOffset(rowcol_idx);
		switch(type.id()) {
			case LogicalTypeId::BOOLEAN:
				return Value::BOOLEAN(*(bool *)(row_data + base_offset + offset));
			case LogicalTypeId::TINYINT:
				return Value::TINYINT(*(int8_t *)(row_data + base_offset + offset));
			case LogicalTypeId::SMALLINT:
				return Value::SMALLINT(*(int16_t *)(row_data + base_offset + offset));
			case LogicalTypeId::INTEGER:
				return Value::INTEGER(*(int32_t *)(row_data + base_offset + offset));
			case LogicalTypeId::BIGINT:
				return Value::BIGINT(*(int64_t *)(row_data + base_offset + offset));
			case LogicalTypeId::UINTEGER:
				return Value::UINTEGER(*(uint32_t *)(row_data + base_offset + offset));
			case LogicalTypeId::UBIGINT:
				return Value::UBIGINT(*(uint64_t *)(row_data + base_offset + offset));
			case LogicalTypeId::FLOAT:
				return Value::FLOAT(*(float *)(row_data + base_offset + offset));
			case LogicalTypeId::DOUBLE:
				return Value::DOUBLE(*(double *)(row_data + base_offset + offset));
			case LogicalTypeId::VARCHAR:
				return Value(*(string_t *)(row_data + base_offset + offset));
			default:
				throw NotImplementedException("GetRowColValue - Unimplemented type for value access");
		}
	} else {
		return Value(type);
	}
}

// LCOV_EXCL_START
string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::DICTIONARY_VECTOR:
		return "DICTIONARY";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	case VectorType::ROW_VECTOR:
		return "ROW";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString(idx_t count) const {
	string retval =
	    VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": " + to_string(count) + " = [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * i) + (i == count - 1 ? "" : ", ");
		}
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	// fprintf(stdout, "%s\n", retval.c_str());
	return retval;
}

void Vector::Print(idx_t count) {
	//Printer::Print(ToString(count));
}

string Vector::ToString() const {
	string retval = VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": (UNKNOWN COUNT) [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print() {
	//Printer::Print(ToString());
}
// LCOV_EXCL_STOP

template <class T>
static void TemplatedFlattenConstantVector(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = Load<T>(old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Normalify(idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType());
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		bool is_null = ConstantVector::IsNull(*this);
		// allocate a new buffer for the vector
		auto old_buffer = move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type, MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		data = buffer->GetData();
		vector_type = VectorType::FLAT_VECTOR;
		if (is_null) {
			// constant NULL, set nullmask
			validity.EnsureWritable();
			validity.SetAllInvalid(count);
			return;
		}
		// non-null constant: have to repeat the constant
		switch (GetType().InternalType()) {
		case PhysicalType::BOOL:
			TemplatedFlattenConstantVector<bool>(data, old_data, count);
			break;
		case PhysicalType::INT8:
			TemplatedFlattenConstantVector<int8_t>(data, old_data, count);
			break;
		case PhysicalType::INT16:
			TemplatedFlattenConstantVector<int16_t>(data, old_data, count);
			break;
		case PhysicalType::INT32:
			TemplatedFlattenConstantVector<int32_t>(data, old_data, count);
			break;
		case PhysicalType::INT64:
			TemplatedFlattenConstantVector<int64_t>(data, old_data, count);
			break;
		case PhysicalType::UINT8:
			TemplatedFlattenConstantVector<uint8_t>(data, old_data, count);
			break;
		case PhysicalType::UINT16:
			TemplatedFlattenConstantVector<uint16_t>(data, old_data, count);
			break;
		case PhysicalType::UINT32:
			TemplatedFlattenConstantVector<uint32_t>(data, old_data, count);
			break;
		case PhysicalType::UINT64:
			TemplatedFlattenConstantVector<uint64_t>(data, old_data, count);
			break;
		case PhysicalType::INT128:
			TemplatedFlattenConstantVector<hugeint_t>(data, old_data, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenConstantVector<float>(data, old_data, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenConstantVector<double>(data, old_data, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedFlattenConstantVector<interval_t>(data, old_data, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedFlattenConstantVector<string_t>(data, old_data, count);
			break;
		case PhysicalType::LIST: {
			TemplatedFlattenConstantVector<list_entry_t>(data, old_data, count);
			break;
		}
		case PhysicalType::STRUCT: {
			auto normalified_buffer = make_unique<VectorStructBuffer>();

			auto &new_children = normalified_buffer->GetChildren();

			auto &child_entries = StructVector::GetEntries(*this);
			for (auto &child : child_entries) {
				D_ASSERT(child->GetVectorType() == VectorType::CONSTANT_VECTOR);
				auto vector = make_unique<Vector>(*child);
				vector->Normalify(count);
				new_children.push_back(move(vector));
			}
			auxiliary = move(normalified_buffer);
		} break;
		default:
			throw InternalException("Unimplemented type for VectorOperations::Normalify");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, start, increment);
		break;
	}
	case VectorType::ROW_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType());
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify");
	}
}

template <class T>
static void TemplatedFlattenRowStore(const Vector &source, const SelectionVector &sel, Vector &target, idx_t count) {
	// source data
	auto ldata = FlatVector::GetData<rowcol_t>(source);
	auto rowcol_idx = source.GetRowColIdx();
	auto *row_buffer = (VectorRowStoreBuffer *)(source.GetAuxiliary().get());
	char *row_data = row_buffer->GetRowData();

	// target data
	auto tdata = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	D_ASSERT(rowcol_idx >= 0);

	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto base_offset = ldata[idx].offset;
		PartialSchema *schema_ptr = (PartialSchema *)ldata[idx].schema_ptr;
		if (schema_ptr->hasIthCol(rowcol_idx)) {
			auto offset = schema_ptr->getIthColOffset(rowcol_idx);
			memcpy(tdata + idx,
				row_data + offset,
				sizeof(T));
		} else {
			target_validity.SetInvalid(idx);
		}
	}
}

void Vector::Normalify(const SelectionVector &sel, idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	case VectorType::ROW_VECTOR: {
		Vector other(GetType());
		auto &other_validity = FlatVector::Validity(other);

		if(!this->is_valid) {
			other_validity.SetAllInvalid(count);
			break;
		}

		switch(GetType().InternalType()) {
		case PhysicalType::BOOL:
			TemplatedFlattenRowStore<bool>(*this, sel, other, count);
			break;
		case PhysicalType::INT8:
			TemplatedFlattenRowStore<int8_t>(*this, sel, other, count);
			break;
		case PhysicalType::INT16:
			TemplatedFlattenRowStore<int16_t>(*this, sel, other, count);
			break;
		case PhysicalType::INT32:
			TemplatedFlattenRowStore<int32_t>(*this, sel, other, count);
			break;
		case PhysicalType::INT64:
			TemplatedFlattenRowStore<int64_t>(*this, sel, other, count);
			break;
		case PhysicalType::UINT8:
			TemplatedFlattenRowStore<uint8_t>(*this, sel, other, count);
			break;
		case PhysicalType::UINT16:
			TemplatedFlattenRowStore<uint16_t>(*this, sel, other, count);
			break;
		case PhysicalType::UINT32:
			TemplatedFlattenRowStore<uint32_t>(*this, sel, other, count);
			break;
		case PhysicalType::UINT64:
			TemplatedFlattenRowStore<uint64_t>(*this, sel, other, count);
			break;
		case PhysicalType::INT128:
			TemplatedFlattenRowStore<hugeint_t>(*this, sel, other, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenRowStore<float>(*this, sel, other, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenRowStore<double>(*this, sel, other, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedFlattenRowStore<interval_t>(*this, sel, other, count);
			break;
		case PhysicalType::VARCHAR: {
			auto ldata = FlatVector::GetData<rowcol_t>(*this);
			auto rowcol_idx = this->GetRowColIdx();
			auto *row_buffer = (VectorRowStoreBuffer *)(this->GetAuxiliary().get());
			char *row_data = row_buffer->GetRowData();
			auto tdata = FlatVector::GetData<string_t>(other);
			auto &target_validity = FlatVector::Validity(other);

			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto base_offset = ldata[idx].offset;
				PartialSchema *schema_ptr = (PartialSchema *)ldata[idx].schema_ptr;
				if (schema_ptr->hasIthCol(rowcol_idx)) {
					auto offset = schema_ptr->getIthColOffset(rowcol_idx);
					string_t str = *((string_t *)(row_data + offset));
					tdata[idx] = StringVector::AddStringOrBlob(other, str);
				} else {
					target_validity.SetInvalid(idx);
				}
			}
			break;
		}
		default:
			throw InternalException("Unimplemented type for VectorOperations::Normalify");
		}

		this->Reference(other);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::Orrify(idx_t count, VectorData &data) {
	switch (GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		if (child.GetVectorType() == VectorType::FLAT_VECTOR) {
			data.sel = &sel;
			data.data = FlatVector::GetData(child);
			data.validity = FlatVector::Validity(child);
			data.is_valid = child.is_valid;
		} else {
			// dictionary with non-flat child: create a new reference to the child and normalify it
			Vector child_vector(child);
			child_vector.Normalify(sel, count);
			auto new_aux = make_buffer<VectorChildBuffer>(move(child_vector));

			data.sel = &sel;
			data.data = FlatVector::GetData(new_aux->data);
			data.validity = FlatVector::Validity(new_aux->data);
			data.is_valid = new_aux->data.is_valid;
			this->auxiliary = move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		data.sel = ConstantVector::ZeroSelectionVector(count, data.owned_sel);
		data.data = ConstantVector::GetData(*this);
		data.validity = ConstantVector::Validity(*this);
		data.is_valid = this->is_valid;
		break;
	default:
		Normalify(count);
		data.sel = FlatVector::IncrementalSelectionVector(count, data.owned_sel);
		data.data = FlatVector::GetData(*this);
		data.validity = FlatVector::Validity(*this);
		data.is_valid = this->is_valid;
		break;
	}
}

void Vector::Sequence(int64_t start, int64_t increment) {
	this->vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 2);
	auto data = (int64_t *)buffer->GetData();
	data[0] = start;
	data[1] = increment;
	validity.Reset();
	auxiliary.reset();
}

void Vector::Serialize(idx_t count, Serializer &serializer) {
	D_ASSERT(false);
	/*auto &type = GetType();

	VectorData vdata;
	Orrify(count, vdata);

	const auto write_validity = (count > 0) && !vdata.validity.AllValid();
	serializer.Write<bool>(write_validity);
	if (write_validity) {
		ValidityMask flat_mask(count);
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = vdata.sel->get_index(i);
			flat_mask.Set(i, vdata.validity.RowIsValid(row_idx));
		}
		serializer.WriteData((const_data_ptr_t)flat_mask.GetData(), flat_mask.ValidityMaskSize(count));
	}
	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[write_size]);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteData(ptr.get(), write_size);
	} else {
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = (string_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
				serializer.WriteStringLen((const_data_ptr_t)source.GetDataUnsafe(), source.GetSize());
			}
			break;
		}
		case PhysicalType::STRUCT: {
			Normalify(count);
			auto &entries = StructVector::GetEntries(*this);
			for (auto &entry : entries) {
				entry->Serialize(count, serializer);
			}
			break;
		}
		case PhysicalType::LIST: {
			auto &child = ListVector::GetEntry(*this);
			auto list_size = ListVector::GetListSize(*this);

			// serialize the list entries in a flat array
			auto data = unique_ptr<list_entry_t[]>(new list_entry_t[count]);
			auto source_array = (list_entry_t *)vdata.data;
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = source_array[idx];
				data[i].offset = source.offset;
				data[i].length = source.length;
			}

			// write the list size
			serializer.Write<idx_t>(list_size);
			serializer.WriteData((data_ptr_t)data.get(), count * sizeof(list_entry_t));

			child.Serialize(list_size, serializer);
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Serialize!");
		}
	}*/
}

void Vector::Deserialize(idx_t count, Deserializer &source) {
	D_ASSERT(false);
	/*auto &type = GetType();

	auto &validity = FlatVector::Validity(*this);
	validity.Reset();
	const auto has_validity = source.Read<bool>();
	if (has_validity) {
		validity.Initialize(count);
		source.ReadData((data_ptr_t)validity.GetData(), validity.ValidityMaskSize(count));
	}

	if (TypeIsConstantSize(type.InternalType())) {
		// constant size type: read fixed amount of data from
		auto column_size = GetTypeIdSize(type.InternalType()) * count;
		auto ptr = unique_ptr<data_t[]>(new data_t[column_size]);
		source.ReadData(ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				// read the strings
				auto str = source.Read<string>();
				// now add the string to the StringHeap of the vector
				// and write the pointer into the vector
				if (validity.RowIsValid(i)) {
					strings[i] = StringVector::AddStringOrBlob(*this, str);
				}
			}
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			for (auto &entry : entries) {
				entry->Deserialize(count, source);
			}
			break;
		}
		case PhysicalType::LIST: {
			// read the list size
			auto list_size = source.Read<idx_t>();
			ListVector::Reserve(*this, list_size);
			ListVector::SetListSize(*this, list_size);

			// read the list entry
			auto list_entries = FlatVector::GetData(*this);
			source.ReadData(list_entries, count * sizeof(list_entry_t));

			// deserialize the child vector
			auto &child = ListVector::GetEntry(*this);
			child.Deserialize(list_size, source);

			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Deserialize!");
		}
	}*/
}

void Vector::SetVectorType(VectorType vector_type_p) {
	this->vector_type = vector_type_p;
	if (TypeIsConstantSize(GetType().InternalType()) &&
	    (GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR)) {
		auxiliary.reset();
	}
	if (vector_type == VectorType::CONSTANT_VECTOR && GetType().InternalType() == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(*this);
		for (auto &entry : entries) {
			entry->SetVectorType(vector_type);
		}
	}
}

void Vector::UTFVerify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetType().InternalType() == PhysicalType::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch (GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].Verify();
				}
			}
			break;
		}
		default:
			break;
		}
	}
#endif
}

void Vector::UTFVerify(idx_t count) {
	if (!is_valid) return;
	SelectionVector owned_sel;
	auto flat_sel = FlatVector::IncrementalSelectionVector(count, owned_sel);

	UTFVerify(*flat_sel, count);
}

void Vector::Verify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*this);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*this);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(sel, count);
		SelectionVector new_sel(new_buffer);
		child.Verify(new_sel, count);
		return;
	}
	if (TypeIsConstantSize(GetType().InternalType()) &&
	    (GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR)) {
		D_ASSERT(!auxiliary);
	}
	if (GetType().id() == LogicalTypeId::VARCHAR || GetType().id() == LogicalTypeId::JSON) {
		// verify that there are no '\0' bytes in string values
		switch (GetVectorType()) {
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].VerifyNull();
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (GetType().InternalType() == PhysicalType::STRUCT) {
		auto &child_types = StructType::GetChildTypes(GetType());
		D_ASSERT(!child_types.empty());
		if (GetVectorType() == VectorType::FLAT_VECTOR || GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// create a selection vector of the non-null entries of the struct vector
			auto &children = StructVector::GetEntries(*this);
			D_ASSERT(child_types.size() == children.size());
			for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
				if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
					D_ASSERT(children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR);
					if (ConstantVector::IsNull(*this)) {
						D_ASSERT(ConstantVector::IsNull(*children[child_idx]));
					}
				} else if (GetVectorType() == VectorType::FLAT_VECTOR &&
				           children[child_idx]->GetVectorType() == VectorType::FLAT_VECTOR) {
					// for any NULL entry in the struct, the child should be NULL as well
					auto &validity = FlatVector::Validity(*this);
					auto &child_validity = FlatVector::Validity(*children[child_idx]);
					for (idx_t i = 0; i < count; i++) {
						auto index = sel.get_index(i);
						if (!validity.RowIsValid(index)) {
							D_ASSERT(!child_validity.RowIsValid(index));
						}
					}
				}
				D_ASSERT(children[child_idx]->GetType() == child_types[child_idx].second);
				children[child_idx]->Verify(sel, count);
			}
		}
	}

	if (GetType().InternalType() == PhysicalType::LIST) {
		if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*this)) {
				auto &child = ListVector::GetEntry(*this);
				SelectionVector child_sel(ListVector::GetListSize(*this));
				idx_t child_count = 0;
				auto le = ConstantVector::GetData<list_entry_t>(*this);
				D_ASSERT(le->offset + le->length <= ListVector::GetListSize(*this));
				for (idx_t k = 0; k < le->length; k++) {
					child_sel.set_index(child_count++, le->offset + k);
				}
				child.Verify(child_sel, child_count);
			}
		} else if (GetVectorType() == VectorType::FLAT_VECTOR) {
			auto &child = ListVector::GetEntry(*this);
			auto child_size = ListVector::GetListSize(*this);
			auto list_data = FlatVector::GetData<list_entry_t>(*this);
			idx_t total_size = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					total_size += le.length;
				}
			}
			SelectionVector child_sel(total_size);
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					for (idx_t k = 0; k < le.length; k++) {
						child_sel.set_index(child_count++, le.offset + k);
					}
				}
			}
			child.Verify(child_sel, child_count);
		}
	}
#endif
}

void Vector::Verify(idx_t count) {
	if (!is_valid) return;
	SelectionVector owned_sel;
	auto flat_sel = FlatVector::IncrementalSelectionVector(count, owned_sel);
	Verify(*flat_sel, count);
}

void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.validity.Set(idx, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			FlatVector::SetNull(*entry, idx, is_null);
		}
	}
}

void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.validity.Set(0, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			entry->SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(*entry, is_null);
		}
	}
}

const SelectionVector *FlatVector::IncrementalSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return FlatVector::IncrementalSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, i);
	}
	return &owned_sel;
}

const SelectionVector *ConstantVector::ZeroSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return ConstantVector::ZeroSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, 0);
	}
	return &owned_sel;
}

void ConstantVector::Reference(Vector &vector, Vector &source, idx_t position, idx_t count) {
	D_ASSERT(position < count);
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		VectorData vdata;
		source.Orrify(count, vdata);

		auto list_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(list_index)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		auto list_data = (list_entry_t *)vdata.data;
		auto list_entry = list_data[list_index];

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetData<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetEntry(vector);
		child.Reference(ListVector::GetEntry(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	case PhysicalType::STRUCT: {
		VectorData vdata;
		source.Orrify(count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(*target_entries[i], *source_entries[i], position, count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		if (source.GetIsValid()) {
			auto value = source.GetValue(position);
			vector.Reference(value);
		} else {
			// if the source is invalid, reference a NULL value
			auto value = Value(vector.GetType());
			vector.Reference(value);
		}
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, len));
}

string_t StringVector::AddStringOrBlob(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddStringOrBlob(vector, string_t(data, len));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, strlen(data)));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), data.size()));
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::JSON);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len < string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	return string_buffer.EmptyString(len);
}

/*void StringVector::AddHandle(Vector &vector, unique_ptr<BufferHandle> handle) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(make_buffer<ManagedVectorBuffer>(move(handle)));
}*/

void StringVector::AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(buffer.get() != vector.auxiliary.get());
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = (VectorStringBuffer &)*vector.auxiliary;
	string_buffer.AddHeapReference(move(buffer));
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(other.GetType().InternalType() == PhysicalType::VARCHAR);

	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		StringVector::AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
	if (!other.auxiliary) {
		return;
	}
	StringVector::AddBuffer(vector, other.auxiliary);
}

vector<unique_ptr<Vector>> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return StructVector::GetEntries(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return ((VectorStructBuffer *)vector.auxiliary.get())->GetChildren();
}

const vector<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

const Vector &ListVector::GetEntry(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return ((VectorListBuffer *)vector.auxiliary.get())->GetChild();
}

Vector &ListVector::GetEntry(Vector &vector) {
	const Vector &cvector = vector;
	return const_cast<Vector &>(ListVector::GetEntry(cvector));
}

void ListVector::Reserve(Vector &vector, idx_t required_capacity) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = *((VectorListBuffer *)vector.auxiliary.get());
	child_buffer.Reserve(required_capacity);
}

template <class T>
void TemplatedSearchInMap(Vector &list, T key, vector<idx_t> &offsets, bool is_key_null, idx_t offset, idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (T *)vector_data.data;
	auto validity_mask = vector_data.validity;

	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (key == data[i]) {
				offsets.push_back(i);
			}
		}
	}
}

template <class T>
void TemplatedSearchInMap(Vector &list, const Value &key, vector<idx_t> &offsets, bool is_key_null, idx_t offset,
                          idx_t length) {
	TemplatedSearchInMap<T>(list, key.template GetValueUnsafe<T>(), offsets, is_key_null, offset, length);
}

void SearchStringInMap(Vector &list, const string &key, vector<idx_t> &offsets, bool is_key_null, idx_t offset,
                       idx_t length) {
	auto &list_vector = ListVector::GetEntry(list);
	VectorData vector_data;
	list_vector.Orrify(ListVector::GetListSize(list), vector_data);
	auto data = (string_t *)vector_data.data;
	auto validity_mask = vector_data.validity;
	if (is_key_null) {
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				offsets.push_back(i);
			}
		}
	} else {
		string_t key_str_t(key);
		for (idx_t i = offset; i < offset + length; i++) {
			if (!validity_mask.RowIsValid(i)) {
				continue;
			}
			if (Equals::Operation<string_t>(data[i], key_str_t)) {
				offsets.push_back(i);
			}
		}
	}
}

vector<idx_t> ListVector::Search(Vector &list, const Value &key, idx_t row) {
	vector<idx_t> offsets;

	auto &list_vector = ListVector::GetEntry(list);
	auto &entry = ((list_entry_t *)list.GetData())[row];

	switch (list_vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedSearchInMap<int8_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT16:
		TemplatedSearchInMap<int16_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT32:
		TemplatedSearchInMap<int32_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT64:
		TemplatedSearchInMap<int64_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::INT128:
		TemplatedSearchInMap<hugeint_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT8:
		TemplatedSearchInMap<uint8_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT16:
		TemplatedSearchInMap<uint16_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT32:
		TemplatedSearchInMap<uint32_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::UINT64:
		TemplatedSearchInMap<uint64_t>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::FLOAT:
		TemplatedSearchInMap<float>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::DOUBLE:
		TemplatedSearchInMap<double>(list, key, offsets, key.IsNull(), entry.offset, entry.length);
		break;
	case PhysicalType::VARCHAR:
		SearchStringInMap(list, StringValue::Get(key), offsets, key.IsNull(), entry.offset, entry.length);
		break;
	default:
		throw InvalidTypeException(list.GetType().id(), "Invalid type for List Vector Search");
	}
	return offsets;
}

Value ListVector::GetValuesFromOffsets(Vector &list, vector<idx_t> &offsets) {
	auto &child_vec = ListVector::GetEntry(list);
	vector<Value> list_values;
	list_values.reserve(offsets.size());
	for (auto &offset : offsets) {
		list_values.push_back(child_vec.GetValue(offset));
	}
	return Value::LIST(ListType::GetChildType(list.GetType()), move(list_values));
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return ((VectorListBuffer &)*vec.auxiliary).size;
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = other.auxiliary;
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
	}
	((VectorListBuffer &)*vec.auxiliary).size = size;
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = (VectorListBuffer &)*target.auxiliary;
	target_buffer.PushBack(insert);
}

} // namespace duckdb
