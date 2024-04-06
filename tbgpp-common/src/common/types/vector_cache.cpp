#include "common/types/vector_cache.hpp"
#include "common/types/vector.hpp"

#include "icecream.hpp"

namespace duckdb {

class VectorCacheBuffer : public VectorBuffer {
public:
	explicit VectorCacheBuffer(const LogicalType &type_p, size_t size = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), type(type_p) {
		if (size == 0) return;
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::ADJLIST: {
			owned_data = unique_ptr<data_t[]>(new data_t[size * GetTypeIdSize(internal_type)]);
			LogicalType child_type = LogicalType::UBIGINT;
			child_caches.push_back(make_buffer<VectorCacheBuffer>(child_type));
			auto child_vector = make_unique<Vector>(child_type, false, false);
			auxiliary = make_unique<VectorListBuffer>(move(child_vector));
			break;
		}
		case PhysicalType::LIST: {
			// memory for the list offsets
			owned_data = unique_ptr<data_t[]>(new data_t[size * GetTypeIdSize(internal_type)]);
			// child data of the list
			if (!type.AuxInfo()) {
				type = LogicalType::LIST(LogicalType::UBIGINT);
			}
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(child_type, size));
			auto child_vector = make_unique<Vector>(child_type, false, false, size);
			// TODO correctness check - 240316 we originally use below code
			// auto child_vector = make_unique<Vector>(child_type, true, false, size);
			auxiliary = make_unique<VectorListBuffer>(move(child_vector), size);
			break;
		}
		case PhysicalType::STRUCT: {
			D_ASSERT(false); // not supported currently
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_buffer<VectorCacheBuffer>(child_type.second));
			}
			auto struct_buffer = make_unique<VectorStructBuffer>(type);
			auxiliary = move(struct_buffer);
			break;
		}
		default:
			if (GetTypeIdSize(internal_type) > 0) {
				owned_data = unique_ptr<data_t[]>(new data_t[size * GetTypeIdSize(internal_type)]);	
			}
			break;
		}
	}

	void ResetFromCache(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset();
		switch (internal_type) {
		case PhysicalType::ADJLIST: {
			result.data = owned_data.get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &list_buffer = (VectorListBuffer &)*result.auxiliary;
			list_buffer.capacity = STANDARD_VECTOR_SIZE;
			list_buffer.size = 0;

			auto &list_child = list_buffer.GetChild();
			auto &child_cache = (VectorCacheBuffer &)*child_caches[0];
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::LIST: {
			result.data = owned_data.get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &list_buffer = (VectorListBuffer &)*result.auxiliary;
			// list_buffer.capacity = STANDARD_VECTOR_SIZE; // TODO is it OK?
			// list_buffer.capacity = STANDARD_VECTOR_SIZE;
			list_buffer.size = 0;

			auto &list_child = list_buffer.GetChild();
			auto &child_cache = (VectorCacheBuffer &)*child_caches[0];
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::STRUCT: {
			// struct does not have data
			result.data = nullptr;
			// reinitialize the VectorStructBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through children
			auto &children = ((VectorStructBuffer &)*result.auxiliary).GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = (VectorCacheBuffer &)*child_caches[i];
				child_cache.ResetFromCache(*children[i], child_caches[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
			result.data = owned_data.get();
			result.auxiliary.reset();
			break;
		}
	}

	void ResetFromCacheForRowCol(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == LogicalType::ROWCOL);
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset();
		switch (internal_type) {
		case PhysicalType::ROWCOL: {
			// TODO
			result.data = owned_data.get();
			break;
		}
		default:
			D_ASSERT(false);
			// regular type: no aux data and reset data to cached data
			// result.data = owned_data.get();
			// result.auxiliary.reset();
			break;
		}
	}

	const LogicalType &GetType() {
		return type;
	}

private:
	//! The type of the vector cache
	LogicalType type;
	//! Owned data
	unique_ptr<data_t[]> owned_data;
	//! Child caches (if any). Used for nested types.
	vector<buffer_ptr<VectorBuffer>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
};

VectorCache::VectorCache(const LogicalType &type_p) {
	buffer = make_unique<VectorCacheBuffer>(type_p);
}

VectorCache::VectorCache(const LogicalType &type_p, size_t size) {
	buffer = make_unique<VectorCacheBuffer>(type_p, size);
}

void VectorCache::ResetFromCache(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = (VectorCacheBuffer &)*buffer;
	vcache.ResetFromCache(result, buffer);
}

void VectorCache::ResetFromCacheForRowCol(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = (VectorCacheBuffer &)*buffer;
	vcache.ResetFromCacheForRowCol(result, buffer);
}

void VectorCache::AllocateBuffer(const LogicalType &type, size_t size) {
	buffer = make_unique<VectorCacheBuffer>(type);
}

const LogicalType &VectorCache::GetType() const {
	auto &vcache = (VectorCacheBuffer &)*buffer;
	return vcache.GetType();
}

} // namespace duckdb
