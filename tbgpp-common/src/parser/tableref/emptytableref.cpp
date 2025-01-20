#include "parser/tableref/emptytableref.hpp"

#include "common/field_writer.hpp"

namespace s62 {

string EmptyTableRef::ToString() const {
	return "";
}

bool EmptyTableRef::Equals(const TableRef *other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> EmptyTableRef::Copy() {
	return make_unique<EmptyTableRef>();
}

void EmptyTableRef::Serialize(FieldWriter &writer) const {
}

unique_ptr<TableRef> EmptyTableRef::Deserialize(FieldReader &reader) {
	return make_unique<EmptyTableRef>();
}

} // namespace s62
