#ifndef EXTENT_MANAGER_H
#define EXTENT_MANAGER_H

#include "common/common.hpp"
#include "common/vector.hpp"

namespace s62 {

class Catalog;
class DataChunk;
class ClientContext;
class ExtentCatalogEntry;
class PartitionCatalogEntry;
class PropertySchemaCatalogEntry;
class ChunkDefinitionCatalogEntry;

class ExtentManager {

   public:
    ExtentManager();
    ~ExtentManager() {}

    // for bulk loading
    ExtentID CreateExtent(ClientContext &context, DataChunk &input,
                          PartitionCatalogEntry &part_cat,
                          PropertySchemaCatalogEntry &ps_cat);
    void CreateExtent(ClientContext &context, DataChunk &input,
                      PartitionCatalogEntry &part_cat,
                      PropertySchemaCatalogEntry &ps_cat, ExtentID new_eid);
    void AppendChunkToExistingExtent(ClientContext &context, DataChunk &input,
                                     ExtentID eid);

    // Add Index
    void AddIndex(ClientContext &context, DataChunk &input) {}

   private:
    void _AppendChunkToExtentWithCompression(
        ClientContext &context, DataChunk &input, Catalog &cat_instance,
        ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID eid);
    void _UpdatePartitionMinMaxArray(ClientContext &context,
                                     Catalog &cat_instance,
                                     PartitionCatalogEntry &part_cat,
                                     PropertySchemaCatalogEntry &ps_cat,
                                     ExtentCatalogEntry &extent_cat_entry);
    void _UpdatePartitionMinMaxArray(
        PartitionCatalogEntry &part_cat, PropertyKeyID prop_key_id,
        ChunkDefinitionCatalogEntry &chunkdef_cat_entry);
};

}  // namespace s62

#endif  // TILE_MANAGER_H
