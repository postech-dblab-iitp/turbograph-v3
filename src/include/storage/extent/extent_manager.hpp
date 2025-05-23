#ifndef EXTENT_MANAGER_H
#define EXTENT_MANAGER_H

#include "common/common.hpp"
#include "common/vector.hpp"

namespace duckdb {

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
    ExtentID CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &part_cat, 
                          PropertySchemaCatalogEntry &ps_cat);
    void CreateExtent(ClientContext &context, DataChunk &input, PartitionCatalogEntry &part_cat,
                      PropertySchemaCatalogEntry &ps_cat, ExtentID new_eid);
    void AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, ExtentID eid);

    // Add Index
    void AddIndex(ClientContext &context, DataChunk &input) {}

private:
    void _AppendChunkToExtentWithCompression(ClientContext &context, DataChunk &input, Catalog &cat_instance, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID eid);
    void _UpdatePartitionMinMaxArray(ClientContext &context, Catalog& cat_instance, PartitionCatalogEntry &part_cat, PropertySchemaCatalogEntry &ps_cat, ExtentCatalogEntry &extent_cat_entry);
    void _UpdatePartitionMinMaxArray(PartitionCatalogEntry &part_cat, PropertyKeyID prop_key_id, ChunkDefinitionCatalogEntry& chunkdef_cat_entry);
/*  
    TileID CreateVertexTile(DBInstance &db, VLabels label_set, Schema &schema, bool is_temporary) ;
    void DestroyVertexTile(DBInstance &db, TileID &tile_id) // tile_id needs to be distinguished from transaction_id
    TileID CreateDeltaVertexTile(DBInstance &db, VLabels label_set, Schema &schema, bool is_temporary);
    void DestroyDeltaVertexTile(DBInstance &db, TileID tile_id);
    TileID CreateDeltaEdgeTile(DBInstance &db, VLabels label_set, Schema &schema, bool is_temporary);
    void DestroyDeltaEdgeTile(DBInstance &db, TileID tile_id);

    // when opening a tile, we first load the catalog table into memory
    // we do not need to load individual segments in a tile
    unsigned int OpenTile(DBInstance &db, TileID tile_id); // return open_tile_num; 
    void CloseTile(unsigned int open_tile_number)
    unsigned int OpenSegment(TileID tile_id, SegmentID seg_id)
    void CloseSegment(unsinged int open_seg_number)


    // do we need to support random access to individual records in a tile
    void CreateSegIterator(SegmentID seg_id, Iterator<Segment> &seg_iter) // this memory address is valid until closing the segment

    TileID CreateEdgeTile(DBInstance &db, EdgeType edge_type, Schema &schema, bool is_temporary) 
    void DestroyVertexTile(DBInstance &db, TileID &tile_id) 
    TileID CreateDeltaDeltaTile(DBInstance &db, VLabels label_set, Schema &schema, bool is_temporary);
    TileID CreateDeltaEdgeTile(DBInstance &db, VLabels label_set, Schema &schema, bool is_temporary);

    // moving a delta store to an immutable tile
    void TupleMover(DBInstance &db, TileID tile_id); 

private:
    TileID _CreateTile((DBInstance &db, Schema &schema, bool is_temporary);
    TileID _DestroyTile(DBInstance &db, TileID &tile_id);
    void _DestroyDeltaTile(DBInstance &db, TileID &tile_id);*/
};

} // namespace duckdb

#endif // TILE_MANAGER_H
