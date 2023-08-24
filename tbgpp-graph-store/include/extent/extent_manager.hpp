#ifndef EXTENT_MANAGER_H
#define EXTENT_MANAGER_H

#include "common/common.hpp"
#include "common/vector.hpp"

namespace duckdb {

class Catalog;
class DataChunk;
class ClientContext;
class ExtentCatalogEntry;
class PropertySchemaCatalogEntry;

class ExtentManager {

public:
    ExtentManager();
    ~ExtentManager() {}

    // for bulk loading
    ExtentID CreateExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry);
    //ExtentID CreateEdgeExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry);
    void CreateExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentID new_eid);
    void AppendChunkToExistingExtent(ClientContext &context, DataChunk &input, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentID eid);


    //tjyoon, for partitioning
    void GenerateExtentFromChunkInBuffer(DataChunk& input, std::vector<char*> &buffer_allocated_ptr_list, std::vector<int64_t> &buffer_allocated_size_list, int32_t &buffer_allocated_count);

    // Add Index
    void AddIndex(ClientContext &context, DataChunk &input) {}

private:
    void _AppendChunkToExtent(ClientContext &context, DataChunk &input, Catalog &cat_instance, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID eid);
    void _AppendChunkToExtentWithCompression(ClientContext &context, DataChunk &input, Catalog &cat_instance, PropertySchemaCatalogEntry &prop_schema_cat_entry, ExtentCatalogEntry &extent_cat_entry, PartitionID pid, ExtentID eid);

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
