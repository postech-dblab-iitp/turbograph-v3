#pragma once

#include "common/common.hpp"
#include "common/vector.hpp"

#include "boost/accumulators/accumulators.hpp"
#include "boost/accumulators/statistics/stats.hpp"
#include "boost/accumulators/statistics/extended_p_square_quantile.hpp"
#include <queue>

namespace duckdb {

class ClientContext;
class ExtentIterator;
class LogicalType;
class DataChunk;

//! Class for creating histogram
class HistogramGenerator {
private:
    std::queue<ExtentIterator *> ext_its;
    std::vector<boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::extended_p_square_quantile>> *> accms;
    std::vector<idx_t> target_cols;

public:
    //! ctor
    HistogramGenerator() {}

    //! dtor
    ~HistogramGenerator() {}

    //! Create histogram for all partitions in the database
    void CreateHistogram(std::shared_ptr<ClientContext> client);

    //! Create histogram for the specific partition
    void CreateHistogram(std::shared_ptr<ClientContext> client, string &part_name);

    //! Create histogram for the specific partition
    void CreateHistogram(std::shared_ptr<ClientContext> client, idx_t partition_oid);

private:
    //! Create histogram internal function
    void _create_histogram(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat);

    //! Initialize accumulator for target types
    void _init_accumulators(vector<LogicalType> &universal_schema);

    //! Iterate data chunk & accumulate values
    void _accumulate_data(DataChunk &chunk, vector<LogicalType> &universal_schema);
};

} // namespace duckdb