#pragma once

#include "common/common.hpp"
#include "common/vector.hpp"

#include "common/boost_typedefs.hpp"
#include "boost/histogram.hpp"
#include "boost/accumulators/accumulators.hpp"
#include "boost/accumulators/statistics/stats.hpp"
#include "boost/accumulators/statistics/extended_p_square_quantile.hpp"
#include <queue>

namespace duckdb {

class ClientContext;
class ExtentIterator;
class LogicalType;
class DataChunk;
class PartitionCatalogEntry;

//! Class for creating histogram
class HistogramGenerator {
private:
    std::queue<ExtentIterator *> ext_its;
    std::vector<boost::accumulators::accumulator_set<int64_t, boost::accumulators::stats<boost::accumulators::tag::extended_p_square_quantile>> *> accms;
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

    //! Create histogram internal function
    void _create_histogram_test(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat);

    //! Initialize accumulator for target types
    void _init_accumulators(vector<LogicalType> &universal_schema);

    //! Iterate data chunk & accumulate values
    void _accumulate_data(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema);

    void _create_bucket(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema,
        vector<boost::histogram::histogram<std::tuple<boost::histogram::axis::variable<>>>> &histograms);
};

} // namespace duckdb