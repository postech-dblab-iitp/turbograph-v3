#pragma once

#include <folly/init/Init.h>
// #include "velox/connectors/tpch/TpchConnector.h"
// #include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
// #include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/SwitchExpr.h"

#include "velox/dwio/common/DecoderUtil.h"
#include <folly/Random.h>
#include "velox/common/base/Nulls.h"
#include "velox/type/Filter.h"

using namespace facebook::velox;
// using namespace facebook::velox::test;
// using namespace facebook::velox::exec::test;

namespace facebook::velox::dwio::common {
// Excerpt from LazyVector.h.
struct NoHook {
  void addValues(
      const int32_t* /*rows*/,
      const void* /*values*/,
      int32_t /*size*/,
      uint8_t /*valueWidth*/) {}
};

} // namespace facebook::velox::dwio::common

namespace duckdb {

class VeloxPropertyStore : public facebook::velox::test::VectorTestBase {
public:
    VeloxPropertyStore() {
        // Register Presto scalar functions.
        functions::prestosql::registerAllScalarFunctions();

        // Register Presto aggregate functions.
        aggregate::prestosql::registerAllAggregateFunctions();

        // Setup random generator
        rng_.seed(1);
    }

    ~VeloxPropertyStore() {}

    void randomBits(std::vector<uint64_t>& bits, int32_t onesPer1000) {
        for (auto i = 0; i < bits.size() * 64; ++i) {
            if (folly::Random::rand32(rng_) % 1000 < onesPer1000) {
                bits::setBit(bits.data(), i);
            }
        }
    }

    void randomRows(
      int32_t numRows,
      int32_t rowsPer1000,
      raw_vector<int32_t>& result) {
        for (auto i = 0; i < numRows; ++i) {
            if (folly::Random::rand32(rng_) % 1000 < rowsPer1000) {
                result.push_back(i);
            }
        }
    }

    template <bool isFilter, bool outputNulls>
    bool nonNullRowsFromSparseReference(
        const uint64_t* nulls,
        RowSet rows,
        raw_vector<int32_t>& innerRows,
        raw_vector<int32_t>& outerRows,
        uint64_t* resultNulls,
        int32_t& tailSkip) {
        bool anyNull = false;
        auto numIn = rows.size();
        innerRows.resize(numIn);
        outerRows.resize(numIn);
        int32_t lastRow = -1;
        int32_t numNulls = 0;
        int32_t numInner = 0;
        int32_t lastNonNull = -1;
        for (auto i = 0; i < numIn; ++i) {
            auto row = rows[i];
            if (row > lastRow + 1) {
                numNulls += bits::countNulls(nulls, lastRow + 1, row);
            }
            if (bits::isBitNull(nulls, row)) {
                ++numNulls;
                lastRow = row;
                if (!isFilter && outputNulls) {
                    bits::setNull(resultNulls, i);
                    anyNull = true;
                }
            } else {
                innerRows[numInner] = row - numNulls;
                outerRows[numInner++] = isFilter ? row : i;
                lastNonNull = row;
                lastRow = row;
            }
        }
        innerRows.resize(numInner);
        outerRows.resize(numInner);
        tailSkip = bits::countBits(nulls, lastNonNull + 1, lastRow);
        return anyNull;
    }

    // Maps 'rows' where the row falls on a non-null in 'nulls' to an
    // index in non-null rows. This uses both a reference implementation
    // and the SIMDized fast path and checks consistent results.
    template <bool isFilter, bool outputNulls>
    void testNonNullFromSparse(uint64_t* nulls, RowSet rows) {
        raw_vector<int32_t> referenceInner;
        raw_vector<int32_t> referenceOuter;
        std::vector<uint64_t> referenceNulls(bits::nwords(rows.size()), ~0ULL);
        int32_t referenceSkip;
        auto referenceAnyNull =
            nonNullRowsFromSparseReference<isFilter, outputNulls>(
                nulls,
                rows,
                referenceInner,
                referenceOuter,
                referenceNulls.data(),
                referenceSkip);
        // raw_vector<int32_t> testInner;
        // raw_vector<int32_t> testOuter;
        // std::vector<uint64_t> testNulls(bits::nwords(rows.size()), ~0ULL);
        // int32_t testSkip;
        // auto testAnyNull = nonNullRowsFromSparse<isFilter, outputNulls>(
        //     nulls, rows, testInner, testOuter, testNulls.data(), testSkip);

        // EXPECT_EQ(testAnyNull, referenceAnyNull);
        // EXPECT_EQ(testSkip, referenceSkip);
        // for (auto i = 0; i < testInner.size() && i < testOuter.size(); ++i) {
        // EXPECT_EQ(testInner[i], referenceInner[i]);
        // EXPECT_EQ(testOuter[i], referenceOuter[i]);
        // }
        // EXPECT_EQ(testInner.size(), referenceInner.size());
        // EXPECT_EQ(testOuter.size(), referenceOuter.size());

        // if (outputNulls) {
        //     for (auto i = 0; i < rows.size(); ++i) {
        //         EXPECT_EQ(
        //             bits::isBitSet(testNulls.data(), i),
        //             bits::isBitSet(referenceNulls.data(), i));
        //     }
        // }
    }

    void testNonNullFromSparseCases(uint64_t* nulls, RowSet rows) {
        testNonNullFromSparse<false, true>(nulls, rows);
        testNonNullFromSparse<true, false>(nulls, rows);
    }

    void test() {
        // Let’s create two vectors of 64-bit integers and one vector of strings.
        auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        auto dow = makeFlatVector<std::string>(
            {"monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday"});

        auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

        // We can also filter rows that have even values of 'a'.
        // facebook::velox::core::PlanNodePtr plan = 
        //     facebook::velox::exec::test::PlanBuilder().values({data}).filter("a % 2 == 0").planNode();
        
        // auto evenA = facebook::velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());

        // std::cout << std::endl
        //     << "> rows with even values of 'a': " << evenA->toString()
        //     << std::endl;
        // std::cout << evenA->toString(0, evenA->size()) << std::endl;
    }

    void test2() {
        auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
        auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
        auto dow = makeFlatVector<std::string>(
            {"monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday"});

        auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

        auto queryCtx = std::make_shared<core::QueryCtx>();

        auto pool = memory::addDefaultLeafMemoryPool();
        core::ExecCtx execCtx{pool.get(), queryCtx.get()};

        auto inputRowType = ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"dow", VARCHAR()}});

        auto fieldAccessExprNode =
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "a");
        auto fieldAccessExprPtr =
            std::vector<facebook::velox::core::TypedExprPtr>{fieldAccessExprNode};

        // auto specialForm = facebook::velox::exec::constructSpecialForm(
        //     "if",
        //     BIGINT(),
        //     {std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<bool>({false})),
        //     std::make_shared<facebook::velox::exec::FieldReference>(
        //         BIGINT(),
        //         fieldAccessExprPtr,
        //         "a"
        //     ),
        //     std::make_shared<facebook::velox::exec::ConstantExpr>(
        //         vectorMaker_.constantVector<int64_t>({1}))},
        //     false);
    }

    // Copy from DecoderUtilTest, nonNullsFromSparse
    void test3() {
        // We cover cases with different null frequencies and different density of
        // access.
        constexpr int32_t kSize = 2000;
        for (auto nullsIn1000 = 1; nullsIn1000 < 1011; nullsIn1000 += 10) {
            for (auto rowsIn1000 = 1; rowsIn1000 < 1011; rowsIn1000 += 10) {
                raw_vector<int32_t> rows;
                // Have an extra word at the end to allow 64 bit access.
                std::vector<uint64_t> nulls(bits::nwords(kSize) + 1);
                randomBits(nulls, 1000 - nullsIn1000);
                randomRows(kSize, rowsIn1000, rows);
                if (rows.empty()) {
                    // The operation is not defined for 0 rows.
                    rows.push_back(1234);
                }
                testNonNullFromSparseCases(nulls.data(), rows);
            }
        }
    }

    // Copy from DecoderUtilTest, processFixedWithRun
    void test4() {
        // Tests processing consecutive batches of integers with processFixedWidthRun.
        constexpr int kSize = 100;
        constexpr int32_t kStep = 17;
        raw_vector<int32_t> data;
        raw_vector<int32_t> scatter;
        data.reserve(kSize);
        scatter.reserve(kSize);
        // Data is 0, 100,  2, 98 ... 98, 2.
        // scatter is 0, 2, 4,6 ... 196, 198.
        for (auto i = 0; i < kSize; i += 2) {
            data.push_back(i / 2);
            data.push_back(kSize - i);
            scatter.push_back(i * 2);
            scatter.push_back((i + 1) * 2);
        }

        // the row numbers that pass the filter come here, translated via scatter.
        raw_vector<int32_t> hits(kSize);
        // Each valid index in 'data'
        raw_vector<int32_t> rows(kSize);
        auto filter = std::make_unique<common::BigintRange>(40, 1000, false);
        std::iota(rows.begin(), rows.end(), 0);
        // The passing values are gathered here. Before each call to
        // processFixedWidthRun, the candidate values are appended here and
        // processFixedWidthRun overwrites them with the passing values and sets
        // numValues to be the first unused index after the passing values.
        raw_vector<int32_t> results;
        int32_t numValues = 0;
        for (auto rowIndex = 0; rowIndex < kSize; rowIndex += kStep) {
            int32_t numInput = std::min<int32_t>(kStep, kSize - rowIndex);
            results.resize(numValues + numInput);
            std::memcpy(
                results.data() + numValues,
                data.data() + rowIndex,
                numInput * sizeof(results[0]));

            facebook::velox::dwio::common::NoHook noHook;
            facebook::velox::dwio::common::processFixedWidthRun<int32_t, false, true, false>(
                rows,
                rowIndex,
                numInput,
                scatter.data(),
                results.data(),
                hits.data(),
                numValues,
                *filter,
                noHook);
        }
        // // Check that each value that passes the filter is in 'results' and that   its
        // // index times 2 is in 'data' is in 'hits'. The 2x is because the scatter maps
        // // each row to 2x the row number.
        // int32_t passedCount = 0;
        // for (auto i = 0; i < kSize; ++i) {
        //     if (data[i] >= 40) {
        //         EXPECT_EQ(data[i], results[passedCount]);
        //         EXPECT_EQ(i * 2, hits[passedCount]);
        //         ++passedCount;
        //     }
        // }
    }

    folly::Random::DefaultGenerator rng_;
};

} // namespace duckdb