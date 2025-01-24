#ifndef ISOMORPHISM_CHECKER
#define ISOMORPHISM_CHECKER

#include <memory>

#include "cuckoofilter.h"

class IsoMorphismChecker {
public:
    IsoMorphismChecker() {}
    ~IsoMorphismChecker() {}

    virtual void initialize(uint64_t num_max_items) = 0;
    virtual void addToSet(uint64_t edge_id) = 0;
    virtual bool checkIsoMorphism(uint64_t edge_id) = 0;
    virtual void removeFromSet(uint64_t edge_id) = 0;
    virtual bool isProbabilistic() = 0;
};

class CuckooIsoChecker : public IsoMorphismChecker {
public:
    CuckooIsoChecker() {}
    ~CuckooIsoChecker() {}

    virtual void initialize(uint64_t num_max_items) {
        filter = std::make_unique<cuckoofilter::CuckooFilter<uint64_t, 12>>(num_max_items);
    }

    virtual void addToSet(uint64_t edge_id) {
        auto rt = filter->Add(edge_id);
        D_ASSERT(rt == cuckoofilter::Ok);
    }

    virtual bool checkIsoMorphism(uint64_t edge_id) {
        auto rt = filter->Contain(edge_id);
        return rt == cuckoofilter::Ok;
    }

    virtual void removeFromSet(uint64_t edge_id) {
        D_ASSERT(filter->Contain(edge_id) == cuckoofilter::Ok);
        auto rt = filter->Delete(edge_id);
        D_ASSERT(rt == cuckoofilter::Ok);
    }

    virtual bool isProbabilistic() {
        return true;
    }

private:
    std::unique_ptr<cuckoofilter::CuckooFilter<uint64_t, 12>> filter;
};


#endif // ISOMORPHISM_CHECKER
