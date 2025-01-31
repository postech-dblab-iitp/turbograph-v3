#ifndef AGGREGATOR_H
#define AGGREGATOR_H

/*
 * Design of the Aggregator
 *
 * This class provides a set of unified APIs that supports local and 
 * global aggregation of variables that have system-provided primitive 
 * data types and use system-provided primitive aggregate operations.
 */

#include "Global.hpp"

template <typename T, Op op = UNDEFINED>
class Aggregator {

    public:

    /* Construct the class */
    Aggregator() {
        /* Get identity value */
        iden_elem = idenelem<T>(op);
        /* Construct the aggregateed value for each thread */
        num_threads = NumaHelper::num_cores();
        per_thread_agg = new T[num_threads];
        for (int i = 0; i < num_threads; i++)
            per_thread_agg[i] = iden_elem;
        /* Set the global aggregated value */
        agg_val = iden_elem;
    }
    ~Aggregator() {}


    /* Initialize the aggregator */
    void Init(int num_threads_) {
        /* Get identity value */
        iden_elem = idenelem<T>(op);
        /* Construct the aggregateed value for each thread */
        num_threads = num_threads_;
        per_thread_agg = new T[num_threads];
        //per_thread_agg = (T*) NumaHelper::alloc_numa_local_memory(sizeof(T) * num_threads);
        for (int i = 0; i < num_threads; i++)
            per_thread_agg[i] = iden_elem;
        /* Set the global aggregated value */
        agg_val = iden_elem;
    }

    /* 
    * Do operation: (thread-locally aggregated value) = (thread-locally aggregated value) op (val) 
    * The strategy that uses the thread-localy aggregated value reduces contention to the globally aggregated value
    */
    inline void Accumulate(T val) {
        int tid = TG_ThreadContexts::thread_id;
        //per_thread_agg[val] += val;
        Operation<T, op>(&per_thread_agg[tid], val);
        //AtomicOperation<T, op>(&agg_val[tid], val);
    }
    
    /* 
    * Do operation: (thread-locally aggregated value) = ((thread-locally aggregated value) inverseed op (old_val)) op (val) 
    * The strategy that uses the thread-localy aggregated value reduces contention to the globally aggregated value
    */
    inline void Accumulate(T val, T old_val) {
        int tid = TG_ThreadContexts::thread_id;
        //per_thread_agg[val] += val;
        UpdateAggregation<T, op>(&per_thread_agg[tid], old_val, val);
        //AtomicOperation<T, op>(&agg_val[tid], val);
    }

    /* Gather the all thread-locally aggegated value and apply it to the globally aggregated value */
    T Reduce() {
        for (int i = 0; i < num_threads; i++)
            Operation<T, op>(&agg_val, per_thread_agg[i]);
        for (int i = 0; i < num_threads; i++)
            per_thread_agg[i] = iden_elem;
        MPI_Datatype mpi_type_ = PrimitiveType2MPIType<T>(0);
        // TODO add mpi_optype = PrimitiveType2MPIOpType... 
        T agg_val_total = agg_val;
        MPI_Allreduce(MPI_IN_PLACE, &agg_val_total, 1, mpi_type_, MPI_SUM, MPI_COMM_WORLD);
        return agg_val_total;
    }

    /* Get the globally aggregated value */
    T GetVal() { return agg_val; }
    void Reset() { agg_val = iden_elem; }

	T* per_thread_agg; /* thread-locally aggegated values */
    //thread_local T per_thread_agg_;
    T agg_val; // Final aggregated value /* globally aggegated value */
    T iden_elem; /* the identity value of this aggregator */
    int num_threads; /* the number of threads that access to this aggregator */
};

#endif

