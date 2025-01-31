#ifndef CLUSTERING_ALGORITHM_HPP
#define CLUSTERING_ALGORITHM_HPP

#include <vector>
#include <memory>

#include "common/clustering/clustering_problem.hpp"

class ClusteringAlgorithm {

// struct Individual {
//     std::vector<int> data;
//     double obj_value;
// };
protected:
    struct ClusteringParams {
        double sigma;
    };

    struct Population {
        std::vector<std::vector<int>> pop;
        std::vector<double> obj_values;
        std::vector<double> solution;
        std::vector<double> position;
    };

public:
    ClusteringAlgorithm() {}
    virtual ~ClusteringAlgorithm() {}

    virtual int Run() = 0;

public:
    //! Create a New Individual
    void new_individual() {
        // if ~exist('x', 'var') || isempty(x)
        //     x = rand(this.problem.var_size);
        // end
        
        // ind = this.empty_individual;
        // ind.position = ypea_clip(x, 0, 1);
        // [ind.obj_value, ind.solution] = this.decode_and_eval(x);
    }
    
    //! Initialize Population
    void init_pop(bool sorted_ = false) {
        // % Initialize Population Array
        // this.pop = repmat(this.empty_individual, this.pop_size, 1);
        pop.pop.resize(pop_size);
        
        // % Initialize Best Solution to the Worst Possible Solution
        // this.best_sol = this.empty_individual;
        // this.best_sol.obj_value = this.problem.worst_value;
        
        // % Generate New Individuals
        for (auto i = 0; i < pop_size; i++) {
        //     % Generate New Solution
        //     this.pop(i) = this.new_individual();
            
        //     % Compare to the Best Solution Ever Found
        //     if ~sorted && this.is_better(this.pop(i), this.best_sol)
        //         this.best_sol = this.pop(i);
        //     end
        }
        
        // % Sort the Population if it is needed
        if (sorted_) {
            //     this.pop = this.sort_population(this.pop);
            //     this.best_sol = this.pop(1);
        }
    }

     //! Clips the inputs, and ensures the lower and upper bounds
    void ypea_clip() {
        // y = min(max(x, lb), ub);
    }

protected:
    //! Population Array
    Population pop;

    //! Population Size
    int pop_size = 100;

    //! Clustering Parameters
    ClusteringParams params;

    //! The Optimization Problem being solved by the algorithm
    std::unique_ptr<ClusteringProblem> problem;
};

#endif
