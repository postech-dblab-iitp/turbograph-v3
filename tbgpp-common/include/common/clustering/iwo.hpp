#ifndef IWO_H
#define IWO_H

#include <cmath>
#include <random>

#include "common/clustering/clustering_algorithm.hpp"
#include "common/clustering/clustering_problem.hpp"

typedef unsigned int uint;

template <typename T, typename Float>
class IWO : public ClusteringAlgorithm {
    using TVector = std::vector<T>;
    using DistanceFunc = std::function<Float(const T &, const T &)>;
public:
    IWO() {}
    ~IWO() {}

    void initialize();
    int Run(
        TVector *V, const uint dim, const Float eps, const uint min,
        const DistanceFunc &disfunc = [](const T &t1, const T &t2) -> Float {
            return 0;
        });

private:
    //! Initial Population Size
    int initial_pop_size = 10;
    
    //! Minimum Seed Count
    int min_seed_count = 0;

    //! Maximum Seed Count
    int max_seed_count = 5;

    //! Step size
    double step_size = 0.1;

    //! Step Size Damp Rate
    double step_size_damp = 0.001;

    //! Data dimension
    uint _datadim;

};

template <typename T, typename Float>
void IWO<T, Float>::initialize()
{
    // Create Initial Population (Not Sorted)
    bool sorted = false;
    init_pop(sorted);
    
    // Initial Value of Step Size
    this->params.sigma = this->step_size;
}

template <typename T, typename Float>
int IWO<T, Float>::Run(TVector *V, const uint dim, const Float eps,
                          const uint min, const DistanceFunc &disfunc)
{
    // % Minimum and Maximum Number of Seeds
    int Smin = this->min_seed_count;
    int Smax = this->max_seed_count;
    
    // % Decision Vector Size
    this->_datadim = dim;
    auto var_size = this->_datadim; // var_size = this.problem.var_size;
    
    // % Get Best and Worst Cost Values
    auto &obj_values = this->pop.obj_values;
    double best_obj_value = this->problem->find_best(obj_values);
    double worst_obj_value = this->problem->find_worst(obj_values);
    
    // % Initialize Offsprings Population
    // newpop = repmat(this.empty_individual, numel(this.pop)*Smax, 1);
    std::vector<std::vector<int>> newpop;
    newpop.resize(this->pop.size() * Smax);
    int c = 0;
    
    std::random_device rd{};
    std::mt19937 gen{rd()};
    std::normal_distribution d{0.0, 1.0};

    // % Reproduction
    for (auto i = 0; i < this->pop.size(); i++) {
        double ratio = (this->pop.obj_values[i] - worst_obj_value) /
                       (best_obj_value - worst_obj_value);
        int S = std::floor(Smin + (Smax - Smin) * ratio);

        for (auto j = 0; j < S; j++) {
            // % Generate Random Location
            // xnew = this.pop(i).position + this.params.sigma * randn(var_size);
            // newsol = this.new_individual(xnew);
            
            // Add Offpsring to the Population
            c++;
            // newpop(c) = newsol;
        }
    }

    // newpop = newpop(1:c);
    
    // % Merge and Sort Populations
    // this.pop = this.sort_and_select([this.pop; newpop]);

    // % Update Best Solution Ever Found
    // this.best_sol = this.pop(1);
    
    // % Damp Step Size
    this->params.sigma = this->step_size_damp * this->params.sigma;
}

#endif
