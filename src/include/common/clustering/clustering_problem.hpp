#ifndef CLUSTERING_PROBLEM_HPP
#define CLUSTERING_PROBLEM_HPP

#include <vector>
#include <string>

class ClusteringProblem {

public:
    enum class ProblemType {
        REAL,
        INT,
    };

    struct ProblemVars {
        std::string name;
        ProblemType type;
        
    };

public:
    ClusteringProblem() {}
    virtual ~ClusteringProblem() {}

    double find_best(std::vector<double> &obj_values) {
        return 0.0;
    }

    double find_worst(std::vector<double> &obj_values) {
        return 0.0;
    }

    //! Generate Random Solution
    void rand_sol() {
        // xhat = rand(this.var_size);
    }

    //! Generate, Decode and Evaluate Random Solution
    void rand_sol_eval() {
        // xhat = this.rand_sol();
        // [z, sol] = this.decode_and_eval(xhat);
    }

    //! Decode (Parse) Coded Solution
    void decode() {
        // sol = ypea_decode_solution(this.vars, xhat);
    }

    //! Evaluate Solution
    void eval() {
        // z = this.obj_func(sol);
    }

    //! Decode and Evaluate Coded Solution
    void decode_and_eval() {
        // sol = this.decode(xhat);
        // z = this.eval(sol);
    }

};

#endif