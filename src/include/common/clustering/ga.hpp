#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <vector>

// Helper function to calculate Euclidean distance
double euclideanDistance(const std::vector<double> &a,
                         const std::vector<double> &b)
{
    double sum = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        sum += (a[i] - b[i]) * (a[i] - b[i]);
    }
    return std::sqrt(sum);
}

double _ComputeVecOvh(size_t num_tuples) {
    if (num_tuples > 1024) return 1;
    else return (double) 1024 / num_tuples;
}

double DistanceFunc(const std::pair<std::vector<uint64_t>, uint64_t> &a,
                    const std::pair<std::vector<uint64_t>, uint64_t> &b)
{
    const double CostSchemaVal = 0.0001;
    const double CostNullVal = 0.01;
    const double CostVectorizationVal = 0.2;
    double cost_current =
        2 * CostSchemaVal + _ComputeVecOvh(a.second) + _ComputeVecOvh(b.second);

    int64_t num_nulls1 = 0;
    int64_t num_nulls2 = 0;
    uint64_t i = 0;
    uint64_t j = 0;
    while (i < a.first.size() && j < b.first.size()) {
        if (a.first[i] == b.first[j]) {
            i++;
            j++;
        }
        else if (a.first[i] < b.first[j]) {
            num_nulls1++;
            i++;
        }
        else {
            num_nulls2++;
            j++;
        }
    }
    while (i < a.first.size()) {
        num_nulls1++;
        i++;
    }
    while (j < b.first.size()) {
        num_nulls2++;
        j++;
    }

    double cost_after =
        CostSchemaVal +
        CostNullVal * (num_nulls1 * a.second + num_nulls2 * b.second) +
        _ComputeVecOvh(a.second + b.second);
    double distance = cost_after / cost_current;
}

class Individual {
   public:
    std::vector<double> genes;
    std::vector<uint64_t> genes_idx;
    int dim;
    std::vector<std::pair<std::vector<uint64_t>, uint64_t>> *data;

   public:
    Individual(std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x, int k,
               std::vector<double> genes_init)
        : genes(genes_init)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        if (!x.empty()) {
            for (int i = 0; i < k; ++i) {
                std::uniform_int_distribution<> dis(0, x.size() - 1);
                int idx = dis(gen);
                // const std::vector<double> &point = x[idx];
                // this->genes.insert(this->genes.end(), point.begin(),
                //                    point.end());
                this->genes_idx.push_back(idx);
            }
            // this->dim = x[0].first.size();
        }
        else {
            // this->dim = genes.size() / k;
        }
        this->data = &x;
    }

    std::vector<int> assign(std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x)
    {
        std::vector<int> output;
        for (const auto &point : x) {
            std::vector<double> distance;
            for (size_t index = 0; index < this->genes_idx.size(); ++index) {
                // std::vector<double> centroid(
                //     this->genes.begin() + index * this->dim,
                //     this->genes.begin() + (index + 1) * this->dim);
                // distance.push_back(DistanceFunc(point, centroid));
                distance.push_back(DistanceFunc(point, x[this->genes_idx[index]]));
            }
            output.push_back(std::distance(
                distance.begin(),
                std::min_element(distance.begin(), distance.end())));
        }
        return output;
    }

    void update(std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x,
                const std::vector<int> &output)
    {
        // for (size_t index = 0; index < this->genes.size() / this->dim;
        //      ++index) {
        //     std::vector<size_t> xi;
        //     for (size_t i = 0; i < output.size(); ++i) {
        //         if (output[i] == index) {
        //             xi.push_back(i);
        //         }
        //     }
        //     if (!xi.empty()) {
        //         for (int d = index * this->dim; d < (index + 1) * this->dim;
        //              ++d) {
        //             double sum = 0;
        //             for (size_t item : xi) {
        //                 sum += x[item][d % this->dim];
        //             }
        //             this->genes[d] = sum / xi.size();
        //         }
        //     }
        // }
        for (size_t index = 0; index < this->genes_idx.size(); ++index) {
            std::vector<size_t> xi;
            for (size_t i = 0; i < output.size(); ++i) {
                if (output[i] == index) {
                    xi.push_back(i);
                }
            }
            if (!xi.empty()) {
                for (int d = index * this->dim; d < (index + 1) * this->dim;
                     ++d) {
                    double sum = 0; // TODO
                    // for (size_t item : xi) {
                    //     sum += x[item][d % this->dim];
                    // }
                    this->genes[d] = sum / xi.size();
                }
            }
        }
    }

    double fitness(std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x)
    {
        auto output = assign(x);
        update(x, output);
        auto inter = intercluster();
        auto intra = intracluster(x, output);
        return *std::min_element(inter.begin(), inter.end()) /
               *std::max_element(intra.begin(), intra.end());
    }

    std::vector<double> intercluster()
    {
        std::vector<double> inter;
        // for (int index = 0; index < this->genes.size() / this->dim; ++index) {
        //     for (int j = index + 1; j < this->genes.size() / this->dim; ++j) {
        //         std::vector<double> vec1(
        //             this->genes.begin() + index * this->dim,
        //             this->genes.begin() + (index + 1) * this->dim);
        //         std::vector<double> vec2(
        //             this->genes.begin() + j * this->dim,
        //             this->genes.begin() + (j + 1) * this->dim);
        //         inter.push_back(DistanceFunc(vec1, vec2));
        //     }
        // }
        for (int index = 0; index < this->genes_idx.size(); ++index) {
            for (int j = index + 1; j < this->genes_idx.size(); ++j) {
                // std::vector<double> vec1(
                //     this->genes.begin() + index * this->dim,
                //     this->genes.begin() + (index + 1) * this->dim);
                // std::vector<double> vec2(
                //     this->genes.begin() + j * this->dim,
                //     this->genes.begin() + (j + 1) * this->dim);
                inter.push_back(DistanceFunc(this->data->at(index), this->data->at(j)));
            }
        }
        return inter;
    }

    std::vector<double> intracluster(std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x,
                                     const std::vector<int> &output)
    {
        std::vector<double> intra;
        for (int index = 0; index < this->genes.size() / this->dim; ++index) {
            std::vector<size_t> xi;
            for (size_t i = 0; i < output.size(); ++i) {
                if (output[i] == index) {
                    xi.push_back(i);
                }
            }
            double dmax = 0.0;
            for (size_t m = 0; m < xi.size(); ++m) {
                for (size_t n = m + 1; n < xi.size(); ++n) {
                    double d = DistanceFunc(x[xi[m]], x[xi[n]]);
                    if (d > dmax)
                        dmax = d;
                }
            }
            intra.push_back(dmax);
        }
        return intra;
    }

    // TODO can be used for non-numeric data?
    void mutation(double pmut)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0.0, 1.0);

        for (double &gene : genes) {
            if (dis(gen) <= pmut) {
                double delta = dis(gen);
                if (dis(gen) <= 0.5) {
                    gene -= 2 * delta * gene;
                }
                else {
                    gene += 2 * delta * gene;
                }
            }
        }
    }
};

// Initialize the population of individuals
std::vector<std::unique_ptr<Individual>> GAPopulationInit(
    int npop, const std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x, int k)
{
    std::vector<std::unique_ptr<Individual>> population;
    for (int i = 0; i < npop; ++i) {
        population.push_back(
            std::make_unique<Individual>(x, k, std::vector<double>()));
    }
    return population;
}

// Perform crossover between two parents to generate two children
std::pair<std::unique_ptr<Individual>, std::unique_ptr<Individual>> Crossover(
    const Individual &parent1, const Individual &parent2, int k)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, parent1.genes.size() - 2);

    int point = dis(gen);

    std::vector<double> genes1(parent1.genes.begin(),
                               parent1.genes.begin() + point);
    genes1.insert(genes1.end(), parent2.genes.begin() + point,
                  parent2.genes.end());

    std::vector<double> genes2(parent2.genes.begin(),
                               parent2.genes.begin() + point);
    genes2.insert(genes2.end(), parent1.genes.begin() + point,
                  parent1.genes.end());

    return {std::make_unique<Individual>(std::vector<std::pair<std::vector<uint64_t>, uint64_t>>(), k,
                                         genes1),
            std::make_unique<Individual>(std::vector<std::pair<std::vector<uint64_t>, uint64_t>>(), k,
                                         genes2)};
}

// Roulette Wheel Selection
const Individual &RouletteWheel(
    const std::vector<std::unique_ptr<Individual>> &population,
    const std::vector<double> &fit)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    double sumf = std::accumulate(fit.begin(), fit.end(), 0.0);
    double offset = 0.0;
    double r = dis(gen) * sumf;

    for (size_t i = 0; i < fit.size(); ++i) {
        offset += fit[i];
        if (offset >= r) {
            return *population[i];
        }
    }
    return *population.back();  // To handle any rounding issues
}

// Main Genetic Algorithm Function
std::vector<double> GeneticAlg(int npop, int k, double pcros, double pmut,
                               int maxit,
                               const std::vector<std::pair<std::vector<uint64_t>, uint64_t>> &x)
{
    std::vector<std::unique_ptr<Individual>> pop = GAPopulationInit(npop, x, k);
    std::vector<double> fit(npop);
    std::transform(pop.begin(), pop.end(), fit.begin(),
                   [&x](auto &indiv) { return indiv->fitness(x); });

    double bestFit = *std::max_element(fit.begin(), fit.end());
    std::vector<double> bestGenes =
        pop[std::distance(fit.begin(),
                          std::max_element(fit.begin(), fit.end()))]
            ->genes;

    std::random_device rd;
    std::mt19937 gen(rd());
    for (int i = 0; i < maxit; ++i) {
        std::vector<std::unique_ptr<Individual>> newPop;
        while (newPop.size() < pop.size()) {
            const Individual &parent1 = RouletteWheel(pop, fit);
            std::uniform_real_distribution<> dis(0.0, 1.0);
            if (dis(gen) <= pcros) {
                const Individual &parent2 = RouletteWheel(pop, fit);
                auto children = Crossover(parent1, parent2, k);
                newPop.push_back(std::move(children.first));
                if (newPop.size() < pop.size()) {
                    newPop.push_back(std::move(children.second));
                }
            }
            else {
                std::unique_ptr<Individual> child =
                    std::make_unique<Individual>(&parent1);
                child->mutation(pmut);
                newPop.push_back(std::move(child));
            }
        }
        pop.swap(newPop);
        std::transform(pop.begin(), pop.end(), fit.begin(),
                       [&x](auto &indiv) { return indiv->fitness(x); });

        double currentBestFit = *std::max_element(fit.begin(), fit.end());
        if (currentBestFit > bestFit) {
            bestFit = currentBestFit;
            bestGenes =
                pop[std::distance(fit.begin(),
                                  std::max_element(fit.begin(), fit.end()))]
                    ->genes;
        }
    }

    return bestGenes;
}

// int main()
// {
//     // Example usage
//     std::vector<std::vector<double>> x = {{0.1, 0.2}, {0.4, 0.6}};
    
//     // npop, k, pcros, pmut, maxit, x
//     auto result = GeneticAlg(50, 2, 0.7, 0.1, 100, x);
//     for (auto gene : result) {
//         std::cout << gene << " ";
//     }
//     return 0;
// }