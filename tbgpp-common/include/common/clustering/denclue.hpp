/* 
 *  Copyright 2006 Andre Cardoso de Souza
 *  
 *  
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  */

#ifndef DENCLUE_HPP
#define DENCLUE_HPP

/** INCLUSIONS **/
#include <getopt.h>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>
// #include "hypercube.h"
// #include "hyperspace.h"
// #include "dataset.h"
// #include "denclue_functions.h"
using namespace std;

#define MAX_FILENAME 64
#define MAXSIZE_LINE 1024

template <typename T>
class DENCLUE {

    using DistanceFunc = std::function<double(const T &, const T &)>;

   private:
    DistanceFunc &disfunc;
    double sigma;
    double xi;

    double calculateInfluence(const T &point, const T &neighbor, double sigma)
    {
        double distance = disfunc(point, neighbor);
        if (distance == 0) {
            return 0;
        }

        double exponent = -powl(distance, 2) / (2.0 * powl(sigma, 2));
        double influence = expl(exponent);
        return influence;
    }

    double calculateDensity(const T &point, const std::vector<T> &points,
                            std::vector<std::size_t> &neighbor)
    {
        double density = 0;
        for (auto i = 0; i < neighbor.size(); i++) {
            density += calculateInfluence(point, points[neighbor[i]], sigma);
        }
        return density;
    }

   public:
    DENCLUE(DistanceFunc &distanceFunc, double sigma, double xi)
        : disfunc(distanceFunc), sigma(sigma), xi(xi)
    {}
    ~DENCLUE() {}

    void Run(const std::vector<T> &points)
    {
        std::vector<std::vector<std::size_t>> neighbors;
        std::vector<double> densities(points.size(), 0.0);

        /* Calculate density of each entity */
        HyperSpace::EntityIterator hs_iter(spatial_region);

        for (auto i = 0; i < points.size(); i++) {
            double curr_density = calculateDensity(points[i], points, neighbors[i]);
            densities[i] = curr_density;
        }

        // for (hs_iter.begin(); !hs_iter.end(); hs_iter++) {
        //     HyperSpace::EntityIterator calculation_iter(spatial_region);
        //     calculation_iter.begin();

        //     double curr_density = DenclueFunctions::calculateDensity(
        //         *hs_iter, calculation_iter, args.sigma);

        //     hs_iter->setDensity(curr_density);
        // }

        // cout << "Densities calculated, determining density-attractors" << endl;

        /* Determine density attractors and entities attracted by each of them */
        map<string, vector<DatasetEntity>>
            clusters;  // Map density-attractors to entities
        map<string, vector<DatasetEntity>> clusters;

        for (auto i = 0; i < points.size(); i++) {
            
        }

        HyperSpace::EntityIterator iter_entities(spatial_region);
        iter_entities.begin();
        while (!iter_entities.end()) {

            HyperSpace::EntityIterator attractor_entity_iter(spatial_region);
            attractor_entity_iter.begin();

            DatasetEntity curr_attractor =
                DenclueFunctions::getDensityAttractor(
                    *iter_entities, spatial_region, attractor_entity_iter,
                    args.sigma);

            // Ignores density-attractors that don't satisfy minimum density
            // restriction
            if (curr_attractor.getDensity() < args.xi) {
                iter_entities++;
                continue;
            }

            // Create a new cluster if necessary
            if (clusters.count(curr_attractor.getStringRepresentation()) <= 0) {

                vector<DatasetEntity> *vec = new vector<DatasetEntity>();
                clusters.insert(
                    make_pair(curr_attractor.getStringRepresentation(), *vec));
            }

            // Assign current entity to the cluster represented by its density-attractor
            clusters[curr_attractor.getStringRepresentation()].push_back(
                *iter_entities);

            // Move cursosr to next entity
            iter_entities++;
        }

        cout << "Density attractors determined, determining clusters" << endl;

        /* Merge clusters with a path between them */
        map<string, vector<DatasetEntity>>::iterator outer_iter =
            clusters.begin();
        while (outer_iter != clusters.end()) {

            // Try to merge a pair of clusters
            map<string, vector<DatasetEntity>>::iterator inner_iter =
                outer_iter;
            inner_iter++;

            while (inner_iter != clusters.end()) {

                // Build entities that represent each density-attractor
                ostringstream outer_str;
                ostringstream inner_str;
                outer_str << outer_iter->first << Constants::EOL;
                inner_str << inner_iter->first << Constants::EOL;
                DatasetEntity outer(args.dimension);
                DatasetEntity inner(args.dimension);
                outer.buildEntityFromString(outer_str.str());
                inner.buildEntityFromString(inner_str.str());

                // Mark ends of desired path as used in path's sequence
                map<string, bool> usedEntities;
                usedEntities[inner.getStringRepresentation()] = true;
                usedEntities[outer.getStringRepresentation()] = true;

                bool canMerge = DenclueFunctions::pathBetweenExists(
                    outer, inner, spatial_region, args.xi, args.sigma,
                    usedEntities);

                // Merge clusters if there's an appropriate path between their
                // density-attractors
                if (canMerge) {

                    DenclueFunctions::AppendVector(outer_iter->second,
                                                   inner_iter->second);

                    clusters.erase(
                        inner_iter++);  // Erase appended vector and go to next cluster
                    continue;
                }

                inner_iter++;
            }

            outer_iter++;
        }

        /* Print clusters representation to output file */

        //  printOutput(clusters, args.output_file, args.xi);

        //  cout << "Clusters written to output file " << args.output_filename << endl;
    }
};

// Main function of DENCLUE

#endif
