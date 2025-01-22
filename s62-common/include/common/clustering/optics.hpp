#define _SCL_SECURE_NO_WARNINGS
// Copyright Ingo Proff 2016.
// https://github.com/CrikeeIP/OPTICS-Clustering
// Distributed under the MIT Software License (X11 license).
// (See accompanying file LICENSE)

#pragma once

#ifndef _HAS_AUTO_PTR_ETC
#define _HAS_AUTO_PTR_ETC 1

#ifndef _SILENCE_CXX17_ITERATOR_BASE_CLASS_DEPRECATION_WARNING
#define _SILENCE_CXX17_ITERATOR_BASE_CLASS_DEPRECATION_WARNING 1
#endif
#ifndef _SILENCE_CXX17_OLD_ALLOCATOR_MEMBERS_DEPRECATION_WARNING
#define _SILENCE_CXX17_OLD_ALLOCATOR_MEMBERS_DEPRECATION_WARNING 1
#endif

// #include <boost/geometry.hpp>
// #include <boost/geometry/geometries/box.hpp>
// #include <boost/geometry/geometries/point.hpp>
// #include <boost/geometry/index/rtree.hpp>

#undef _HAS_AUTO_PTR_ETC

#else
static_assert(_HAS_AUTO_PTR_ETC,
              "_HAS_AUTO_PTR_ETC has to be 1 for boost includes in MSVC_17, "
              "but has externally already been set to 0");
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/index/rtree.hpp>
#endif

// #include "bgr_image.hpp"
// #include "kdTree.hpp"
// #include "nanoflann.hpp"
// #include "tree.hpp"

#include "fplus.hpp"
// #include <geometry/geometry.hpp>

#include <exception>
#include <vector>

namespace optics {

typedef std::pair<std::size_t, std::size_t> chi_cluster_indices;
// typedef optics::Tree<chi_cluster_indices> cluster_tree;

// template <typename T>
// using Point = std::array<T>;
template <typename T>
using DistanceFunc = std::function<double(const T &, const T &)>;

struct reachability_dist {
    reachability_dist(std::size_t point_index_, double reach_dist_)
        : point_index(point_index_), reach_dist(reach_dist_)
    {}

    std::string to_string() const
    {
        return "{" + std::to_string(point_index) + "," +
               std::to_string(reach_dist) + "}";
    }
    std::size_t point_index;
    double reach_dist;
};

inline bool operator<(const reachability_dist &lhs,
                      const reachability_dist &rhs)
{
    return (lhs.reach_dist <= rhs.reach_dist &&
            lhs.reach_dist >= rhs.reach_dist)
               ? (lhs.point_index < rhs.point_index)
               : (lhs.reach_dist < rhs.reach_dist);
}
inline bool operator==(const reachability_dist &lhs,
                       const reachability_dist &rhs)
{
    return (lhs.reach_dist <= rhs.reach_dist &&
            lhs.reach_dist >= rhs.reach_dist) &&
           (lhs.point_index == rhs.point_index);
}
inline std::ostringstream &operator<<(std::ostringstream &stream,
                                      const reachability_dist &r)
{
    stream << r.to_string();
    return stream;
}

template <typename T>
void find_neighbor_indices(
    const std::vector<T> &points,
    double epsilon, std::vector<std::vector<std::size_t>> &neighbors,
    const DistanceFunc<T> &disfunc)
{
    // brute force way
    for (std::size_t i = 0; i < points.size(); ++i) {
        for (std::size_t j = 0; j < points.size(); ++j) {
            if (i == j)
                continue;
            double dist = disfunc(points[i], points[j]);
            if (dist < epsilon) {
                neighbors[i].push_back(j);
            }
        }
    }
}

template <typename T>
fplus::maybe<double> compute_core_dist(
    const T &point, const std::vector<T> &points,
    const std::vector<std::size_t> &neighbor_indices, const std::size_t min_pts,
    const DistanceFunc<T> &disfunc)
{
    if (neighbor_indices.size() < min_pts) {
        return {};
    }

    auto core_elem_idx = fplus::nth_element_on(
        [&points, &point, &disfunc](const std::size_t &idx) -> double {
            return disfunc(point, points[idx]);
        },
        min_pts - 1, neighbor_indices);
    double core_dist = disfunc(points[core_elem_idx], point);
    return core_dist;
}

inline void erase_idx_from_set(const reachability_dist &d,
                               std::set<reachability_dist> &seeds)
{
    auto x = seeds.erase(d);
    assert(x == 1);
}

template <typename T>
T pop_from_set(std::set<T> &set)
{
    T element = *set.begin();
    set.erase(set.begin());
    return element;
}

template <typename T>
void update(const T &point, const std::vector<T> &points,
            const std::vector<std::size_t> &neighbor_indices,
            const double core_dist, const std::vector<bool> &processed,
            std::vector<double> &reachability,
            std::set<reachability_dist> &seeds)
{
    for (const auto &o : neighbor_indices) {
        if (processed[o]) {
            continue;
        }
        double new_reachability_dist;// =
            // fplus::max(core_dist, geom::dist<T, N>(point, points[o]));
        if (reachability[o] < 0.0) {
            reachability[o] = new_reachability_dist;
            seeds.insert(reachability_dist(o, new_reachability_dist));
        }
        else if (new_reachability_dist < reachability[o]) {
            //erase from seeds
            erase_idx_from_set(reachability_dist(o, reachability[o]), seeds);
            //update reachability
            reachability[o] = new_reachability_dist;
            //re-insert seed with new reachability
            seeds.insert(reachability_dist(o, new_reachability_dist));
        }
    }
}

// template <typename T, std::size_t dimension>
// std::pair<Point<T, dimension>, Point<T, dimension>> bounding_box(
//     const std::vector<Point<T, dimension>> &points)
// {
//     assert(points.size() > 0);  //Bounding Box of 0 points not defined
//     static_assert(std::is_convertible<T, double>::value,
//                   "bounding_box(): bounding_box can only be computed for point "
//                   "types which can be converted to double!");

//     std::array<T, dimension> min(points[0]);
//     std::array<T, dimension> max(points[1]);

//     for (const auto &p : points) {
//         for (std::size_t i = 0; i < dimension; i++) {
//             if (p[i] < min[i])
//                 min[i] = p[i];
//             if (p[i] > max[i])
//                 max[i] = p[i];
//         }
//     }

//     return {{min}, {max}};
// }

// template <typename T, std::size_t dimension>
// double hypercuboid_voulume(const Point<T, dimension> &bl,
//                            const Point<T, dimension> &tr)
// {
//     double volume = 1;
//     for (std::size_t i = 0; i < dimension; i++) {
//         volume *= std::abs(static_cast<double>(tr[i] - bl[i]));
//     }
//     return volume;
// }

// template <typename T, std::size_t dimension>
// double epsilon_estimation(const std::vector<Point<T, dimension>> &points,
//                           const std::size_t min_pts)
// {
//     // static_assert(std::is_convertible<double, T>::value,
//     //               "optics::epsilon_estimation: Point type 'T' must be "
//     //               "convertible to double!");
//     // static_assert(dimension >= 1,
//     //               "optics::epsilon_estimation: dimension must be >=1");
//     if (points.size() <= 1) {
//         return 0;
//     }

//     double d = static_cast<double>(dimension);
//     auto space = bounding_box(points);
//     double space_volume = hypercuboid_voulume(space.first, space.second);

//     double space_per_minpts_points =
//         (space_volume / static_cast<double>(points.size())) *
//         static_cast<double>(min_pts);
//     double n_dim_unit_ball_vol =
//         std::sqrt(std::pow(geom::pi, d)) / std::tgamma(d / 2.0 + 1.0);
//     double r = std::pow(space_per_minpts_points / n_dim_unit_ball_vol, 1.0 / d);

//     //double nominator = space_volume * static_cast<double>(min_pts) * std::tgamma( d/2.0 + 1.0 );
//     //double denominator = static_cast<double>(points.size()) * std::sqrt( std::pow( geom::pi, d ) );
//     //double r = std::pow( nominator / denominator, 1.0 / d );
//     return r;
// }

// template <typename T, std::size_t dimension>
// PointCloud<T, dimension> toPointCloud(
//     const std::vector<Point<T, dimension>> &points)
// {
//     static_assert(std::is_signed<T>::value,
//                   "Type not allowed. Only Integers, Float & Double supported");
//     static_assert(std::is_convertible<double, T>::value,
//                   "optics::compute_reachability_dists: Point type 'T' must be "
//                   "convertible to double!");
//     static_assert(dimension >= 1,
//                   "optics::compute_reachability_dists: dimension must be >=1");
//     if (points.empty()) {
//         return {};
//     }

//     PointCloud<T, dimension> cloud;
//     cloud.pts.reserve(points.size());
//     for (const auto &p : points) {
//         cloud.pts.push_back(p);
//     }

//     return cloud;
// }

enum RadiusSearchMethod { S62, NANOFLANN, KDTREE, BOOSTRSTAR };
static const RadiusSearchMethod method = S62;

// const std::vector<std::array<T, dimension>> &points,
// template <std::size_t n_points, typename T, std::size_t dimension>
template <typename T>
std::vector<reachability_dist> compute_reachability_dists(
    const std::vector<T> &points, std::size_t min_pts, double epsilon,
    const DistanceFunc<T> &disfunc)
{
    // static_assert(std::is_signed<T>::value,
    //               "Type not allowed. Only Integers, Float & Double supported");
    // static_assert(std::is_convertible<T, double>::value,
    //               "Point type 'T' must be convertible to double!");
    // static_assert(dimension >= 1, "dimension must be >=1");
    // static_assert(n_points >= 2, "Number of points to cluster must be >= 2");

    // if (points.size() != n_points) {
    //     std::cerr << "Error: provided vector of points does not have expected "
    //                  "length n_points";
    //     std::exit(1);  //points.size() must be == n_points for the kdTree
    // }
    if (points.empty()) {
        return {};
    }

    // if (epsilon <= 0.0) {
    //     epsilon = epsilon_estimation(points, min_pts);
    // }
    assert(epsilon > 0);

    //algorithm tracker
    std::vector<bool> processed(points.size(), false);
    std::vector<std::size_t> ordered_list;
    ordered_list.reserve(points.size());
    std::vector<double> reachability(points.size(), -1.0f);

    std::vector<std::vector<std::size_t>> neighbors;
    switch (method) {
        case S62: {
            neighbors.resize(points.size());
            find_neighbor_indices(points, epsilon, neighbors, disfunc);
            break;
        }
        default:
            std::cerr << "RadiusSearchMethod " << method << " not implemented!"
                      << std::endl;
            std::exit(11);
    }

    assert(neighbors.size() == points.size());

    for (std::size_t point_idx = 0; point_idx < points.size(); point_idx++) {
        if (processed[point_idx] == true)
            continue;
        processed[point_idx] = true;
        ordered_list.push_back(point_idx);
        std::set<reachability_dist> seeds;

        auto neighbor_indices = neighbors[point_idx];

        fplus::maybe<double> core_dist_m = compute_core_dist(
            points[point_idx], points, neighbor_indices, min_pts, disfunc);
        if (!core_dist_m.is_just()) {
            continue;
        }
        double core_dist = core_dist_m.unsafe_get_just();

        update(points[point_idx], points, neighbor_indices, core_dist,
               processed, reachability, seeds);
        while (!seeds.empty()) {
            reachability_dist s = pop_from_set(seeds);
            assert(processed[s.point_index] == false);
            processed[s.point_index] = true;
            ordered_list.push_back(s.point_index);

            auto s_neighbor_indices = neighbors[s.point_index];

            auto s_core_dist_m =
                compute_core_dist(points[s.point_index], points,
                                  s_neighbor_indices, min_pts, disfunc);
            if (!s_core_dist_m.is_just()) {
                continue;
            }
            double s_core_dist = s_core_dist_m.unsafe_get_just();

            update(points[s.point_index], points, s_neighbor_indices,
                   s_core_dist, processed, reachability, seeds);
        }
    }
    //sanity checks
    assert(ordered_list.size() == points.size());
    assert(fplus::all_unique(ordered_list));

    //merge reachabilities into ordered list
    auto result = fplus::transform(
        [&reachability](std::size_t point_idx) -> reachability_dist {
            return reachability_dist(point_idx, reachability[point_idx]);
        },
        ordered_list);
    return result;
}

inline std::vector<std::vector<std::size_t>> get_cluster_indices(
    const std::vector<reachability_dist> &reach_dists,
    double reachability_threshold,
    const std::string &reachability_plot_image_path = "")
{
    assert(reach_dists.front().reach_dist < 0.0);
    std::vector<std::vector<std::size_t>> result;
    for (const auto &r : reach_dists) {
        if (r.reach_dist < 0.0 || r.reach_dist >= reachability_threshold) {
            result.push_back({r.point_index});
        }
        else {
            result.back().push_back(r.point_index);
        }
    }
    return result;
}

struct SDA {
    SDA(std::size_t begin_idx_, std::size_t end_idx_, double mib_)
        : begin_idx(begin_idx_), end_idx(end_idx_), mib(mib_)
    {}
    std::size_t begin_idx;
    std::size_t end_idx;
    double mib;
};

inline std::vector<chi_cluster_indices> get_chi_clusters_flat(
    const std::vector<reachability_dist> &reach_dists_, const double chi,
    std::size_t min_pts, double steep_area_min_diff = 0.0)
{
    std::vector<std::pair<std::size_t, std::size_t>> clusters;
    std::vector<SDA> SDAs;
    const std::size_t n_reachdists = reach_dists_.size();
    double mib(0);
    double max_reach(0.0);
    for (const auto &r : reach_dists_) {
        if (r.reach_dist > max_reach)
            max_reach = r.reach_dist;
    }

    const auto get_reach_dist = [&reach_dists_,
                                 &max_reach](const std::size_t idx) -> double {
        assert(idx <= reach_dists_.size());
        if (idx == reach_dists_.size())
            return max_reach;
        if (idx == 0)
            return max_reach;
        const auto r = reach_dists_[idx].reach_dist;
        return ((r < 0) ? 2 * max_reach : r);
    };
    const auto is_steep_down_pt = [&get_reach_dist, &n_reachdists,
                                   &chi](std::size_t idx) {
        if (idx == 0)
            return true;
        if (idx + 1 >= n_reachdists)
            return false;
        return get_reach_dist(idx + 1) <= get_reach_dist(idx) * (1 - chi);
    };
    const auto is_steep_up_pt = [&get_reach_dist, &n_reachdists,
                                 &chi](std::size_t idx) {
        if (idx + 1 >= n_reachdists)
            return true;
        return get_reach_dist(idx + 1) * (1 - chi) >= get_reach_dist(idx);
    };
    const auto filter_sdas = [&chi, &steep_area_min_diff, &SDAs, &mib,
                              &get_reach_dist]() {
        SDAs = fplus::keep_if(
            [&mib, &chi, &steep_area_min_diff,
             &get_reach_dist](const SDA &sda) -> bool {
                const double f = fplus::max(chi, steep_area_min_diff);
                return mib <= get_reach_dist(sda.begin_idx) * (1 - f);
            },
            SDAs);
        for (auto &sda : SDAs) {
            sda.mib = std::max(sda.mib, mib);
        }
    };
    const auto get_sda_end =
        [&chi, &n_reachdists, &get_reach_dist, &min_pts,
         &is_steep_down_pt](const std::size_t start_idx) -> std::size_t {
        assert(is_steep_down_pt(start_idx));
        std::size_t last_sd_idx = start_idx;
        std::size_t idx = start_idx + 1;
        while (idx < n_reachdists) {
            if (idx - last_sd_idx >= min_pts) {
                return last_sd_idx;
            }
            if (get_reach_dist(idx) > get_reach_dist(idx - 1)) {
                return last_sd_idx;
            }
            if (is_steep_down_pt(idx)) {
                last_sd_idx = idx;
            }
            idx++;
        }
        return std::max(n_reachdists - 2, last_sd_idx);
    };
    const auto get_sua_end =
        [&chi, &n_reachdists, &get_reach_dist, &min_pts,
         &is_steep_up_pt](const std::size_t start_idx) -> std::size_t {
        assert(is_steep_up_pt(start_idx));
        std::size_t last_su_idx = start_idx;
        std::size_t idx = start_idx + 1;
        while (idx < n_reachdists) {
            if (idx - last_su_idx >= min_pts) {
                return last_su_idx;
            }
            if (get_reach_dist(idx) < get_reach_dist(idx - 1)) {
                return last_su_idx;
            }
            if (is_steep_up_pt(idx)) {
                last_su_idx = idx;
            }
            idx++;
        }
        return std::max(n_reachdists - 2, last_su_idx);
    };
    const auto cluster_borders =
        [&get_reach_dist, &n_reachdists, &chi](
            const SDA &sda, std::size_t sua_begin_idx,
            std::size_t sua_end_idx) -> std::pair<std::size_t, std::size_t> {
        double start_reach = get_reach_dist(sda.begin_idx);
        double end_reach =
            get_reach_dist(std::min(sua_end_idx + 1, n_reachdists - 1));
        // if (geom::in_range(start_reach, end_reach, start_reach * chi)) {
        if ((start_reach * chi >= start_reach) &&
            (start_reach * chi <= end_reach)) {
            return {sda.begin_idx, sua_end_idx};
        }
        if (start_reach > end_reach) {
            std::size_t start_idx = sda.begin_idx + 1;
            while (start_idx <= sda.end_idx &&
                   get_reach_dist(start_idx) > end_reach) {
                start_idx++;
            }
            return {start_idx - 1, sua_end_idx};
        }
        if (start_reach < end_reach) {
            std::size_t end_idx = sua_end_idx;
            while (end_idx >= sua_begin_idx &&
                   get_reach_dist(end_idx) >= start_reach) {
                end_idx--;
            }
            return std::make_pair(sda.begin_idx, end_idx + 1);
        }
        assert(false);
        return {0, 0};
    };
    const auto valid_combination =
        [&chi, &steep_area_min_diff, &min_pts, &get_reach_dist](
            const SDA &sda, std::size_t sua_begin_idx,
            std::size_t sua_end_idx) -> bool {
        const double f = fplus::max(chi, steep_area_min_diff);
        if (sda.mib > get_reach_dist(sua_end_idx + 1) * (1 - f)) {
            return false;
        }

        std::size_t sda_middle =
            (sda.begin_idx + (sda.end_idx - sda.begin_idx) / 2);
        std::size_t sua_middle =
            (sua_begin_idx + (sua_end_idx - sua_begin_idx) / 2);
        if (sua_middle - sda_middle < min_pts - 2) {
            return false;
        }

        return true;
    };

    for (std::size_t idx = 0; idx < n_reachdists; idx++) {
        double reach_i = get_reach_dist(idx);

        //Start of Steep Down Area?
        if (idx < n_reachdists && is_steep_down_pt(idx)) {
            if (reach_i > mib) {
                mib = reach_i;
            }
            filter_sdas();
            std::size_t sda_end_idx = get_sda_end(idx);
            if (reach_i * (1.0 - steep_area_min_diff) <
                get_reach_dist(sda_end_idx + 1)) {
                continue;
            }
            SDAs.push_back(SDA(idx, sda_end_idx, 0.0));
            idx = sda_end_idx;
            if (idx < n_reachdists - 1) {
                mib = get_reach_dist(idx + 1);
            }
            continue;
        }
        //Start of Steep Up Area?
        else if (idx < n_reachdists && is_steep_up_pt(idx)) {
            filter_sdas();
            std::size_t sua_end_idx = get_sua_end(idx);
            if (reach_i >
                get_reach_dist(sua_end_idx + 1) * (1.0 - steep_area_min_diff)) {
                continue;
            }
            for (auto &sda : SDAs) {
                if (valid_combination(sda, idx, sua_end_idx)) {
                    clusters.push_back(cluster_borders(sda, idx, sua_end_idx));
                }
            }
            idx = sua_end_idx;
            if (idx < n_reachdists - 1) {
                mib = get_reach_dist(idx + 1);
            }
        }
        else {
            if (reach_i > mib) {
                mib = reach_i;
            }
        }
    }
    return clusters;
}

// inline std::vector<cluster_tree> flat_clusters_to_tree(
//     const std::vector<chi_cluster_indices> &clusters_flat)
// {
//     //sort clusters_flat such that children are ordered before their parents in clusters_flat_sorted
//     std::vector<fplus::maybe<chi_cluster_indices>> clusters_flat_sorted_m(
//         clusters_flat.size(), fplus::nothing<chi_cluster_indices>());
//     std::size_t next_free_idx = 0;
//     for (std::size_t idx = 0; idx < clusters_flat.size(); idx++) {
//         while (next_free_idx < clusters_flat_sorted_m.size() &&
//                clusters_flat_sorted_m[next_free_idx].is_just()) {
//             next_free_idx++;
//         }
//         std::size_t idx_pos = next_free_idx;
//         std::size_t following_idx = idx + 1;
//         while (following_idx < clusters_flat.size() &&
//                clusters_flat[following_idx].second <=
//                    clusters_flat[idx].second) {
//             following_idx++;
//             idx_pos++;
//         }
//         clusters_flat_sorted_m[idx_pos] = fplus::just(clusters_flat[idx]);
//     }

//     auto clusters_flat_sorted = fplus::justs(clusters_flat_sorted_m);
//     assert(clusters_flat_sorted.size() == clusters_flat.size());

//     //compute tree from clusters_flat_sorted
//     std::vector<cluster_tree> result;
//     std::vector<cluster_tree> cluster_trees = fplus::transform(
//         [](const chi_cluster_indices &c) -> cluster_tree {
//             return cluster_tree(c);
//         },
//         clusters_flat_sorted);

//     auto get_first_parent_idx =
//         [&cluster_trees](std::size_t idx) -> std::size_t {
//         auto cluster = cluster_trees[idx].get_root().get_data();
//         for (std::size_t first_parent_idx = idx + 1;
//              first_parent_idx < cluster_trees.size(); first_parent_idx++) {
//             auto parent_cluster =
//                 cluster_trees[first_parent_idx].get_root().get_data();
//             if (cluster.first >= parent_cluster.first &&
//                 cluster.second <= parent_cluster.second) {
//                 return first_parent_idx;
//             }
//         }
//         return cluster_trees.size();
//     };
//     for (std::size_t idx = 0; idx < cluster_trees.size(); idx++) {
//         auto first_parent_idx = get_first_parent_idx(idx);
//         if (first_parent_idx >= cluster_trees.size()) {
//             result.push_back(cluster_trees[idx]);
//         }
//         else {
//             cluster_trees[first_parent_idx].get_root().add_child(
//                 cluster_trees[idx].get_root());
//         }
//     }

//     return result;
// }

// inline std::vector<cluster_tree> get_chi_clusters(
//     const std::vector<reachability_dist> &reach_dists, const double chi,
//     std::size_t min_pts, const double steep_area_min_diff = 0.0)
// {
//     auto clusters_flat =
//         get_chi_clusters_flat(reach_dists, chi, min_pts, steep_area_min_diff);
//     return flat_clusters_to_tree(clusters_flat);
// }

}  //namespace optics