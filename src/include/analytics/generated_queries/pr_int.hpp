#include "analytics/Turbograph.hpp"

class pr_int : public Turbograph {
  TG_NWSM<pr_int> engine;
  TG_DistributedVector<int32_t, PLUS> vec_pr_ov;
  TG_DistributedVector<int32_t, UNDEFINED> vec_pr_iv;
  int32_t scaled_epsilon;
  int32_t scaled_damping_factor;
  int32_t scale;

  int64_t num_skipped_edges;
  int64_t num_processed_edges1;
  int64_t num_processed_edges2;

public:
  pr_int() {
    UserArguments::MAX_LEVEL = 1;
    need_OutDegree[0] = true;
    num_skipped_edges = 0;
    num_processed_edges1 = 0;
    num_processed_edges2 = 0;
  }

  virtual void Superstep(int argc, char **argv) {
    Range<int> version_range = Range<int>(0, 0);
    engine.Start(version_range);
  }

  virtual void Initialize() {
    engine.InitUserProgram((pr_int*) this);
    engine.SetApplyFunction(&pr_int::Apply);
    engine.AddIncScatter(&pr_int::S_Lv1_SubQ1, 1, 1, true, false, false);
    engine.AddPruningScatter(&pr_int::Backward_BFS_Lv1_SubQ2_NWSM_Lv1, 1, 1, 2);
    engine.AddIncScatter(&pr_int::S_Lv1_SubQ2, 1, 2, false, false, true);
    engine.AddScatter(&pr_int::S_Lv1, 1);
    options.add_options()("scaled_epsilon", "Global Variable: scaled_epsilon",
                          cxxopts::value<int32_t>());
    options.add_options()("scaled_damping_factor", "Global Variable: scaled_damping_factor",
                          cxxopts::value<int32_t>());
    options.add_options()("scale", "Global Variable: scale",
                          cxxopts::value<int32_t>());
    auto parse_result = options.parse(argc_, argv_);
    scaled_epsilon = parse_result["scaled_epsilon"].as<int32_t>();
    scaled_damping_factor = parse_result["scaled_damping_factor"].as<int32_t>();
    scale = parse_result["scale"].as<int32_t>();
  } 

  virtual void OpenVectors() {
    vec_pr_ov.Open(NOUSE, RDWR, 1, "pr_ov");
    vec_pr_ov.OpenWindow(NOUSE, RDWR, 1);
    register_vector(vec_pr_ov);
    vec_pr_iv.Open(RDWR, NOUSE, 1, "pr_iv");
    vec_pr_iv.OpenWindow(RDWR, NOUSE, 1);
    register_vector(vec_pr_iv);
  }

  virtual void Initialize(node_t u) {

    vec_pr_iv.Initialize(u, (scale));
    MarkAtVOI(1, u, true);
  }
  virtual void InitializePrev(node_t u) {

    vec_pr_iv.InitializePrev(u, (scale));
    MarkAtVOIPrev(1, u, true);
  }
  virtual void DebugInitialize(node_t u) {
    int32_t out_degree = vec_out_degree.Read(u);
    fprintf(stdout, "OutDegree[%ld] = %d\n", u, out_degree);
  }
  virtual void DebugInitializePrev(node_t u) {
    int32_t out_degree = vec_out_degree.ReadPrev(u);
    fprintf(stdout, "OutDegreePrev[%ld] = %d\n", u, out_degree);
  }

  inline void S_Lv1(node_t u, node_t *u_nbrs[], node_t u_nbrs_sz[]) {
    bool u_d = true;
    int32_t pr_iv = vec_pr_iv.Read(u);
    int32_t out_degree = vec_out_degree.Read(u);

    bool u_dv_new = true;
    bool u_dv_old = false;
    // u --> v
    // Select (E) where 'src' == u and 'dst' == v
    ForNeighbor<OLD>(
        u_nbrs_sz, u_d, [&](node_t v_idx_from, node_t v_idx_end, bool v_d) {
          for (node_t v_idx = v_idx_from; v_idx < v_idx_end; v_idx++) {
            node_t v = GetVisibleDstVid(u_nbrs[0][v_idx]);
            if (v == -1)
              continue;
            if (!true)
              continue;
            vec_pr_ov.Update<1>(v, (pr_iv / out_degree));
          }
        });
  }

  virtual void Apply(node_t u) {
    int32_t out_degree = vec_out_degree.Read(u);
    int32_t pr_iv = vec_pr_iv.Read(u);
    int32_t pr_ov = vec_pr_ov.AccumulatedUpdate(u);

    int32_t new_val = (scale - scaled_damping_factor) / num_vertices + (scaled_damping_factor * pr_ov / scale);

    if (pr_ov == 0) return;
    vec_pr_iv.Write(u, new_val);

    if ((ABS(new_val - pr_iv) > scaled_epsilon) and (out_degree > 0)) {
      vec_pr_iv.Write(u, new_val);
      MarkAtVOIAtApply(1, u, true);

    } else {
      MarkAtVOIAtApply(1, u, false);
    }
  }

  /* Incremental Scatters */

  inline void S_Lv1_SubQ1(node_t u, node_t *u_nbrs[], node_t u_nbrs_sz[]) {
    bool u_d = true;

    bool u_dv_new = needGenerateNewUpdate(u);
    bool u_dv_old = needGenerateOldUpdate(u);
    if (!u_dv_new and !u_dv_old)
      return;

    int32_t pr_iv = vec_pr_iv.Read(u);
    int32_t pr_iv_prev_snapshot = vec_pr_iv.ReadPrev(u);
    int32_t out_degree = vec_out_degree.Read(u);
    int32_t out_degree_prev_snapshot = vec_out_degree.ReadPrev(u);

    // u --> v
    // Select (E) where 'src' == u and 'dst' == v
    ForNeighbor<OLD>(
        u_nbrs_sz, u_d, [&](node_t v_idx_from, node_t v_idx_end, bool v_d) {
          for (node_t v_idx = v_idx_from; v_idx < v_idx_end; v_idx++) {
            node_t v = GetVisibleDstVidAtOld(u_nbrs[0][v_idx]);
            if (v == -1)
              continue;
            if (!true)
              continue;

            vec_pr_ov.Update<1>(
                v,
                (u_dv_new && v_d) ? ((out_degree > 0) ? (pr_iv / out_degree) : vec_pr_ov.iden_elem) : vec_pr_ov.iden_elem,
                (u_dv_old || !v_d)
                    ? ((out_degree_prev_snapshot > 0) ? (pr_iv_prev_snapshot / out_degree_prev_snapshot) : vec_pr_ov.iden_elem)
                    : vec_pr_ov.iden_elem);
          }
        });
  }

  inline void S_Lv1_SubQ2(node_t u, node_t *u_nbrs[], node_t u_nbrs_sz[]) {
    bool u_d = true;

    int32_t pr_iv = vec_pr_iv.Read(u);
    int32_t pr_iv_prev_snapshot = vec_pr_iv.ReadPrev(u);
    int32_t out_degree = vec_out_degree.Read(u);
    int32_t out_degree_prev_snapshot = vec_out_degree.ReadPrev(u);

    bool u_dv_new = true;
    bool u_dv_old = false;
    // u --> v
    // Select (E) where 'src' == u and 'dst' == v
    ForNeighbor<DELTA>(
        u_nbrs_sz, u_d, [&](node_t v_idx_from, node_t v_idx_end, bool v_d) {
          for (node_t v_idx = v_idx_from; v_idx < v_idx_end; v_idx++) {
            node_t v = GetVisibleDstVid(u_nbrs[0][v_idx]);
            if (v == -1)
              continue;
            if (!true)
              continue;
            vec_pr_ov.Update<1>(v,
                                (u_dv_new && v_d) ? ((out_degree > 0) ? (pr_iv / out_degree) : vec_pr_ov.iden_elem)
                                                  : vec_pr_ov.iden_elem,
                                (u_dv_old || !v_d) ? ((out_degree > 0) ? (pr_iv / out_degree) : vec_pr_ov.iden_elem)
                                                   : vec_pr_ov.iden_elem);
          }
        });
  }

  /* Pruning Scatters */

  // Invoked if for (bfs_lv = 1 and window_lv = 1)
  // (1) dE AND (bfs_lv 1 == 1)
  // (2) (E or dE) AND (bfs_lv 1 >1) AND dq_nodes[2][window_lv 1].Get(v)
  inline void Backward_BFS_Lv1_SubQ2_NWSM_Lv1(node_t v, node_t *v_nbrs[],
                                              node_t v_nbrs_sz[]) {
    for (node_t u_idx = 0; u_idx < v_nbrs_sz[0]; u_idx++) {
      node_t u = GetVisibleDstVid(v_nbrs[0][u_idx]);
      if (u == -1)
        continue;
      if (!true)
        continue;
      if (1 == 1 && 1 < UserArguments::MAX_LEVEL) { // mark dst
        set_dq_nodes(2, 1, v);                      // for nwsm at lv (1 + 1)
      }
      StartingVertices.Update<1>(u, true);
      set_dq_nodes(2, 1 - 1, u); // for nwsm at lv 1
    }
  }

  /* Pull Sactters */

  /* Post Processing */
  virtual void Postprocessing() {
    fprintf(stdout, "num_skipped_edges = %ld\n", num_skipped_edges);
    fprintf(stdout, "num_processed_edges1 = %ld, num_processed_edges2 = %ld\n", num_processed_edges1, num_processed_edges2);
    return;
    ProcessVertices([&](node_t vid) {
        if (vid % 777777 == 0) {
          fprintf(stdout, "(%ld, %ld) PageRankScore %ld %ld\n", (int32_t) UserArguments::UPDATE_VERSION, (int32_t) UserArguments::SUPERSTEP, (int32_t) vid, (int32_t) vec_pr_iv.Read(vid));
        }
        }, false);
  }

  void synchronize_vectors() {}

};