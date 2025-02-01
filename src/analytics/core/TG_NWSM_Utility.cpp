#include "analytics/util/TG_NWSM_Utility.hpp"



template <>
void TriangleIntersectionTemp<NONE, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  int64_t merge_cost = v1_sz + v2_sz;

  int64_t tc_cnt = 0;

  // if a > b, alogb > bloga
  if(v1_sz+v2_sz > nlogm(v1_sz, v2_sz)) {
    if(nlogm(v1_sz, v2_sz) > nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
    else if(v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=1;
  } else if(v1_sz+v2_sz > 10*nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;

  //strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        common_nbrs[common_nbrs_idx++] = v1[v1_idx];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        v2_idx+=2;
        continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        v1_idx+=2;
        continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        v1_idx+=2;
        v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        v2_idx+=2;
      } else {
        v1_idx+=2;
      }
    }

    while( v1_idx < v1_sz && v2_idx < v2_sz ) {
      if(v1[v1_idx] == v2[v2_idx] ) {
        common_nbrs[common_nbrs_idx++] = v1[v1_idx];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        v1_idx++;
        v2_idx++;
      } else if(v2[v2_idx] < v1[v1_idx] ) {
        v2_idx++;
      } else { // v2[v2_idx] > v1[v1_idx]
        v1_idx++;
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
    if(binary_splitting_idx != NULL && v2_sz > BINARY_SPLITTING_THRESHOLD && v2_sz > v1_sz * BINARY_SPLITTING_MAGNITUDE) {
      //TriangleIntersection_HybridTemp<NONE, NONE>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
      v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
      while( v2_idx < v2_sz ) {
        if( v2[v2_idx] == v1[v1_idx] ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        }

        v1_idx++;
        if(v1_idx == v1_sz) break;
        v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
      }
    }
  } else { //v2*log(v1)
    if(binary_splitting_idx != NULL && v1_sz > BINARY_SPLITTING_THRESHOLD && v1_sz > v2_sz * BINARY_SPLITTING_MAGNITUDE) {
      //TriangleIntersection_HybridTemp<NONE, NONE>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
      //binary search
      v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
      while( v1_idx < v1_sz) {
        if( v1[v1_idx] == v2[v2_idx] ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
          //AggregateFunc(v1[v1_idx], u3_d);
        }

        v2_idx++;
        if(v2_idx == v2_sz) break;

        //binary search
        v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
      }
    }
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}

template <>
void TriangleIntersectionTemp<LT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  /*if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
    } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
    strategy = 1;
    else
    strategy = 2;
    }*/
  // if a > b, alogb > bloga
  if(v1_sz+v2_sz > nlogm(v1_sz, v2_sz)) {
    if(nlogm(v1_sz, v2_sz) > nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
    else if(v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=1;
  } else if(v1_sz+v2_sz > 10*nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;

  node_t lower_key = (v1_key > v2_key) ? v2_key : v1_key;

  //strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v2[v2_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }

        v2_idx+=2;
        continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }

        v1_idx+=2;
        continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }

        v1_idx+=2;
        v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        if (v2[v2_idx+1] >= lower_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx+=2;
      } else {
        if (v1[v1_idx+1] >= lower_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx < v1_sz && v2_idx < v2_sz ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          } else break;

          v1_idx++;
          v2_idx++;
        } else if(v2[v2_idx] < v1[v1_idx] ) {
          if (v1[v1_idx] >= lower_key) {
            break;
          }
          v2_idx++;
        } else { // v2[v2_idx] > v1[v1_idx]
          if (v2[v2_idx] >= lower_key) {
            break;
          }
          v1_idx++;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
    if(binary_splitting_idx != NULL && v2_sz > BINARY_SPLITTING_THRESHOLD && v2_sz > v1_sz * BINARY_SPLITTING_MAGNITUDE) {
      TriangleIntersection_HybridTemp<LT, LT>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
      v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
      while( v2_idx < v2_sz ) {

        if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
          if( v2[v2_idx] == v1[v1_idx] ) {
            common_nbrs[common_nbrs_idx++] = v2[v2_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          }
        } else break;

        v1_idx++;
        if(v1_idx == v1_sz) break;
        v2_idx=std::lower_bound(v2+v2_idx, v2+v2_sz, v1[v1_idx])-v2;
      }
    }
  } else { //v2*log(v1)
    if(binary_splitting_idx != NULL && v1_sz > BINARY_SPLITTING_THRESHOLD && v1_sz > v2_sz * BINARY_SPLITTING_MAGNITUDE) {
      TriangleIntersection_HybridTemp<LT, LT>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
      //binary search
      v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
      while( v1_idx < v1_sz) {
        if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
          if( v1[v1_idx] == v2[v2_idx] ) {
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
            //AggregateFunc(v1[v1_idx], u3_d);
          }
        } else break;

        v2_idx++;
        if(v2_idx == v2_sz) break;

        //binary search
        v1_idx=std::lower_bound(v1+v1_idx, v1+v1_sz, v2[v2_idx])-v1;
      }
    }
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}


template <>
void TriangleIntersectionTemp<NONE, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  node_t lower_key = v2_key;

  strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v2[v2_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v2[v2_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }
        v2_idx+=2;
        continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
        continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
        v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        if (v2[v2_idx+1] >= lower_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx+=2;
      } else {
        if (v1[v1_idx+1] >= lower_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx < v1_sz && v2_idx < v2_sz ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v2[v2_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v2[v2_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          }
          v1_idx++;
          v2_idx++;
        } else if(v2[v2_idx] < v1[v1_idx] ) {
          if (v1[v1_idx] >= lower_key) {
            break;
          }
          v2_idx++;
        } else { // v2[v2_idx] > v1[v1_idx]
          if (v2[v2_idx] >= lower_key) {
            break;
          }
          v1_idx++;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}


template <>
void TriangleIntersectionTemp<LT, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  node_t lower_key = v1_key;

  strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
          //} else if (v1[v1_idx] >= lower_key) {
      } else {
        is_satisfy_po = false;
        break;
      }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v2[v2_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
          //} else if (v1[v1_idx] >= lower_key) {
      } else {
        is_satisfy_po = false;
        break;
      }
      v2_idx+=2;
      continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
          //} else if (v1[v1_idx+1] >= lower_key) {
      } else {
        is_satisfy_po = false;
        break;
      }
      v1_idx+=2;
      continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
          //} else if (v1[v1_idx+1] >= lower_key) {
      } else {
        is_satisfy_po = false;
        break;
      }
      v1_idx+=2;
      v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        v2_idx+=2;
      } else {
        if (v1[v1_idx+1] >= lower_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx < v1_sz && v2_idx < v2_sz ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          } else break;
          v1_idx++;
          v2_idx++;
        } else if(v2[v2_idx] < v1[v1_idx] ) {
          if (v1[v1_idx] >= lower_key) {
            break;
          }
          v2_idx++;
        } else { // v2[v2_idx] > v1[v1_idx]
          if (v1[v1_idx] >= lower_key) {
            break;
          }
          v1_idx++;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}

template<>
void TriangleIntersectionTemp<GT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }
  /*if(v1_sz+v2_sz > nlogm(v1_sz, v2_sz)) {
    if(nlogm(v1_sz, v2_sz) > nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;
    else if(v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=1;
  } else if(v1_sz+v2_sz > 10*nlogm(v2_sz, v1_sz) && v1_sz > BINARY_SPLITTING_THRESHOLD) strategy=2;*/

  int32_t v2_idx=v2_sz-1, v1_idx=v1_sz-1;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=0, v2_end=0;
    while(v1_idx > v1_end && v2_idx > v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] <= v1_key) {
          is_satisfy_po = false;
          break;
        } else if (v2[v2_idx] <= v2_key) {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v2[v2_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] <= v1_key) {
          is_satisfy_po = false;
          break;
        } else if (v2[v2_idx-1] <= v2_key) {
          is_satisfy_po = false;
          break;
        }

        v2_idx-=2;
        continue;
      } else if( v1[v1_idx-1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx-1] <= v1_key) {
          is_satisfy_po = false;
          break;
        } else if (v2[v2_idx] <= v2_key) {
          is_satisfy_po = false;
          break;
        }

        v1_idx-=2;
        continue;
      }

      if( v1[v1_idx-1] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx-1] <= v1_key) {
          is_satisfy_po = false;
          break;
        } else if (v2[v2_idx-1] <= v2_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
        v2_idx-=2;
      } else if( v1[v1_idx-1] > v2[v2_idx-1] ) {
        if (v1[v1_idx-1] <= v1_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
      } else {
        if (v2[v2_idx-1] <= v2_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx-=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx >= 0 && v2_idx >= 0 ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
            //AggregateFunc(v1[v1_idx], u3_d);
          } //else break;
          //if(CheckPartialOrder(v1[v1_idx])) // nbr_vid > 3rd node vid
          //    AggregateFunc(v1[v1_idx], u3_d);

          v1_idx--;
          v2_idx--;
        } else if(v2[v2_idx] > v1[v1_idx] ) {
          //if(!CheckPartialOrder(v1[v1_idx]))
          //    break;
          v2_idx--;
        } else {
          //if(!CheckPartialOrder(v2[v2_idx]))
          //    break;
          v1_idx--;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
    //if(binary_splitting_idx != NULL && v2_sz > BINARY_SPLITTING_THRESHOLD && v2_sz > v1_sz * BINARY_SPLITTING_MAGNITUDE) {
    if(false) {
      TriangleIntersection_HybridTemp<GT, GT>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
			while (v1_idx >= 0 and CheckPartialOrder(v1[v1_idx])) {
        int last = v2_idx+1;
        v2_idx = std::lower_bound(v2, v2+last, v1[v1_idx]) - v2;
        if (v2_idx != last and v2[v2_idx] == v1[v1_idx]) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        }
        if (v2_idx == 0) break;
        v1_idx--;
      }
    }
  } else { //v2*log(v1)
    //if(binary_splitting_idx != NULL && v1_sz > BINARY_SPLITTING_THRESHOLD && v1_sz > v2_sz * BINARY_SPLITTING_MAGNITUDE) {
    if(false) {
      TriangleIntersection_HybridTemp<GT, GT>(v1, v2, v1_sz, v2_sz, v1_vid, v2_vid, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
    } else {
      while (v2_idx >= 0 and CheckPartialOrder(v2[v2_idx])) {
        int last = v1_idx+1;
        v1_idx = std::lower_bound(v1, v1+last, v2[v2_idx]) - v1;
        if (v1_idx != last and v1[v1_idx] == v2[v2_idx]) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        }
        if (v1_idx == 0) break;
        v2_idx--;
      }
    }
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}


template<>
void TriangleIntersectionTemp<NONE, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  node_t greater_key = v2_key;

  strategy = 0;
  int32_t v2_idx=v2_sz-1, v1_idx=v1_sz-1;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=0, v2_end=0;
    while(v1_idx > v1_end && v2_idx > v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v2[v2_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v2[v2_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx-=2;
        continue;
      } else if( v1[v1_idx-1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx] <= greater_key) {
          is_satisfy_po = false;
          break;
        }

        v1_idx-=2;
        continue;
      }

      if( v1[v1_idx-1] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
        v2_idx-=2;
      } else if( v1[v1_idx-1] > v2[v2_idx-1] ) {
        v1_idx-=2;
      } else {
        if (v2[v2_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx-=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx >= 0 && v2_idx >= 0 ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v2[v2_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v2[v2_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          }
          v1_idx--;
          v2_idx--;
        } else if(v2[v2_idx] > v1[v1_idx] ) {
          v2_idx--;
        } else {
          if (v2[v2_idx] <= greater_key) {
            break;
          }
          v1_idx--;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}


template<>
void TriangleIntersectionTemp<GT, NONE>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  node_t greater_key = v1_key;

  strategy = 0;
  int32_t v2_idx=v2_sz-1, v1_idx=v1_sz-1;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=0, v2_end=0;
    while(v1_idx > v1_end && v2_idx > v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v2[v2_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx-=2;
        continue;
      } else if( v1[v1_idx-1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
        continue;
      }

      if( v1[v1_idx-1] == v2[v2_idx-1] ) {
        if( CheckPartialOrder(v1[v1_idx-1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx-1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
        v2_idx-=2;
      } else if( v1[v1_idx-1] > v2[v2_idx-1] ) {
        if (v1[v1_idx-1] <= greater_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx-=2;
      } else {
        v2_idx-=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx >= 0 && v2_idx >= 0 ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
          }
          v1_idx--;
          v2_idx--;
        } else if(v2[v2_idx] > v1[v1_idx] ) {
          v2_idx--;
        } else {
          if (v1[v1_idx] <= greater_key) {
            break;
          }
          v1_idx--;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}

template <>
void TriangleIntersectionTemp<GT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  //int64_t merge_cost = v1_sz + v1_sz;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx] >= v2_key) {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v2[v2_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx+1] >= v2_key) {
          is_satisfy_po = false;
          break;
        }

        v2_idx+=2;
        continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx] >= v2_key) {
          is_satisfy_po = false;
          break;
        }

        v1_idx+=2;
        continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v2[v2_idx+1] >= v2_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
        v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        if (v2[v2_idx+1] >= v2_key) {
          is_satisfy_po = false;
          break;
        }
        v2_idx+=2;
      } else {
        v1_idx+=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx < v1_sz && v2_idx < v2_sz ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
            //AggregateFunc(v1[v1_idx], u3_d);
          } //else break;
          //if(CheckPartialOrder(v1[v1_idx])) // nbr_vid > 3rd node vid
          //    AggregateFunc(v1[v1_idx], u3_d);

          v1_idx++;
          v2_idx++;
        } else if(v2[v2_idx] < v1[v1_idx] ) {
          //if(!CheckPartialOrder(v1[v1_idx]))
          //    break;
          v2_idx++;
        } else {
          //if(!CheckPartialOrder(v2[v2_idx]))
          //    break;
          v1_idx++;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}

template <>
void TriangleIntersectionTemp<LT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key,
    Range<int32_t>* binary_splitting_idx, bool u3_d,
    std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int


  int strategy=0;
  int64_t merge_cost = v1_sz + v2_sz;
  int64_t binary_search_cost1 = nlogm(v1_sz, v2_sz) * 4;
  int64_t binary_search_cost2 = nlogm(v2_sz, v1_sz) * 4;

  int64_t tc_cnt = 0;

  if (merge_cost < binary_search_cost1 and merge_cost < binary_search_cost2) {
    strategy = 0;
  } else {
    if (nlogm(v1_sz, v2_sz) < nlogm(v2_sz, v1_sz))
      strategy = 1;
    else
      strategy = 2;
  }

  strategy = 0;
  int32_t v2_idx=0, v1_idx=0;
  if(strategy == 0) { //v1+v2
    bool is_satisfy_po = true;
    int32_t v1_end=v1_sz-1, v2_end=v2_sz-1;
    while(v1_idx < v1_end && v2_idx < v2_end) {
      if( v1[v1_idx] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] >= v1_key) {
          is_satisfy_po = false;
          break;
        }
      } else if( v1[v1_idx] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v2[v2_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v2[v2_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx] >= v1_key) {
          is_satisfy_po = false;
          break;
        }

        v2_idx+=2;
        continue;
      } else if( v1[v1_idx+1] == v2[v2_idx] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx+1] >= v1_key) {
          is_satisfy_po = false;
          break;
        }

        v1_idx+=2;
        continue;
      }

      if( v1[v1_idx+1] == v2[v2_idx+1] ) {
        if( CheckPartialOrder(v1[v1_idx+1]) ) {
          common_nbrs[common_nbrs_idx++] = v1[v1_idx+1];
          if (common_nbrs_idx == 128) {
            AggregateFunc(0, 128, common_nbrs, u3_d);
            common_nbrs_idx = 0;
          }
        } else if (v1[v1_idx+1] >= v1_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
        v2_idx+=2;
      } else if( v1[v1_idx+1] > v2[v2_idx+1] ) {
        v2_idx+=2;
      } else {
        if (v1[v1_idx+1] >= v1_key) {
          is_satisfy_po = false;
          break;
        }
        v1_idx+=2;
      }
    }

    if (is_satisfy_po) {
      while( v1_idx < v1_sz && v2_idx < v2_sz ) {
        if(v1[v1_idx] == v2[v2_idx] ) {
          if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
            common_nbrs[common_nbrs_idx++] = v1[v1_idx];
            if (common_nbrs_idx == 128) {
              AggregateFunc(0, 128, common_nbrs, u3_d);
              common_nbrs_idx = 0;
            }
            //AggregateFunc(v1[v1_idx], u3_d);
          } //else break;
          //if(CheckPartialOrder(v1[v1_idx])) // nbr_vid > 3rd node vid
          //    AggregateFunc(v1[v1_idx], u3_d);

          v1_idx++;
          v2_idx++;
        } else if(v2[v2_idx] < v1[v1_idx] ) {
          //if(!CheckPartialOrder(v1[v1_idx]))
          //    break;
          v2_idx++;
        } else {
          //if(!CheckPartialOrder(v2[v2_idx]))
          //    break;
          v1_idx++;
        }
      }
    }
  } else if(strategy == 1) { //v1*log(v2)
  } else { //v2*log(v1)
  }
  if (common_nbrs_idx != 0) {
    AggregateFunc(0, common_nbrs_idx, common_nbrs, u3_d);
    common_nbrs_idx = 0;
  }
}

template <>
void FindCommonNeighbors<NONE, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<NONE, NONE>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template <>
void FindCommonNeighbors<GT, GT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<GT, GT>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}


template <>
void FindCommonNeighbors<LT, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<LT, LT>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}


template <>
void FindCommonNeighbors<GT, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<GT, LT>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template <>
void FindCommonNeighbors<LT, GT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<LT, GT>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template <>
void FindCommonNeighbors<NONE, LT>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<NONE, LT>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template <>
void FindCommonNeighbors<LT, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<LT, NONE>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template <>
void FindCommonNeighbors<GT, NONE>(node_t v1, node_t* v1_data, int64_t v1_sz, node_t v1_key, node_t v2, node_t* v2_data, int64_t v2_sz, node_t v2_key, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc) {
  if (v1_sz == 0 || v2_sz == 0) return;

  DummyUDFDataStructure ds;
  Range<int32_t>* binary_splitting_idx=(Range<int32_t>*)(ds.data);

  node_t v1_degreeorder;
  node_t v2_degreeorder;
  if (UserArguments::USE_DEGREE_ORDER_REPRESENTATION) {
    v1_degreeorder = v1;
    v2_degreeorder = v2;
  } else {
    v1_degreeorder = PartitionStatistics::VidToDegreeOrder(v1);
    v2_degreeorder = PartitionStatistics::VidToDegreeOrder(v2);
  }
  node_t common_nbrs[128];
  int64_t common_nbrs_idx = 0;
  TriangleIntersectionTemp<GT, NONE>(v1_data, v2_data, v1_sz, v2_sz, v1_degreeorder, v2_degreeorder, v1_key, v2_key, binary_splitting_idx, u3_d, CheckPartialOrder, AggregateFunc, &common_nbrs[0], common_nbrs_idx);
}

template<>
void TriangleIntersection_HybridTemp<LT, LT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int
  int32_t v1_idx=0, v2_idx=0, v1_cur_sz, v2_cur_sz;;
  int64_t triangle_cnt_delta=0;
  int binary_splitting_idx_size=0;
  Range<int32_t> v1_range (0, v1_sz), v2_range (0, v2_sz); //start - inclusive, end - exclusive

  while(1) {
    v1_cur_sz=v1_range.end - v1_range.begin;
    v2_cur_sz=v2_range.end - v2_range.begin;

    if(v1_cur_sz == 0 || v2_cur_sz == 0) {
      if(binary_splitting_idx_size == 0) break;

      binary_splitting_idx_size-=2;
      v1_range = binary_splitting_idx[binary_splitting_idx_size];
      v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
      continue;
    }

    if(v1_cur_sz < v2_cur_sz) {
      if(v2_cur_sz < BINARY_SPLITTING_THRESHOLD || v2_cur_sz < BINARY_SPLITTING_MAGNITUDE * v1_cur_sz) {
        TriangleIntersectionTemp<LT, LT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
        if(binary_splitting_idx_size == 0) break;

        binary_splitting_idx_size-=2;
        v1_range = binary_splitting_idx[binary_splitting_idx_size];
        v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
        continue;
      }

      v1_idx=(v1_range.begin+v1_range.end)/2;
      v2_idx=std::lower_bound(v2+v2_range.begin, v2+v2_range.end, v1[v1_idx])-v2;
      if(v2_idx == v2_range.end) {
        v1_range.end=v1_idx;
        v2_range.end=v2_idx;
        continue;
      }

    } else {
      if(v1_cur_sz < BINARY_SPLITTING_THRESHOLD || v1_cur_sz < BINARY_SPLITTING_MAGNITUDE * v2_cur_sz) {
        TriangleIntersectionTemp<LT, LT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
        if(binary_splitting_idx_size == 0) break;

        binary_splitting_idx_size-=2;
        v1_range = binary_splitting_idx[binary_splitting_idx_size];
        v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
        continue;
      }

      v2_idx=(v2_range.begin+v2_range.end)/2;
      v1_idx=std::lower_bound(v1+v1_range.begin, v1+v1_range.end, v2[v2_idx])-v1;
      if(v1_idx == v1_range.end) {
        v1_range.end=v1_idx;
        v2_range.end=v2_idx;
        continue;
      }
    }

    if(binary_splitting_idx_size == MAXIMUM_BINARY_SPLITTING_IDX_SIZE) {
      TriangleIntersectionTemp<LT, LT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);

      if(binary_splitting_idx_size == 0) break;

      binary_splitting_idx_size-=2;
      v1_range = binary_splitting_idx[binary_splitting_idx_size];
      v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
      continue;
    }

    if(v1[v1_idx] == v2[v2_idx]) {
      if(CheckPartialOrder(v2[v2_idx])) { // nbr_vid > 3rd node vid
        //AggregateFunc(v2[v2_idx], u3_d); // XXX
        common_nbrs[common_nbrs_idx++] = v2[v2_idx];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
        binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
      }
      v1_range.end=v1_idx;
      v2_range.end=v2_idx;
    } else {
      if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
        // v1_idx is lower_bound so v1[v1_idx] > v2[v2_idx]

        if(v1_cur_sz < v2_cur_sz) {
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx, v2_range.end);
        } else {
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx, v1_range.end);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
        }
      }
      v1_range.end=v1_idx;
      v2_range.end=v2_idx;
    }
  }
}

template<>
void TriangleIntersection_HybridTemp<GT, GT>(node_t* v1, node_t* v2, int64_t v1_sz, int64_t v2_sz, node_t v1_vid, node_t v2_vid, node_t v1_key, node_t v2_key, Range<int32_t>* binary_splitting_idx, bool u3_d, std::function<bool(node_t)> CheckPartialOrder, std::function<void(node_t, node_t, node_t*, bool)> AggregateFunc, node_t* common_nbrs, int64_t& common_nbrs_idx) { //v1 - ext, v2 - int
  int32_t v1_idx=0, v2_idx=0, v1_cur_sz, v2_cur_sz;;
  int64_t triangle_cnt_delta=0;
  int binary_splitting_idx_size=0;
  Range<int32_t> v1_range (0, v1_sz), v2_range (0, v2_sz); //start - inclusive, end - exclusive

  while(1) {
    v1_cur_sz=v1_range.end - v1_range.begin;
    v2_cur_sz=v2_range.end - v2_range.begin;

    if(v1_cur_sz == 0 || v2_cur_sz == 0) {
      if(binary_splitting_idx_size == 0) break;

      binary_splitting_idx_size-=2;
      v1_range = binary_splitting_idx[binary_splitting_idx_size];
      v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
      continue;
    }

    if(v1_cur_sz < v2_cur_sz) {
      if(v2_cur_sz < BINARY_SPLITTING_THRESHOLD || v2_cur_sz < BINARY_SPLITTING_MAGNITUDE * v1_cur_sz) {
        TriangleIntersectionTemp<GT, GT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
        if(binary_splitting_idx_size == 0) break;

        binary_splitting_idx_size-=2;
        v1_range = binary_splitting_idx[binary_splitting_idx_size];
        v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
        continue;
      }

      v1_idx=(v1_range.begin+v1_range.end)/2;
      v2_idx=std::lower_bound(v2+v2_range.begin, v2+v2_range.end, v1[v1_idx])-v2;
      if(v2_idx == v2_range.end) {
        v1_range.end=v1_idx;
        v2_range.end=v2_idx;
        continue;
      }

    } else {
      if(v1_cur_sz < BINARY_SPLITTING_THRESHOLD || v1_cur_sz < BINARY_SPLITTING_MAGNITUDE * v2_cur_sz) {
        TriangleIntersectionTemp<GT, GT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);
        if(binary_splitting_idx_size == 0) break;

        binary_splitting_idx_size-=2;
        v1_range = binary_splitting_idx[binary_splitting_idx_size];
        v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
        continue;
      }

      v2_idx=(v2_range.begin+v2_range.end)/2;
      v1_idx=std::lower_bound(v1+v1_range.begin, v1+v1_range.end, v2[v2_idx])-v1;
      if(v1_idx == v1_range.end) {
        v1_range.end=v1_idx;
        v2_range.end=v2_idx;
        continue;
      }
    }

    if(binary_splitting_idx_size == MAXIMUM_BINARY_SPLITTING_IDX_SIZE) {
      TriangleIntersectionTemp<GT, GT>(v1+v1_range.begin, v2+v2_range.begin, v1_cur_sz, v2_cur_sz, v1_vid, v2_vid, v1_key, v2_key, NULL, u3_d, CheckPartialOrder, AggregateFunc, common_nbrs, common_nbrs_idx);

      if(binary_splitting_idx_size == 0) break;

      binary_splitting_idx_size-=2;
      v1_range = binary_splitting_idx[binary_splitting_idx_size];
      v2_range = binary_splitting_idx[binary_splitting_idx_size+1];
      continue;
    }

    if(v1[v1_idx] == v2[v2_idx]) {
      if(CheckPartialOrder(v2[v2_idx])) { // nbr_vid > 3rd node vid
        //AggregateFunc(v2[v2_idx], u3_d); // XXX
        common_nbrs[common_nbrs_idx++] = v2[v2_idx];
        if (common_nbrs_idx == 128) {
          AggregateFunc(0, 128, common_nbrs, u3_d);
          common_nbrs_idx = 0;
        }

        //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
        //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
        binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_range.begin, v1_idx);
        binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_range.begin, v2_idx);
      }
      v1_range.begin=v1_idx;
      v2_range.begin=v2_idx;
    } else {
      if(CheckPartialOrder(v1[v1_idx])) { // nbr_vid > 3rd node vid
        // v1_idx is lower_bound so v1[v1_idx] > v2[v2_idx]

        if(v1_cur_sz < v2_cur_sz) {
          //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx+1, v1_range.end);
          //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx, v2_range.end);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_range.begin, v1_idx);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_range.begin, v2_idx+1);
        } else {
          //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_idx, v1_range.end);
          //binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_idx+1, v2_range.end);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v1_range.begin, v1_idx+1);
          binary_splitting_idx[binary_splitting_idx_size++] = Range<int32_t>(v2_range.begin, v2_idx);
        }
      }
      v1_range.end=v1_idx;
      v2_range.end=v2_idx;
    }
  }
}





