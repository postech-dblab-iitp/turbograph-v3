#pragma once
#ifndef TURBOGRAPH_H
#define TURBOGRAPH_H

/*
 * Design of the Turbograph
 *
 * This class is base class of graph analytics query programs. When a user want 
 * to write a graph analytics query program directly in C++, the APIs that user
 * needs to implement are listed as virtual functions in the class.
 */

#include "turbograph_implementation.hpp"

class Turbograph : public TurbographImplementation {
  public:
	/* System-provided APIs */
	// void MarkAtVOI(int lv, node_t vid)
	// bool HasVertexInWindow(int lv, node_t vid)
	// void GetAdjList(int lv, node_t vid, node_t*& data_ptr, int64_t& data_sz)
	// int64_t CountNumberOfCommonNeighbors(v_vid, v_nbr_vids, v_nbr_sz, u_vid, u_nbr_vids, u_nbr_sz, PartialOrrderFunction);
	// void ProcessVertices([&](node_t vid){ /* code */ });

	/* Users implement */
	virtual void OpenVectors() {}
	virtual void Initialize() {}

	virtual void Superstep(int argc, char** argv) = 0;
	virtual void Inc_Superstep(int argc, char** argv, Range<int> version_range) {};
	virtual void Postprocessing() {}

  private:

};

#endif
