#pragma once

/*
 * Design of the MetaDataReader
 *
 * This class is used for parsing the vertex id range allocated to each machine 
 * written in the metadata by taking the path of the metadata file as input. The 
 * parsed result is used to initialize system internal parameters.
 */

#include <algorithm>
#include <limits>

#include "analytics/util/util.hpp"
#include "analytics/io/GraphDataReaderWriterInterface.hpp"
#include "analytics/io/Turbo_bin_mmapper.hpp"
//#include "turbo_helper_functions.h"
#include "analytics/io/TextFormatSequentialReaderWriter.hpp"

class MetaDataReader {
  public:

	/* 
	* Construct this meta data reader
	* It reads the vertex id ranges that allocated to machines in the files
	*/
	MetaDataReader(std::vector<std::string>& meta_lists) {
		ReturnStatus st;
		reader1_.resize(meta_lists.size());
		for (std::size_t idx = 0; idx < reader1_.size(); idx++) {
			st = reader1_[idx].Open(meta_lists[idx].c_str());
			ALWAYS_ASSERT(st == OK);
		}
		std::string buffer;
		std::string token_buffer;
		first_node_id_ = -1;
		last_node_id_ = 0;
		for (std::size_t idx = 0; idx < reader1_.size(); idx++) {
			st = reader1_[idx].getNext(buffer);
			ALWAYS_ASSERT(st == OK);
			std::istringstream iss(buffer);
			node_t tempfirst, templast;
			iss >> tempfirst >> templast;
			if(first_node_id_ == -1)
				first_node_id_ = tempfirst;
			else
				first_node_id_ = std::min(first_node_id_, tempfirst);
			last_node_id_ = std::max(last_node_id_, templast);
		}
		my_num_nodes_ = (last_node_id_ - first_node_id_ + 1);
	}

	/*
	* Get the first node id that allocated to this machine
	*/
	node_t FirstNodeId() {
		return first_node_id_;
	}

	/*
	* Get the last node id that allocated to this machine
	*/
	node_t LastNodeId() {
		return last_node_id_;
	}

	/*
	* Get the number of nodes that allocated to this machine
	*/
	node_t MyNumNodes() {
		return (last_node_id_ - first_node_id_ + 1);
	}

	/*
	* Get the number of edges that allocated to this machine
	*/
	node_t MyNumEdges() {
		return -1;
	}

  private:
	std::vector<TextFormatSequentialReader> reader1_; /* reader */
	node_t first_node_id_; /* the first node id that allocated to this machine */
	node_t last_node_id_; /* the last node id that allocated to this machine */
	node_t my_num_nodes_; /* the number of nodes that allocated to this machine */
};
