#ifndef HOMOGENEOUSPAGEWRITER_H
#define HOMOGENEOUSPAGEWRITER_H

/*
 * Design of the HomogeneousPageWriter
 *
 * This class provides a simple API to append a fixed size edge page to a file
 * and a function to check if the correct edge data is written to the edge pages 
 * of the file. The method to check if the data is correct is as follows:
 *     1) Check whether the source vertex id and destination vertex id of the 
 *     edge existing in the file are included in the vertex id range allocated 
 *     according to partitioning, and 
 *     2) the size of each adj list is greater than 0.
 */

#include "analytics/io/GraphDataReaderWriterInterface.hpp"
#include "analytics/io/Turbo_bin_io_handler.hpp"
#include "analytics/util/TG_NWSM_Utility.hpp"

class HomogeneousPageWriter : public WriterInterface {
  public:

	/* 
	* Construct the writer for the given file 
	*/
	HomogeneousPageWriter(const char* file_name) : WriterInterface() {
		bin_writer_.OpenFile(file_name, true, true);
	}

	/* 
	* Close the file 
	*/
	void Close(bool rm = false) {
		bin_writer_.Close(rm);
	}

	/* 
	* Append a fixed size page to the file 
	*/
	virtual void AppendPage(Page& page) {
		ReturnStatus st;
		st = bin_writer_.Append(sizeof(Page), (char*) page.GetData());
		ALWAYS_ASSERT(SanityCheck(&page));
		if (st != OK) {
			abort();
		}
		ALWAYS_ASSERT(st == OK);
	}

	/* 
	* Check whether the adjacency lists in the page is correct 
	*/
	bool SanityCheck(Page* page) {
		typedef TG_AdjacencyMatrixPageIterator AdjList_Iterator_t;
		typedef TG_AdjacencyListIterator  NbrList_Iterator_t;

		ReturnStatus st;
		AdjList_Iterator_t adjlist_iter;
		NbrList_Iterator_t nbrlist_iter;
		adjlist_iter.SetCurrentPage(page);

		while (adjlist_iter.GetNextAdjList(nbrlist_iter) == OK) {
			ALWAYS_ASSERT (nbrlist_iter.GetSrcVid() >= PartitionStatistics::my_first_node_id()
			               && nbrlist_iter.GetSrcVid() <= PartitionStatistics::my_last_node_id());
			ALWAYS_ASSERT(nbrlist_iter.GetNumEntries() > 0);
		}
		return true;
	}

  private:
	Turbo_bin_io_handler bin_writer_; /* Binary file writer */
};
#endif
