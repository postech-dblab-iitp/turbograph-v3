#ifndef GraphDataReaderWriterInterface_H
#define GraphDataReaderWriterInterface_H

/*
 * Design of the GraphDataReaderWriterInterface
 *
 * These classes are the parent classes of various data readers/writers depending 
 * on the format of the input data. It has the following classes as child classes:
 *     ReaderInterface:
 *         - BinaryFormatEdgePairListReader, BinaryAdjListReader
 *         - TextEdgePairListReader, TextFormatAdjListReader, TextEdgePairListFastReader
 *         - EdgePairListFilesReader, EdgePairListSortedFilesReader
 *         - ParallelEdgePairListSortedFilesReader, ParallelTextEdgePairListFastReader,
 *           ParallelBinaryFormatEdgePairListReader
 *     WrterInterface:
 *         - HomogeneousPageWriter
 */

#include "page.hpp"

template <typename T>
class ReaderInterface {
  public:

	ReaderInterface() {}
	virtual ~ReaderInterface() {}

	/* Rewind this reader */
	virtual ReturnStatus rewind() = 0;
	/* Get the next item */
	virtual ReturnStatus getNext(T& item) = 0;
	/* Get the next item for the given thread id */
	virtual ReturnStatus getNext(T& item, int64_t thread_num) {}
	/* Get the next item */
	virtual ReturnStatus getNext(T* item) {}

  private:

};

class WriterInterface {
  public:

	WriterInterface() {}

	/* Append page to the file */
	virtual void AppendPage(Page& page) = 0;

  private:
};

#endif
