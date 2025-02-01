#ifndef _TURBO_BIN_LISTS_LOADER_H
#define _TURBO_BIN_LSITS_LOADER_H
#include "analytics/io/Turbo_bin_mmapper.hpp"
class Turbo_bin_lists_reader {
  public:

	Turbo_bin_lists_reader(std::vector<std::string>& lists);

	~Turbo_bin_lists_reader();

	ReturnStatus Read(size_t offset, size_t size_to_read, char*& pData);

	ReturnStatus ReadNext(size_t size_to_read, char*& pData);

  private:
	std::vector<std::string> file_lists;
	Turbo_bin_mmapper bin_reader;
};

#endif
