#ifndef VIDRANGEPERPAGE_H
#define VIDRANGEPERPAGE_H

/*
 * Design of the VidRangePerPage
 *
 * This class implements index of the disk pages contatining the edges. We
 * exploit a two-level index on the edge pages by the range of destination 
 * vertices at the first level and by the range of source vertices at the 
 * second level.
 *
 * For the page id given as a parameter, the Get() function returns the range 
 * of the source vertex ids of the edges stored in the page.
 */

#include "analytics/core/TypeDef.hpp"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"

class VidRangePerPage {
  public:

	VidRangePerPage() : num_files(-1), file_path("") {
	}

	VidRangePerPage(int64_t file_num, std::string file_path) {
		OpenVidRangePerPage(file_num, file_path);
	}

	/* Close this vid range per page class */
	void Close(bool rm) {
		if (rm) {
			for(int64_t i = 0 ; i < num_files ; i++) {
				int err = remove((file_path + std::to_string(i)).c_str());
				if (err != 0) {
					fprintf(stdout, "Cannot delete %s ErrorNo=%ld\n", (file_path + std::to_string(i)).c_str(), errno);
				}
			}
		}
		container.clear();
		container2.clear();
	}

	/* Open this vid range per page class */
	void OpenVidRangePerPage(int64_t num_files_, std::string& file_path_) {
		num_files = num_files_;
		file_path = file_path_;
		container.resize(num_files);
	}

	/* Push the range of source vertex ids to the file */
	void push_back(int64_t file, Range<node_t> range) {
        if (range.GetBegin() > range.GetEnd()) {
            fprintf(stdout, "[%ld] range.GetBegin() = %ld, range.GetEnd() = %ld\n", PartitionStatistics::my_machine_id(), range.GetBegin(), range.GetEnd());
        }
		ALWAYS_ASSERT(file < num_files);
		ALWAYS_ASSERT(range.GetBegin() <= range.GetEnd());
		container[file].push_back(range);
	}

	/* Check whether this class has wrong data */
	bool SanityCheck() {
		for(int64_t i = 0 ; i < num_files ; i++) {
			for (int64_t j = 0; j < container[i].size(); j++) {
				Range<node_t> range = container[i][j];
				ALWAYS_ASSERT(range.GetBegin() <= range.GetEnd());
			}
		}
		return true;
	}

	/* Save the ranges to the files */
	void Save() {
		ALWAYS_ASSERT (SanityCheck());
		for(int64_t i = 0 ; i < num_files ; i++) {
			std::string path = file_path + std::to_string(i);
			Turbo_bin_io_handler savefile(path.c_str(), true, true, true);
			for (int64_t j = 0; j < container[i].size(); j++) {
				Range<node_t> range = container[i][j];
				INVARIANT(range.GetBegin() <= range.GetEnd());
				savefile.Write(j * sizeof(Range<node_t>), sizeof(Range<node_t>), (char*) &range);
			}
			savefile.Close(false);
		}
	}
	
	/* Save the ranges to the files */
    void SaveOne() {
		ALWAYS_ASSERT (SanityCheck());
		for(int64_t i = 0 ; i < num_files ; i++) {
			std::string path = file_path;
			Turbo_bin_io_handler savefile(path.c_str(), true, true, true);
			for (int64_t j = 0; j < container[i].size(); j++) {
				Range<node_t> range = container[i][j];
				INVARIANT(range.GetBegin() <= range.GetEnd());
				savefile.Write(j * sizeof(Range<node_t>), sizeof(Range<node_t>), (char*) &range);
			}
			savefile.Close(false);
		}
	}

	/* Load the ranges from the files */
	void Load() {
		container2.clear();
		for(int64_t i = 0 ; i < num_files; i++) {
			Turbo_bin_io_handler loadfile((file_path + std::to_string(i)).c_str());
			INVARIANT(loadfile.file_size() % sizeof(Range<node_t>) == 0);
			container[i].resize(loadfile.file_size() / sizeof(Range<node_t>));
			for (int64_t j = 0; j < container[i].size(); j++) {
				Range<node_t> range(-1, -1);
				loadfile.Read(j * sizeof(Range<node_t>), sizeof(Range<node_t>), (char*) &range);
				INVARIANT(range.GetBegin() <= range.GetEnd());
				container[i][j] = range;
			}
			loadfile.Close(false);
		}

		ALWAYS_ASSERT(SanityCheck());

		for (int64_t i = 0; i < num_files; i++) {
			for (int64_t j = 0; j < container[i].size(); j++) {
				INVARIANT (container[i][j].GetBegin() <= container[i][j].GetEnd());
				container2.push_back(container[i][j]);
			}
		}
	}

	// Input
	// - edge sub-chunk id
	// - Page Idx in [0, # of pages in the edge sub-chunk)

	/* Get the range of source vertex ids in the given page */
	Range<node_t>& Get(int64_t file, int64_t page) {
        if (page < 0 || page > container[file].size()) {
            fprintf(stdout, "[%ld] file_path %s, file %ld, page %ld, container[file].size() %ld\n", PartitionStatistics::my_machine_id(), file_path.c_str(), file, page, container[file].size());
        }
		//ALWAYS_ASSERT (file >= 0 && file < container.size());
		//ALWAYS_ASSERT (page >= 0 && page < container[file].size());
		INVARIANT (file >= 0 && file < container.size());
		INVARIANT (page >= 0 && page < container[file].size());
		return container[file][page];
	}
    
	/* Get the first destination vertex id in the file */
    Range<node_t>& GetFirst(int64_t file) {
		ALWAYS_ASSERT (file >= 0 && file < container.size());
        INVARIANT(!container[file].empty());
		return container[file].front();
	}
	
	/* Get the last destination vertex id in the file */
    Range<node_t>& GetLast(int64_t file) {
		ALWAYS_ASSERT (file >= 0 && file < container.size());
        INVARIANT(!container[file].empty());
		return container[file].back();
	}

	// Input
	// - Page ID in [0, # of pages in the current machine)
	/* Get the range of source vertex ids in the page */
	Range<node_t>& Get(int64_t page) {
		return container2[page];
	}

	// Input
	// - edge sub-chunk id
	// - Page Idx in [0, # of pages in the edge sub-chunk)
	// - Range<node_t>
	/* Set the range of source vertex ids in the page */
	void Set(int64_t grid_partition_id, int64_t pid, Range<node_t> vid_range) {
		container[grid_partition_id][pid] = vid_range;
		return;
	}

	// Input
	// - Page ID in [0, # of pages in the current machine)
	// - Range<node_t>
	/* Set the range of destination vertex idx in the page */
	void Set(int64_t pid, Range<node_t> vid_range) {
		container2[pid] = vid_range;
		return;
	}

  private:
	int64_t num_files; /* the number of files */
	std::string file_path; /* the file path */
	std::vector<std::vector<Range<node_t> > > container; /* the vector that contains the ranges */
	std::vector<Range<node_t>> container2; /* the vector that contains the ranges */
};











#endif
