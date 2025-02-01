#ifndef TG_BUILD_DB_H
#define TG_BUILD_DB_H

/*
 * Design of the TG_BuildDDB
 *
 * This class is a variant of the class TG_BuildDB for dynamic graph. The only 
 * difference is that this class assumes that the vid range per edge sub-chunk 
 * has already been calculated. Therefore, unlike the TG_BuildDB class, the 
 * FindVidRangeForSubchunks() function is not executed.
 */

#include <fstream>
#include <vector>
#include <unordered_map>
#include <chrono>
#include "analytics/glog/logging.hpp"
#include <sys/stat.h>
#include <unistd.h>

#include "analytics/Turbograph.hpp"
#include "analytics/core/Global.hpp"
#include "analytics/core/TurboDB.hpp"
#include "analytics/core/TG_DistributedVectorBase.hpp"
#include "analytics/io/Turbo_bin_io_handler.hpp"
#include "analytics/io/HomogeneousPageWriter.hpp"
#include "analytics/io/EdgePairListReader.hpp"
#include "analytics/datastructure/VidRangePerPage.hpp"
#include "analytics/datastructure/disk_aio_factory.hpp"

#define TIMEDIFF_MILLISEC(begin, end) ((double)std::chrono::duration_cast<std::chrono::milliseconds>((end) - (begin)).count())
#ifndef MIN
#define MIN(x, y) ((x) < (y) ? (x) : (y));
#endif

void create_directory(std::string& path, bool delete_if_exist) {
	if (delete_if_exist) remove(path.c_str());
	mode_t old_umask;
	old_umask = umask(0);
	int is_error = mkdir((path).c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
	if (is_error != 0 && errno != ENOENT && errno != EEXIST) {
		fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", (path).c_str(), errno);
		umask(old_umask);
		return;
	}
	umask(old_umask);
}

class TG_BuildDDB  {

	int argc_;
	char** argv_;
  public:

	TG_BuildDDB() {}

	// TODO
	// Input:
	// Output:

	void run(int argc, char** argv) {
		argc_ = argc;
		argv_ = argv;

		setbuf(stdout, NULL);
		// std::string glog_filename;

		std::vector<std::string> binary_file;
		std::vector<std::string> db_file;
		split_string(std::string(argv_[0]), '/', binary_file);
		split_string(std::string(argv_[2]), '/', db_file);

		if (PartitionStatistics::my_machine_id() == 0) {
			for (int i = 0; i < this->argc_; i++) {
				std::cout << "argv[" << i << "]=" << argv_[i] << std::endl;
			}
		}

		// if(this->argc_ >= 7) {
		// 	glog_filename.append(binary_file[binary_file.size()-1]).append("-");
		// 	for (auto& str : db_file) {
		// 		glog_filename.append(str).append("-");
		// 	}
		// 	for(int i = 3; i <= 5; i++) {
		// 		glog_filename.append(argv_[i]).append("-");
		// 	}
		// 	glog_filename.append(argv_[6]);
		// }
		// char* _glog_filename = (char *) malloc( sizeof(char) * (strlen(glog_filename.c_str()) + 1));
		// strncpy(_glog_filename, glog_filename.c_str(),  (strlen(glog_filename.c_str()) + 1));
		// google::InitGoogleLogging(_glog_filename);
		// std::string glog_dir("/mnt/sdb1/glog/");
		char* name = getlogin();
		getlogin_r(name, 5);
		// glog_dir.append(name);
		std::cout << std::flush;
		// FLAGS_log_dir=glog_dir.c_str();
		FLAGS_logtostderr = 0;

		MPI::Init_thread(argc, argv, MPI_THREAD_MULTIPLE);
		int res;
		DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, 1);

		PartitionStatistics::init();

		auto begin_time = std::chrono::steady_clock::now();
		if(PartitionStatistics::my_machine_id() == 0) {
			std::cout << "Start building a DB" << std::endl;
		}

		//user arguments
		std::string raw_path, new_path, new_range_path, edge_path, range_path, subrange_path, original_subrange_path, edgefile_path;
		std::string hist_path;
		std::string fpid_path;
		int64_t num_files;
		float cut_ratio;
		int64_t num_subchunks_per_edge_chunk = 36; // TODO Tunable Parameter
        DBCatalog catalog;
        int builddb_for_full_list;
        bool build_ddb = true;

		if (argc == 8 || argc == 9) {
			raw_path = argv[1];
			new_path = argv[2];
			fpid_path = argv[2];
			cut_ratio = atof(argv[3]);
			num_files = std::stoll(argv[4]);
			UserArguments::VECTOR_PARTITIONS = num_files;
			num_files *= PartitionStatistics::num_machines();
			num_subchunks_per_edge_chunk = std::stoll(argv[5]);
			num_files *= num_subchunks_per_edge_chunk;
            original_subrange_path = argv[6];
            edgefile_path = argv[7];
            if (argc == 9) {
                builddb_for_full_list = std::atoi(argv[8]);
                if (builddb_for_full_list == 1) build_ddb = false;
                else if (builddb_for_full_list == 2) build_ddb = true;
            }
		} else {
			std::cout << "usage: " << argv[0] << " <raw_data_path> <new_data_path> <cut vertex ratio> <#of files per machine> <num_subchunks_per_edge_chunk> <original_db_path> <edgefile path>" << std::endl;
			return;
		}

        int is_exist = access(edgefile_path.c_str(), R_OK | W_OK);
        if (is_exist < 0) {
            if (errno == ENOENT) {
                fprintf(stdout, "File not exists\n");
            } else if (errno == EACCES) {
                fprintf(stdout, "File exists, but cannot read or write it\n");
            }
        } else {
            create_directory(new_path, false);
        }
        //InitCatalog(new_path, catalog);

		// Paths to Input Files
		//edge_path = raw_path+"/OutEdgesWithInsertion10/";
		range_path = raw_path+"/PartitionRanges/";
		hist_path = raw_path + "/LocalHistogram/";

		// Output Path
		new_range_path = new_path + "/PartitionRanges/";
		subrange_path = new_path + "/EdgeSubChunkVidRanges/";
		new_path = new_path + "/edgedb";

		std::vector<std::string> edgefile_list;
		std::vector<std::string> rangefile_list;

		//DiskAioFactory::GetAioFileList(edge_path, edgefile_list);
		DiskAioFactory::GetAioFileList(range_path, rangefile_list);

		for(int64_t i = 0; i <rangefile_list.size(); i++) {
			rangefile_list[i] = range_path + rangefile_list[i];
		}

        if (is_exist >= 0) {
            edgefile_list.resize(1);
            edgefile_list[0] = edgefile_path;
            ALWAYS_ASSERT(edgefile_list.size() == 1);
        }

        ALWAYS_ASSERT(rangefile_list.size() == 1);
		MetaDataReader range_reader(rangefile_list);

		PartitionStatistics::my_num_internal_nodes() = range_reader.MyNumNodes();
		PartitionStatistics::my_first_node_id() = range_reader.FirstNodeId();
		PartitionStatistics::my_last_node_id() = range_reader.LastNodeId();
		PartitionStatistics::num_subchunks_per_edge_chunk() = num_subchunks_per_edge_chunk;

		fprintf(stdout, "[%ld] V = [%ld, %ld]\n", PartitionStatistics::my_machine_id(), range_reader.FirstNodeId(), range_reader.LastNodeId());

		PartitionStatistics::wait_for_all();
		PartitionStatistics::replicate();

        if (is_exist < 0) {
            fprintf(stdout, "Machine-%lld DONE\n", PartitionStatistics::my_machine_id());
            PartitionStatistics::wait_for_all();
            return;
        }

        if (build_ddb) {
            create_directory(new_range_path, true);
            create_directory(subrange_path, true);

            int32_t num_edge_chunks = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;

            std::vector<std::vector<int32_t>> InDegreeHistogramPerEdgeChunk;
            std::vector<int64_t> SumOfInDegreePerEdgeChunk;
            std::vector<std::vector<int64_t>> SubchunkVidRangePerEdgeChunk;
            node_t bin_width;

            WriteVidRangePerMachine(new_range_path);

            //ReadInDegreeHistogram(hist_path, InDegreeHistogramPerEdgeChunk, bin_width);

            //FindVidRangeForSubchunks(bin_width, num_edge_chunks, num_subchunks_per_edge_chunk, InDegreeHistogramPerEdgeChunk, SumOfInDegreePerEdgeChunk, SubchunkVidRangePerEdgeChunk);
            ReadVidRangeForSubchunks(original_subrange_path, SubchunkVidRangePerEdgeChunk);

            WriteVidRangeOfSubchunks(subrange_path, num_edge_chunks, num_subchunks_per_edge_chunk, SubchunkVidRangePerEdgeChunk);

            //auto edge_begin_time = std::chrono::steady_clock::now();
            ReadEdgesAndBuildDB(fpid_path, new_path, edgefile_list, num_files, num_edge_chunks, num_subchunks_per_edge_chunk, SubchunkVidRangePerEdgeChunk);
            //auto edge_end_time = std::chrono::steady_clock::now();
        } else {
            // build db for full list
            system_fprintf(0, stdout, "Build Db for Full List\n");
            PartitionStatistics::num_subchunks_per_edge_chunk() = 1;
            int32_t num_edge_chunks = 1;
            num_files = 1;
            ReadEdgesAndBuildDBForFullList(new_path, edgefile_list, num_files);
        }

		fprintf(stdout, "Machine-%lld DONE\n", PartitionStatistics::my_machine_id());
		PartitionStatistics::wait_for_all();
		//auto end_time = std::chrono::steady_clock::now();
		//system_fprintf(0, stdout, "DB Build END, Elapsed : %.3f, Edge Elapsed : %.3f\n", TIMEDIFF_MILLISEC(begin_time, end_time) / 1000, TIMEDIFF_MILLISEC(edge_begin_time, edge_end_time) / 1000);
		return;
	}

	~TG_BuildDDB() {
		PartitionStatistics::wait_for_all();
		PartitionStatistics::close();
		PartitionStatistics::wait_for_all();
		MPI::Finalize();
	}

  private:

	ReturnStatus MakePage(Page& page, EdgePairEntry<node_t>& edge) {
		node_t prev_src;
		if(page.NumEntry() == 0) {
			prev_src = -1;
		} else {
			prev_src = page.GetSlot(page.NumEntry() -1)->src_vid;
		}

		if(prev_src != edge.src_vid_) {
			int64_t free_space = 0;
			if(page.NumEntry() != 0) free_space = page.GetSlot(page.NumEntry() -1)->end_offset;
			if((void *)&page[free_space] >= (void *)page.GetSlot(page.NumEntry())) return DONE;

			page.NumEntry()++;
			ALWAYS_ASSERT(page[free_space] == 0);
			page[free_space] = edge.dst_vid_;
			page.GetSlot(page.NumEntry() -1)->src_vid = edge.src_vid_;
			page.GetSlot(page.NumEntry() -1)->end_offset = free_space + 1;
		} else {
			int64_t free_space = page.GetSlot(page.NumEntry() -1)->end_offset;
			if((void *)&page[free_space] >= (void *)page.GetSlot(page.NumEntry() -1)) return DONE;
			ALWAYS_ASSERT(page[free_space] == 0);
			page[free_space] = edge.dst_vid_;
			page.GetSlot(page.NumEntry() -1)->end_offset = free_space +1;
		}
		return OK;
	}

	ReturnStatus CleanPage(Page& page) {
		for(int64_t i = 0 ; i < TBGPP_PAGE_SIZE / sizeof(node_t) ; i++) page[i] = 0;
		return DONE;
	}

	int64_t dst_vid_to_file_num (int64_t dst_vid, int64_t num_subchunks_per_edge_chunk, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
		int64_t idx = dst_vid / PartitionStatistics::my_num_internal_nodes();
		idx *= UserArguments::VECTOR_PARTITIONS;
		int j;
		for(j = idx; j < idx + UserArguments::VECTOR_PARTITIONS; j++) {
			for(int i = 0; i < num_subchunks_per_edge_chunk; i++) {
				if(dst_vid <= SubchunkVidRangePerEdgeChunk[j][i])
					return (j * num_subchunks_per_edge_chunk + i);
			}
		}
		fprintf(stdout, "dst_vid = %lld, idx = %lld, Subchu...[idx][num_subchunks_per_edge_chunk-1] = %lld\n", dst_vid, j, SubchunkVidRangePerEdgeChunk[j][num_subchunks_per_edge_chunk-1]);
		ALWAYS_ASSERT(false);
		abort();
	}

	void WriteVidRangePerMachine(std::string& new_range_path) {
		std::ofstream range_output_os;
		new_range_path += "/" + std::to_string(PartitionStatistics::my_machine_id());
		range_output_os.open(new_range_path);
		if (!range_output_os.is_open()) {
			fprintf(stderr, "[BuildDB] ofstream failed to open the file (%s)\n", new_range_path.c_str());
			return;
		}
		range_output_os << PartitionStatistics::my_first_node_id() << "\t" << PartitionStatistics::my_last_node_id();
		range_output_os.close();
	}

	void ReadInDegreeHistogram(std::string& hist_path, std::vector<std::vector<int32_t>>& InDegreeHistogramPerEdgeChunk, node_t& bin_width) {
		std::ifstream* local_histogram = new std::ifstream[PartitionStatistics::num_machines()];
		InDegreeHistogramPerEdgeChunk.resize(PartitionStatistics::num_machines());
		for(int i = 0; i < PartitionStatistics::num_machines(); i++) {
			std::string file_name = hist_path + std::to_string(i) + ".txt";
			local_histogram[i].open(file_name);

			std::string temp_str;
			std::stringstream temp_stream;
			int64_t num_bins, degree;
			int64_t cnt = 0;

			getline(local_histogram[i], temp_str);
			temp_stream.clear();
			temp_stream.str(temp_str);

			temp_stream >> num_bins >> bin_width;
			InDegreeHistogramPerEdgeChunk[i].resize(num_bins);

			while(getline(local_histogram[i], temp_str)) {
				temp_stream.clear();
				temp_stream.str(temp_str);

				temp_stream >> degree;
				InDegreeHistogramPerEdgeChunk[i][cnt] = degree;
				cnt++;
			}
			ALWAYS_ASSERT(cnt == num_bins);
			local_histogram[i].close();
		}
		delete local_histogram;
	}
	
    void ReadVidRangeForSubchunks(std::string& original_subrange_path, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
        int64_t num_edge_chunks = PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS;
        SubchunkVidRangePerEdgeChunk.resize(num_edge_chunks);
        for (int64_t i = 0; i < num_edge_chunks; i++) {
            SubchunkVidRangePerEdgeChunk[i].resize(2); //XXX hard coding
        }

		std::ifstream original_subrange;
        std::string file_name = original_subrange_path + "/tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt";
        fprintf(stdout, "Read EdgeSubchunkVidRanges file %s\n", file_name.c_str());
        original_subrange.open(file_name);
        int64_t start_vid, end_vid;
        int64_t cnt = 0, cnt2 = 0;
        std::string temp_str;
        std::stringstream temp_stream;
        
        while(getline(original_subrange, temp_str)) {
            temp_stream.clear();
            temp_stream.str(temp_str);

            temp_stream >> start_vid >> end_vid;
            SubchunkVidRangePerEdgeChunk[cnt][cnt2] = end_vid;
            cnt2 = (cnt2 + 1) % 2; //XXX hard coding
            if (cnt2 == 0) cnt++;
        }
	}

	void FindVidRangeForSubchunks(int32_t bin_width, int32_t num_edge_chunks, int32_t num_subchunks_per_edge_chunk, std::vector<std::vector<int32_t>>& InDegreeHistogramPerEdgeChunk, std::vector<int64_t>& SumOfInDegreePerEdgeChunk, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
		SumOfInDegreePerEdgeChunk.resize(PartitionStatistics::num_machines() * UserArguments::VECTOR_PARTITIONS);
		SubchunkVidRangePerEdgeChunk.resize(num_edge_chunks);

		int64_t idx2 = 0;
		// Find Vid Range for Subchunks for each Edge Chunk
		for(int i = 0; i < num_edge_chunks; i++) {
			SumOfInDegreePerEdgeChunk[i] = 0;
			SubchunkVidRangePerEdgeChunk[i].resize(num_subchunks_per_edge_chunk);

			int partition_id = i / UserArguments::VECTOR_PARTITIONS;
			int chunk_id = i % UserArguments::VECTOR_PARTITIONS;
			int64_t start_bin = (InDegreeHistogramPerEdgeChunk[partition_id].size() * chunk_id) / UserArguments::VECTOR_PARTITIONS;
			int64_t end_bin = ceil((double)(InDegreeHistogramPerEdgeChunk[partition_id].size() * (chunk_id + 1)) / UserArguments::VECTOR_PARTITIONS);
			for(int64_t idx = start_bin; idx < end_bin; idx++) {
				SumOfInDegreePerEdgeChunk[i] += InDegreeHistogramPerEdgeChunk[partition_id][idx];
			}
			fprintf(stdout, "SumOfInDegreePerEdgeChunks[%lld] = %lld\n", i, SumOfInDegreePerEdgeChunk[i]);

			int64_t num_edges_per_subchunk = (SumOfInDegreePerEdgeChunk[i] + num_subchunks_per_edge_chunk - 1) / num_subchunks_per_edge_chunk;

			int64_t acc_sum = 0;
			int64_t cur_subchunk_idx = 0;
			int64_t degree_sum = 0;

			for(int64_t idx = start_bin; idx < end_bin; idx++) {
				acc_sum += InDegreeHistogramPerEdgeChunk[partition_id][idx];
				degree_sum += InDegreeHistogramPerEdgeChunk[partition_id][idx];
				if(acc_sum >= num_edges_per_subchunk * (cur_subchunk_idx + 1)) {
					SubchunkVidRangePerEdgeChunk[i][cur_subchunk_idx] = ceil((double)((idx2 + idx + 1) * bin_width - 1) / 64) * 64;
					cur_subchunk_idx++;
					if(cur_subchunk_idx == num_subchunks_per_edge_chunk) {
						SubchunkVidRangePerEdgeChunk[i][cur_subchunk_idx - 1] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
						break;
					}
				}
			}
			if(cur_subchunk_idx < num_subchunks_per_edge_chunk) {
				for(int j = cur_subchunk_idx; j < num_subchunks_per_edge_chunk; j++) {
					SubchunkVidRangePerEdgeChunk[i][j] = PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(partition_id, chunk_id).GetEnd();
				}
			}
			if(chunk_id == UserArguments::VECTOR_PARTITIONS - 1) {
				idx2 += InDegreeHistogramPerEdgeChunk[partition_id].size();
			}
		}
	}

	void WriteVidRangeOfSubchunks(std::string& subrange_path, int32_t num_edge_chunks, int32_t num_subchunks_per_edge_chunk, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
		std::ofstream subrange_file((subrange_path + "/tbgpp_" + std::to_string(PartitionStatistics::my_machine_id()) + ".txt").c_str());
		for(int i = 0; i < num_edge_chunks; i++) {
			for(int j = 0; j < num_subchunks_per_edge_chunk; j++) {
				if(i == 0 && j == 0) {
					ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
					subrange_file << 0 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
				} else if(j == 0) {
					ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
					subrange_file << SubchunkVidRangePerEdgeChunk[i - 1][num_subchunks_per_edge_chunk - 1] + 1 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
				} else {
					ALWAYS_ASSERT(SubchunkVidRangePerEdgeChunk[i][j] <= PartitionStatistics::machine_id_and_chunk_idx_to_vid_range(i / UserArguments::VECTOR_PARTITIONS, i % UserArguments::VECTOR_PARTITIONS).GetEnd());
					subrange_file << SubchunkVidRangePerEdgeChunk[i][j - 1] + 1 << "\t" << SubchunkVidRangePerEdgeChunk[i][j] << "\n";
				}
			}
		}
		subrange_file.close();
	}
	
    void ReadEdgesAndBuildDBForFullList(std::string& new_path, std::vector<std::string>& edgefile_list, int32_t num_files) {
        INVARIANT(num_files == 1);
		EdgePairEntry<node_t> edge_iter;
		EdgePairEntry<node_t> prev_edge_iter;
		prev_edge_iter.src_vid_ = prev_edge_iter.dst_vid_ = -1;

		EdgePairEntry<int64_t> edge_iter_64bits;
		std::vector<Range<node_t> > range_to_append(num_files);
		std::vector<Page> page_to_append(num_files);
		std::vector<HomogeneousPageWriter*> edge_files(num_files, NULL);
		for(int64_t i = 0 ; i < num_files ; i++) {
			CleanPage(page_to_append[i]);
			edge_files[i] = new HomogeneousPageWriter((new_path+"/edgedbForFullList").c_str());
			range_to_append[i].Set(-1,-1);
		}

		EdgePairListFilesReader<BinaryFormatEdgePairListReader<EdgePairEntry<int64_t>, false>, EdgePairEntry<int64_t>> edges_reader(edgefile_list);
		VidRangePerPage vidrangeperpage(num_files, new_path + "VidRangePerPageForFullList");

		int64_t num_edges = 0;
		// Read All Edges
		while(edges_reader.getNext(edge_iter_64bits) == OK) {
			edge_iter = edge_iter_64bits;

			//remove duplicate edges & self_edges
			if(edge_iter == prev_edge_iter) continue;
			if(edge_iter.src_vid_ == edge_iter.dst_vid_) continue;

			if (!(prev_edge_iter.src_vid_ == -1 || prev_edge_iter < edge_iter)) {
				fprintf(stdout, "prev [%ld, %ld], cur [%ld, %ld], num_edges = %ld\n", prev_edge_iter.src_vid_, prev_edge_iter.dst_vid_, edge_iter.src_vid_, edge_iter.dst_vid_, num_edges);
			}
			INVARIANT(prev_edge_iter.src_vid_ == -1 || prev_edge_iter < edge_iter);
			INVARIANT(edge_iter.src_vid_ >= PartitionStatistics::my_first_node_id());
			INVARIANT(edge_iter.src_vid_ <= PartitionStatistics::my_last_node_id());
			INVARIANT(edge_iter.dst_vid_ >= 0);
			INVARIANT(edge_iter.dst_vid_ < PartitionStatistics::num_total_nodes());

            int64_t dst_file = 0;
			if (dst_file < 0 || dst_file >= num_files) {
				fprintf(stdout, "edge (%lld, %lld), dst_file = %lld, num_files = %lld\n", edge_iter.src_vid_, edge_iter.dst_vid_, dst_file, num_files);
			}
			ALWAYS_ASSERT(0 <= dst_file && dst_file < num_files);

			if(MakePage(page_to_append[dst_file], edge_iter) == DONE) {
				ALWAYS_ASSERT(0 <= range_to_append[dst_file].GetBegin() < PartitionStatistics::num_total_nodes());
				ALWAYS_ASSERT(0 <= range_to_append[dst_file].GetEnd() < PartitionStatistics::num_total_nodes());
				ALWAYS_ASSERT(range_to_append[dst_file].GetBegin() <= range_to_append[dst_file].GetEnd());
				vidrangeperpage.push_back(dst_file, range_to_append[dst_file]);
				edge_files[dst_file]->AppendPage(page_to_append[dst_file]);
				CleanPage(page_to_append[dst_file]);

				range_to_append[dst_file].Set(edge_iter.src_vid_, edge_iter.src_vid_);
				MakePage(page_to_append[dst_file], edge_iter);
			} else {
				if(range_to_append[dst_file].GetBegin() == -1)
					range_to_append[dst_file].SetBegin(edge_iter.src_vid_);
				range_to_append[dst_file].SetEnd(edge_iter.src_vid_);
			}

			prev_edge_iter = edge_iter;
			num_edges++;
		}

		// Flush the remaining edges in the pages
		for(int64_t i = 0 ; i < num_files; i++) {
			node_t* pagedata = (node_t*)page_to_append[i].GetData();
			if(pagedata[TBGPP_PAGE_SIZE/sizeof(node_t) -1] != 0) {
				vidrangeperpage.push_back(i, range_to_append[i]);
				edge_files[i]->AppendPage(page_to_append[i]);
			}
		}

		// Close edge files
		for(int64_t i = 0 ; i < num_files ; i++) {
			edge_files[i]->Close(false);
			delete edge_files[i];
		}

		// Write VidRangePerPage Data into Disk
		vidrangeperpage.Save();
	}

	void ReadEdgesAndBuildDB(std::string& fpid_path, std::string& new_path, std::vector<std::string>& edgefile_list, int32_t num_files, int32_t num_edge_chunks, int32_t num_subchunks_per_edge_chunk, std::vector<std::vector<int64_t>>& SubchunkVidRangePerEdgeChunk) {
		EdgePairEntry<node_t> edge_iter;
		EdgePairEntry<node_t> prev_edge_iter;
		prev_edge_iter.src_vid_ = prev_edge_iter.dst_vid_ = -1;

		EdgePairEntry<int64_t> edge_iter_64bits;
		std::vector<Range<node_t> > range_to_append(num_files);
		std::vector<Page> page_to_append(num_files);
		std::vector<HomogeneousPageWriter*> edge_files(num_files, NULL);
		for(int64_t i = 0 ; i < num_files ; i++) {
			CleanPage(page_to_append[i]);
			edge_files[i] = new HomogeneousPageWriter((new_path+std::to_string(i)).c_str());
			range_to_append[i].Set(-1,-1);
		}

		EdgePairListFilesReader<BinaryFormatEdgePairListReader<EdgePairEntry<int64_t>, false>, EdgePairEntry<int64_t>> edges_reader(edgefile_list);
		VidRangePerPage vidrangeperpage(num_files, new_path + "VidRangePerPage");

		int64_t num_edges = 0;
		// Read All Edges
		while(edges_reader.getNext(edge_iter_64bits) == OK) {
			edge_iter = edge_iter_64bits;

			//remove duplicate edges & self_edges
			if(edge_iter == prev_edge_iter) continue;
			if(edge_iter.src_vid_ == edge_iter.dst_vid_) continue;

			if (!(prev_edge_iter.src_vid_ == -1 || prev_edge_iter < edge_iter)) {
				fprintf(stdout, "prev [%ld, %ld], cur [%ld, %ld], num_edges = %ld\n", prev_edge_iter.src_vid_, prev_edge_iter.dst_vid_, edge_iter.src_vid_, edge_iter.dst_vid_, num_edges);
			}
			INVARIANT(prev_edge_iter.src_vid_ == -1 || prev_edge_iter < edge_iter);
			INVARIANT(edge_iter.src_vid_ >= PartitionStatistics::my_first_node_id());
			INVARIANT(edge_iter.src_vid_ <= PartitionStatistics::my_last_node_id());
			INVARIANT(edge_iter.dst_vid_ >= 0);
			INVARIANT(edge_iter.dst_vid_ < PartitionStatistics::num_total_nodes());

			int64_t dst_file = dst_vid_to_file_num(edge_iter.dst_vid_, num_subchunks_per_edge_chunk, SubchunkVidRangePerEdgeChunk);
			if (dst_file < 0 || dst_file >= num_files) {
				fprintf(stdout, "edge (%lld, %lld), dst_file = %lld, num_files = %lld\n", edge_iter.src_vid_, edge_iter.dst_vid_, dst_file, num_files);
			}
			ALWAYS_ASSERT(0 <= dst_file && dst_file < num_files);

			if(MakePage(page_to_append[dst_file], edge_iter) == DONE) {
				ALWAYS_ASSERT(0 <= range_to_append[dst_file].GetBegin() < PartitionStatistics::num_total_nodes());
				ALWAYS_ASSERT(0 <= range_to_append[dst_file].GetEnd() < PartitionStatistics::num_total_nodes());
				ALWAYS_ASSERT(range_to_append[dst_file].GetBegin() <= range_to_append[dst_file].GetEnd());
				vidrangeperpage.push_back(dst_file, range_to_append[dst_file]);
				edge_files[dst_file]->AppendPage(page_to_append[dst_file]);
				CleanPage(page_to_append[dst_file]);

				range_to_append[dst_file].Set(edge_iter.src_vid_, edge_iter.src_vid_);
				MakePage(page_to_append[dst_file], edge_iter);
			} else {
				if(range_to_append[dst_file].GetBegin() == -1)
					range_to_append[dst_file].SetBegin(edge_iter.src_vid_);
				range_to_append[dst_file].SetEnd(edge_iter.src_vid_);
			}

			prev_edge_iter = edge_iter;
			num_edges++;
		}

		// Flush the remaining edges in the pages
		for(int64_t i = 0 ; i < num_files; i++) {
			node_t* pagedata = (node_t*)page_to_append[i].GetData();
			if(pagedata[TBGPP_PAGE_SIZE/sizeof(node_t) -1] != 0) {
				vidrangeperpage.push_back(i, range_to_append[i]);
				edge_files[i]->AppendPage(page_to_append[i]);
			}
		}

		// Close edge files
		for(int64_t i = 0 ; i < num_files ; i++) {
			edge_files[i]->Close(false);
			delete edge_files[i];
		}

		// Write VidRangePerPage Data into Disk
		vidrangeperpage.Save();

		// Concatenate the Edge Chunks into a Single File.
		size_t total_file_size = 0;
		const size_t transfer_unit = TBGPP_PAGE_SIZE;

		fpid_path += "/FirstPageIds.txt";
		remove(fpid_path.c_str());
		std::ofstream fpid;
		fpid.open(fpid_path.c_str());

		create_directory(new_path, true);
		Turbo_bin_io_handler out_file((new_path + "/edgedb").c_str(), true, true);
		char* buffer = new char[transfer_unit];
		for (auto i = 0; i < num_files; i++) {
			Turbo_bin_io_handler in_file((new_path+std::to_string(i)).c_str(), false, false);
			size_t file_size = in_file.file_size();
			size_t data_left = file_size;
			while (data_left) {
				size_t to_read = MIN(data_left, transfer_unit);
				in_file.Read(file_size - data_left, to_read, buffer);
				out_file.Append(to_read, buffer);
				data_left -= to_read;
			}
			fpid << i << " " << (total_file_size / transfer_unit) << std::endl;
			total_file_size += file_size;
			in_file.Close(true);
		}
		out_file.Close();
		fpid.close();
		delete[] buffer;

	}

    void InitCatalog(std::string new_path, DBCatalog& catalog) {
       struct stat buffer;
       std::string catalog_path = new_path + "/catalog.txt";
       if (stat(catalog_path.c_str(), &buffer) == 0) {
           if (catalog.read_catalog_from_file(catalog_path.c_str()) == FAIL) {
               fprintf(stdout, "[%ld] Bad catalog file: %s\n", PartitionStatistics::my_machine_id(), catalog_path.c_str());
               abort();
           }
       } else {
           catalog.db_catalog_path = catalog_path;
           catalog.db_name = std::string(std::strrchr(new_path.c_str(), '/') + 1);
           catalog.latest_version = -1;
       }
    }
};





#endif
