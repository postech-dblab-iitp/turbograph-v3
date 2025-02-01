#ifndef UTIL_H_
#define UTIL_H_
#include <numa.h>
#include <execinfo.h>
#include <signal.h>
#include <string.h>
#include <atomic>
#include <iostream>
#include <cstdlib>
#include <stdexcept>
#include <assert.h>
#include <stdio.h>
#include <endian.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/mman.h>
#include <libaio.h>
#include <tbb/concurrent_queue.h>
// #include <jemalloc/jemalloc.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/stat.h>

// #include "analytics/util/concurrentqueue/concurrentqueue.h"
#include "analytics/util/half.hpp"
#include "analytics/core/TG_NWSMTaskContext.hpp"

#include "analytics/core/TypeDef.hpp"
#include "analytics/util/timer.hpp"
#include "analytics/core/Global.hpp"
#include "analytics/util/json11.hpp"
#include "analytics/datastructure/TG_Vector.hpp"

#define SUCCESS_STAT 0
#define likely(x) __builtin_expect((x),1)

inline bool check_file_exists (const std::string& name) {
    struct stat buffer;   
    return (stat (name.c_str(), &buffer) == 0); 
}
inline int CreateDirectory(std::string& path) {
    mode_t old_umask;
    old_umask = umask(0);
    int is_error = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
    if (is_error != 0) {
        fprintf(stderr, "Cannot create a directory (%s); ErrorCode=%d\n", path.c_str(), errno);
        umask(old_umask);
        return -1;
    }
    umask(old_umask);
    return 0;
}

inline int DeleteFile(std::string& path) {
    int is_error = remove(path.c_str());
    if (is_error != 0 && errno != 2) {
        fprintf(stderr, "The file (or directory) (%s) cannot be deleted; ErrorCode=%d\n", path.c_str(), errno);
        return -1;
    }
    return 0;
}
/**
 * checks if a specific directory exists
 * @param dir_path the path to check
 * @return if the path exists
 */
inline bool dirExists(std::string& dir_path)
{
	struct stat sb;

	if (stat(dir_path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode))
		return true;
	else
		return false;
}

/**
 * deletes all the files in a folder (but not the folder itself). optionally
 * this can traverse subfolders and delete all contents when recursive is true
 * @param dirpath the directory to delete the contents of (can be full or
 * relative path)
 * @param recursive true = delete all files/folders in all subfolders
 *                  false = delete only files in toplevel dir
 * @return SUCCESS_STAT on success
 *         errno on failure, values can be from unlink or rmdir
 * @note this does NOT delete the named directory, only its contents
 */
inline int DeleteFilesInDirectory(std::string& dirpath, bool recursive)
{
	if (dirpath.empty())
		return SUCCESS_STAT;

	DIR *theFolder = opendir(dirpath.c_str());
	struct dirent *next_file;
	char filepath[1024];
	int ret_val;

	if (theFolder == NULL)
		return errno;

	while ( (next_file = readdir(theFolder)) != NULL )
	{
		// build the path for each file in the folder
		sprintf(filepath, "%s/%s", dirpath.c_str(), next_file->d_name);
        std::string filepath_str(filepath);

		//we don't want to process the pointer to "this" or "parent" directory
		if ((strcmp(next_file->d_name,"..") == 0) ||
			(strcmp(next_file->d_name,"." ) == 0) )
		{
			continue;
		}

		//dirExists will check if the "filepath" is a directory
		if (dirExists(filepath_str))
		{
			if (!recursive)
				//if we aren't recursively deleting in subfolders, skip this dir
				continue;

			ret_val = DeleteFilesInDirectory(filepath_str, recursive);

			if (ret_val != SUCCESS_STAT)
			{
				closedir(theFolder);
                DeleteFile(dirpath);
				return ret_val;
			}
		}

		ret_val = remove(filepath);
		//ENOENT occurs when i folder is empty, or is a dangling link, in
		//which case we will say it was a success because the file is gone
		if (ret_val != SUCCESS_STAT && ret_val != ENOENT)
		{
			closedir(theFolder);
            DeleteFile(dirpath);
			return ret_val;
		}

	}

    closedir(theFolder);
    DeleteFile(dirpath);
	return SUCCESS_STAT;
}


struct PtrAndSize {
	char* ptr;
	int64_t size;
};

class SYMBOL_INFO;

class TG_ThreadContexts {
  public:
	static __thread int64_t thread_id;
	static __thread int64_t core_id;
	static __thread int64_t socket_id;

	static __thread bool per_thread_buffer_overflow;
	static __thread bool run_delta_nwsm;
    static __thread PageID pid_being_processed;
	static __thread TG_NWSMTaskContext* ctxt;
};

//This structure mirrors the one found in /usr/include/asm/ucontext.h
typedef struct _sig_ucontext {
	unsigned long     uc_flags;
	struct ucontext   *uc_link;
	stack_t           uc_stack;
	struct sigcontext uc_mcontext;
	sigset_t          uc_sigmask;
} sig_ucontext_t;

void crit_err_hdlr(int sig_num, siginfo_t* info, void* ucontext);

class StackTracer {
  public:
	static StackTracer stack_tracer;

	StackTracer();
	~StackTracer();

	static StackTracer& GetStackTracer() {
		return stack_tracer;
	}

	void print_call_stack();

	template <typename output_stream>
	friend output_stream& operator<<(output_stream& out, StackTracer& o) {
		abort();
		return out;
	}
  private:
	SYMBOL_INFO* symbol;
	void* stack[100];
};

static const long PAGE_SIZE = sysconf(_SC_PAGESIZE);

// padded, aligned primitives
template <typename T>
class aligned_padded_elem {
  public:

	template <class... Args>
	aligned_padded_elem(Args &&... args)
		: elem(std::forward<Args>(args)...) {
		ALWAYS_ASSERT(sizeof(aligned_padded_elem<T>) % CACHELINE_SIZE == 0);
	}

	T elem;
	CACHE_PADOUT;

	// syntactic sugar- can treat like a pointer
	inline T & operator*() {
		return elem;
	}
	inline const T & operator*() const {
		return elem;
	}
	inline T * operator->() {
		return &elem;
	}
	inline const T * operator->() const {
		return &elem;
	}

  private:
	inline void
	__cl_asserter() const {
		ALWAYS_ASSERT((sizeof(*this) % CACHELINE_SIZE) == 0);
	}
} CACHE_ALIGNED;

#define SWAP(a,b) {swap_temp = a; a = b; b = swap_temp;}

class NumaHelper {
  public:
	static int64_t sockets;
	static int64_t cores;
	static int64_t cores_per_socket;

	static int64_t numa_policy;

	static NumaHelper numa_helper;

	NumaHelper() {
		NumaHelper::sockets = numa_num_configured_nodes();
		NumaHelper::cores = numa_num_configured_cpus();
		NumaHelper::cores_per_socket = NumaHelper::cores / NumaHelper::sockets;

        UserArguments::NUM_TOTAL_CPU_CORES = NumaHelper::cores;
    }

	static void print_numa_info() {
		fprintf(stdout, "(# of sockets) = %ld, (# of cores) = %ld\n", NumaHelper::sockets, NumaHelper::cores);
	}

	static int64_t num_sockets() {
		return sockets;
	}
	static int64_t num_cores() {
		return cores;
	}

	// TODO
	// Assuming 'compact'
	static int64_t assign_core_id_by_round_robin (int64_t thread_id, int64_t affinity_from, int64_t affinity_to) {
		return affinity_from + (thread_id % (affinity_to - affinity_from + 1));
	}

	static int64_t assign_core_id_scatter (int64_t thread_id, int64_t affinity_from, int64_t affinity_to) {
		int64_t socket_id = thread_id % NumaHelper::sockets;
		int64_t cores_per_socket = ((affinity_to - affinity_from + 1) / NumaHelper::sockets);
		int64_t core_idx_from = (affinity_from + NumaHelper::sockets -1) / NumaHelper::sockets;
		if (cores_per_socket == 0) cores_per_socket = 1;
		int64_t core_offset = (thread_id / NumaHelper::sockets) % cores_per_socket;
		int64_t core_id = (socket_id * NumaHelper::cores_per_socket) + core_idx_from + core_offset;

		ALWAYS_ASSERT (core_idx_from >= 0 && core_idx_from < NumaHelper::cores_per_socket);
		ALWAYS_ASSERT (socket_id >= 0 && socket_id < NumaHelper::sockets);
		ALWAYS_ASSERT (core_id >= 0 && core_id < NumaHelper::cores);
		return (core_id);
	}

	// Assuming 'scatter'
	static int64_t assign_core_id_by_round_robin (int64_t thread_id) {
		return (thread_id % NumaHelper::cores);
	}

	static int64_t get_socket_id_by_omp_thread_id(int thread_id) {
		return thread_id % NumaHelper::sockets;
	}
	static int64_t get_socket_id_by_core_id(int core_id) {
		return core_id / NumaHelper::cores_per_socket;
	}
	static int64_t get_socket_id(int thread_id) {
		return thread_id % NumaHelper::sockets;
	}

    // mmap with MAP_HUGETLB vs. madvise with MADV_HUGEPAGE
    // https://stackoverflow.com/questions/30470972/using-mmap-and-madvise-for-huge-pages
	static char* malloc_by_mmap_with_hugetlb (int64_t sz) {
        int64_t hugepage_sz = 2 * 1024 * 1024L;
        int64_t sz_hugetlb = (sz / hugepage_sz + 1) * hugepage_sz;
		char* buf = (char*) alloc_mmap(sz_hugetlb);
		if (buf == MAP_FAILED) {
			perror("mmap");
			exit(EXIT_FAILURE);
		}
        madvise(buf, sz_hugetlb, MADV_HUGEPAGE);
		return buf;
	}

	static char* alloc_mmap (int64_t sz) {
		ALWAYS_ASSERT (sz > 0);
		char * array = (char *)mmap(NULL, sz, PROT_READ | PROT_WRITE,
		                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (array == MAP_FAILED) {
			fprintf(stdout, "[NumaHelper] mmap failed; sz = (%ld) ErrorNo = %d\n", (int64_t) sz, errno);
            LOG_ASSERT(false);
		}
		INVARIANT (array != NULL);
		return array;
	}

	static char* alloc_numa_interleaved_memory (int64_t sz) {
		ALWAYS_ASSERT (sz > 0);
		//char* tmp = (char*) numa_alloc_interleaved(sz);
        
        int64_t hugepage_sz = 2 * 1024 * 1024L;
        int64_t sz_hugetlb = (sz / hugepage_sz + 1) * hugepage_sz;
		char* tmp = (char*) malloc_by_mmap_with_hugetlb(sz_hugetlb);
        numa_interleave_memory(tmp, sz_hugetlb, numa_all_nodes_ptr);
        
		ALWAYS_ASSERT (tmp != NULL);
		if (tmp == NULL) {
			fprintf (stderr, "[Error] NumaHelper::alloc_num_interleaved_memory(%ld) failed; ErrorCode = %d\n", (int64_t) sz, errno);
			throw std::runtime_error("[Exception] NumaHelper::alloc_numa_interleaved_memory failed\n");
		}
		return tmp;
	}
	
	static char* alloc_numa_memory_two_part (int64_t sz) {
		char* tmp = alloc_numa_interleaved_memory(sz);
		int64_t pg_sz = numa_pagesize();
		int64_t sz_ = (((sz / pg_sz) * pg_sz) / 2) * 2;
		numa_tonode_memory(tmp, sz_/2, 0);
		numa_tonode_memory(tmp + sz_/2, sz_/2, 1);
		return tmp;
	}

	static char* alloc_numa_memory (int64_t sz) {
		ALWAYS_ASSERT (sz > 0);
		char* tmp;
		switch (NumaHelper::numa_policy) {
		case 0:
			tmp = (char*) numa_alloc_interleaved(sz);
			break;
		case 1:
			tmp = (char*) numa_alloc_onnode(sz, 0);
			break;
		case 2:
			tmp = (char*) numa_alloc_interleaved(sz);
			break;
		default:
			abort();
			break;
		}
		if (tmp == NULL) {
			fprintf (stderr, "[Error] NumaHelper::alloc_num_memory(%ld) failed; ErrorCode = %d\n", (int64_t) sz, errno);
			throw std::runtime_error("[Exception] NumaHelper::alloc_num_memory failed\n");
		}
		INVARIANT (tmp != NULL);
		return tmp;
	}

	static char* alloc_numa_local_memory(int64_t sz, int64_t socket_id) {
		char* tmp = (char*) numa_alloc_onnode(sz, socket_id);
		if (tmp == NULL) {
			fprintf(stdout, "[NumaHelper::alloc_numa_local_memory] Failed; errno = %d\n", errno);
			abort();
		}
		return tmp;
	}
	static char* alloc_numa_local_memory(int64_t sz) {
		int64_t socket_id = TG_ThreadContexts::socket_id;
		return alloc_numa_local_memory(sz, socket_id);
	}

    template <typename T>
	static void free_numa_local_memory (T* buf, int64_t sz) {
		ALWAYS_ASSERT (buf != NULL);
		ALWAYS_ASSERT (sz > 0);
		numa_free((char*)buf, sz);
	}
	
    template <typename T>
	static void free_numa_memory (T* buf, int64_t sz) {
		ALWAYS_ASSERT (buf != NULL);
		ALWAYS_ASSERT (sz > 0);
		numa_free((char*)buf, sz);
		//munmap(buf, PROT_READ | PROT_WRITE);
	}

	template <typename T>
	static void free_mmap_memory (T* buf) {
		ALWAYS_ASSERT (buf != NULL);
		munmap(buf, PROT_READ | PROT_WRITE);
	}
};

inline static int64_t nlogm(int64_t n, int64_t m) {
	int digit = 0;
	if(m & 0xFFFFFFFF00000000LL) {
		m = m >> 32;
		digit = digit | 32;
	}
	if(m & 0x00000000FFFF0000LL) {
		m = m >> 16;
		digit = digit | 16;
	}
	if(m & 0x000000000000FF00LL) {
		m = m >> 8;
		digit = digit | 8;
	}
	if(m & 0x00000000000000F0LL) {
		m = m >> 4;
		digit = digit | 4;
	}
	if(m & 0x000000000000000CLL) {
		m = m >> 2;
		digit = digit | 2;
	}
	if(m & 0x0000000000000002LL) {
		m = m >> 1;
		digit = digit | 1;
	}
	return n * (digit + 1);
}

//using half_float::half;
//using namespace half_float::detail;

template <typename T> inline size_t Encode(T val, uint8_t* buf);
template <typename T> inline T Decode(uint8_t* buf, int64_t& begin, int64_t& end);

template<> inline size_t Encode<uint64_t>(uint64_t val, uint8_t* buf) {
    uint8_t* p = buf;
    while (val >= 128) {
        *p++ = 0x80 | (val & 0x7f);
        val >>= 7;
    }
    *p++ = uint8_t(val);
    return size_t(p - buf);
}

template<> inline size_t Encode<int64_t>(int64_t val, uint8_t* buf) {
    uint8_t* p = buf;
    uint64_t temp_val = (uint64_t)val;
    while (temp_val >= 128) {
        *p++ = 0x80 | (temp_val & 0x7f);
        temp_val >>= 7;
    }
    *p++ = uint8_t(temp_val);
    return size_t(p - buf);
}

template<> inline size_t Encode<int32_t>(int32_t val, uint8_t* buf) {
    uint8_t* p = buf;
    uint32_t temp_val = (uint32_t)val;
    while (temp_val >= 128) {
        *p++ = 0x80 | (temp_val & 0x7f);
        temp_val >>= 7;
    }
    *p++ = uint8_t(temp_val);
    return size_t(p - buf);
}

template<> inline size_t Encode<bool>(bool val, uint8_t* buf) {
    uint8_t* p = buf;
    *p++ = uint8_t(val);
    return size_t(p - buf);
}

template<> inline size_t Encode<float>(float val, uint8_t* buf) {
    uint16_t* p = (uint16_t*) buf;
    //*p = std::move(half_float::detail::half_cast<half_float::half>(val).get());
    *p = half_float::detail::half_cast<half_float::half>(val).get();
    return sizeof(uint16_t);
}

template<> inline size_t Encode<double>(double val, uint8_t* buf) {
    uint16_t* p = (uint16_t*) buf;
    //*p = std::move(half_float::detail::half_cast<half_float::half>(val).get());
    *p = half_float::detail::half_cast<half_float::half>((float)val).get();
    return sizeof(uint16_t);
}

template<> inline bool Decode<bool>(uint8_t* buf, int64_t& begin, int64_t& end) {
    bool val;
    val = static_cast<bool>(buf[begin]);
    begin++;
    return val;
}

template<> inline int64_t Decode<int64_t>(uint8_t* buf, int64_t& begin, int64_t& end) {
    INVARIANT(begin < end);
    const int8_t* begin_ = reinterpret_cast<const int8_t*>(&buf[begin]);
    const int8_t* end_ = reinterpret_cast<const int8_t*>(&buf[end]);
    const int8_t* p = begin_;
    int64_t val;
    uint64_t val_ = 0;

    //end is always greater than or equal to begin, so this subtraction is safe
    if (likely(size_t(end_ - begin_) >= 10)) { // fast path
        int64_t b;
        do {
            b = *p++;
            val_ = (b & 0x7f);
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 7;
            if (b >= 0) break;

            b = *p++;
            val_ |= (b & 0x7f) << 14;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 21;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 28;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 35;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 42;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 49;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 56;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x01) << 63;
            if (b >= 0) break;
            
            INVARIANT(false);
        } while (false);
    } else {
        int shift = 0;
        while (p != end_ && *p < 0) {
            val_ |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
            shift += 7;
        }
        if (p == end_) {
            fprintf(stdout, "%ld, begin = %ld end = %ld\n", static_cast<int64_t>(val_), begin, end);
            INVARIANT(false);
        }
        val_ |= static_cast<uint64_t>(*p++) << shift;
    }

    begin += (p - begin_);
    val = static_cast<int64_t>(val_);
    return val;
}

template<> inline int32_t Decode<int32_t>(uint8_t* buf, int64_t& begin, int64_t& end) {
    const int8_t* begin_ = reinterpret_cast<const int8_t*>(&buf[begin]);
    const int8_t* end_ = reinterpret_cast<const int8_t*>(&buf[end]);
    const int8_t* p = begin_;
    int32_t val;
    uint32_t val_ = 0;

    //end is always greater than or equal to begin, so this subtraction is safe
    if (likely(size_t(end_ - begin_) >= 5)) { // fast path
        int64_t b;
        do {
            b = *p++;
            val_ = (b & 0x7f);
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 7;
            if (b >= 0) break;

            b = *p++;
            val_ |= (b & 0x7f) << 14;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 21;
            if (b >= 0) break;
            
            b = *p++;
            val_ |= (b & 0x7f) << 28;
            if (b >= 0) break;
            
            INVARIANT(false);
        } while (false);
    } else {
        int shift = 0;
        while (p != end_ && *p < 0) {
            val_ |= static_cast<uint32_t>(*p++ & 0x7f) << shift;
            shift += 7;
        }
        if (p == end_) {
            INVARIANT(false);
        }
        val_ |= static_cast<uint32_t>(*p++) << shift;
    }

    begin += (p - begin_);
    val = static_cast<int32_t>(val_);
    return val;
}

template<> inline float Decode<float>(uint8_t* buf, int64_t& begin, int64_t& end) {
    //if (begin + 2 > end) fprintf(stdout, "begin = %ld, end = %ld\n", begin, end);
    //INVARIANT(begin + 2 <= end);
    uint16_t data = buf[begin];
    data << 8;
    data += buf[begin + 1];
    begin += 2;
    return half_float::detail::half2float<float>(data);
}

template<> inline double Decode<double>(uint8_t* buf, int64_t& begin, int64_t& end) {
    //if (begin + 2 > end) fprintf(stdout, "begin = %ld, end = %ld\n", begin, end);
    //INVARIANT(begin + 2 <= end);
    uint16_t data = buf[begin];
    data << 8;
    data += buf[begin + 1];
    begin += 2;
    return static_cast<double>(half_float::detail::half2float<float>(data));
}

/* Error when compile kjhong
template <typename T>
inline int EncodeLZ4(T* source, uint8_t* dest, int maxDestSize) {
    return LZ4_compress_default((char*) source, (char*) dest, sizeof(T), maxDestSize);
}
*/

inline int parseLine(char* line){
    int i = strlen(line);
    while (*line < '0' || *line > '9') line++;
    line[i-3] = '\0';
    i = atoi(line);
    return i;
}

inline int getValueOfVirtualMemoryUsage(){
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmSize:", 7) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

inline int getValueOfPhysicalMemoryUsage(){
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}


inline void printProcessMemoryUsage(const char* tag, int64_t MBytes = 0, int64_t TotalMBytes = 0) {
    int cur_vm = getValueOfVirtualMemoryUsage(); 
    int cur_pm = getValueOfPhysicalMemoryUsage(); 

    fprintf(stdout, "[ProcessMemoryUsage] VM = %d, PM = %d. %s added %ld MB among %ld MB\n", cur_vm, cur_pm, tag, MBytes, TotalMBytes);
}

#endif
