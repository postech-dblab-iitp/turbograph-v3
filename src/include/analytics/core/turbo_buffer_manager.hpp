#ifndef TURBO_BUFFER_MANAGER_H
#define TURBO_BUFFER_MANAGER_H

#include "scopedlock.hpp"
#include "turbo_clock_replacer.hpp"
#include "turbo_callback.hpp"
#include "disk_aio_factory.hpp"
#include "TwoLevelBitMap.hpp"
#include "timer.hpp"

#define ENTER_BUF_MGR_CRIT_SECTION

#define NO_COUNT_COMPULSORY_PAGE_MISS 1

extern void AsyncCallbackUpdateDirectTable(diskaio::DiskAioRequestUserInfo &user_info);

class TurboDB;

#define NUM_BUFFER_COUNTERS 256

struct PaddedBufferCounter {
	int64_t hit;
	int64_t miss;
	CACHE_PADOUT;
};

struct PaddedTimer {
	turbo_timer timer;
	CACHE_PADOUT;
};

class turbo_buffer_manager {
  public:
	turbo_buffer_manager();
	~turbo_buffer_manager();

	ReturnStatus Init(std::size_t buffer_pool_size_bytes, std::size_t num_pages, std::size_t bytes_per_sector);
	void Close();

	void ReportTimers(bool reset);

	ReturnStatus OpenCDB(const char* file_name, int version_id, EdgeType e_type, DynamicDBType d_type, bool bDirectIO=true, int flag=O_RDWR);
	ReturnStatus CloseCDB(bool rm, int version_id, EdgeType e_type, DynamicDBType d_type);

	ReturnStatus ReadPage(AioRequest& req, diskaio::DiskAioInterface* my_io=0);
 	ReturnStatus WritePage(AioRequest& req, diskaio::DiskAioInterface* my_io=0);

	ReturnStatus PinPageCallback(PartitionID edge_partition_id, AioRequest& req, diskaio::DiskAioInterface** my_io);
	ReturnStatus PinPageCallbackLikelyCached(PartitionID edge_partition_id, AioRequest& req, diskaio::DiskAioInterface** my_io);
	ReturnStatus ReplacePageCallback (PartitionID partition_id, AioRequest& req, PageID new_pid, bool& replaced, int old_version=0, diskaio::DiskAioInterface** my_io=0);


	ReturnStatus PrePinPage(PartitionID edge_partition_id, AioRequest& req);
	ReturnStatus PrePinPageUnsafe(PartitionID edge_partition_id, AioRequest& req);

	ReturnStatus GetPagePtr_Unsafe(PageID pid, Page*& page);

	ReturnStatus UnpinPageUnsafe(PartitionID partition_id, PageID pid );
	ReturnStatus UnpinPageUnsafeBulk(PartitionID partition_id, PageID* pids, int num_pages);
	ReturnStatus UnpinPage( PartitionID partition_id, PageID pid );
	ReturnStatus UnpinPages(BitMap<PageID>& bitmap);
	ReturnStatus UnpinAllPages();
	void FlushDirtyPages();
	void ClearAllFrames();

	FrameID FindFrameAndPinPageIfCached(PageID pid);
	FrameID FindFrame( PartitionID partition_id, PageID pid );
	Turbo_Buffer_Frame& GetFrame(FrameID fid);

	PageAndFrameID PickVictim();
	int64_t GetSocketIdByFrameId(FrameID fid);

	bool IsPinnedPage(PageID pid);
	bool IsPageInMemory(PageID pid);

	FrameID GetNumberOfAvailableFrames();
	FrameID GetNumberOfPinnedFrames();
	FrameID GetNumberOfUnReferencedFrames();
	FrameID GetNumberOfFrames();

	size_t GetNumberOfTotalPages();

	void ResetPageReadIO();
	void ResetPageWriteIO();
	int64_t GetPageReadIO();
	int64_t GetPageWriteIO();

	void UpdateBufferHitCount(PageID pid, bool hit);
	void GetPageBufferHitMissRatio(double& hit, double& miss);
	void GetPageBufferHitMissBytes(int64_t& hit, int64_t& miss);
	void GetSnapshotOfResidentPages (TwoLevelBitMap<PageID>& bitmap);
	void GetHitPages (TwoLevelBitMap<PageID>& input_page, TwoLevelBitMap<PageID>& hit_page);

	turbo_direct_table* GetDirectTable();

	int ConvertToFileID(int version_id, EdgeType e_type, DynamicDBType d_type=INSERT, bool use_fulllist_db=true);

	// debug
	void dump_list_of_pinned_frames();
	char* begin_of_buffer();	// [begin, end)
	char* end_of_buffer();
	bool contains(char* ptr);
	int64_t num_read_pages, num_write_pages;

	static turbo_buffer_manager buf_mgr;
	
	turbo_direct_table* m_directTable;
	std::atomic<FrameID> NumberOfUnpinnedFrames;

	void SetNumPages(size_t p) { m_NumPages = p; }

  private:
	BitMap<PageID> compulsory_miss;

	Turbo_Buffer_Frame* m_frames;
	Replacer** m_replacer;

	size_t file_id;
	int file_ids[4][2][MAX_NUM_VERSIONS]; // [OUTEDGE/INEDGE/OUTEDGEFULLLIST/INEDGEFULLLIST][INSERT/DELETE][version_id]

	int64_t m_SystemPageSize;
	char*	m_pBufStartAddr;

	std::size_t m_FrameTableAllocSize;
	std::size_t m_BufAllocSize;
	std::size_t m_NumFrames;
	std::size_t m_NumPages;
	std::size_t m_BufAllocSizePerSocket;
	std::size_t m_NumFramesPerSocket;
	std::size_t m_BytesPerSector;
	std::vector<PaddedBufferCounter> buffer_counters;
	PaddedTimer timer_[MAX_NUM_CPU_CORES];
};

#endif
