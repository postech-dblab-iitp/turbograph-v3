#ifndef TURBO_CLOCK_REPLACER_H
#define TURBO_CLOCK_REPLACER_H

#include <atomic>

#include "turbo_dist_internal.hpp"
#include "turbo_buffer_frame.hpp"
#include "turbo_direct_table.hpp"


#define ADD_CNT -1

class Turbo_Buffer_Frame;
class turbo_direct_table;

struct PageAndFrameID{
	PageID pid;
	FrameID fid;
};

class VictimIndex {
  public:
	VictimIndex() {
		m_index = 0;
	}
	~VictimIndex() {}
	unsigned long get() {
		return std::atomic_fetch_add((std::atomic<unsigned long>*) &m_index, (unsigned long)1);
	}


  private:
	unsigned long m_index;
	CACHE_PADOUT;
};

class Replacer  {
  public :
	Replacer() {}
	virtual ~Replacer() {}
	inline virtual PageAndFrameID PickVictim() = 0;
};

class Turbo_Clock : public Replacer {
  public:

	Turbo_Clock(FrameID NumFrames, Turbo_Buffer_Frame* bufFrames, turbo_direct_table* table) : numOfBuf(-1), m_frames(NULL), m_directTable(NULL) {
		Init(NumFrames, bufFrames, table);
	}
	Turbo_Clock() : numOfBuf(-1), m_frames(NULL), m_directTable(NULL) {}

	void Init(FrameID NumFrames, Turbo_Buffer_Frame* bufFrames, turbo_direct_table* table) {
		numOfBuf = NumFrames;
		m_frames = bufFrames;
		m_directTable = table;
	}

	~Turbo_Clock() { }

	inline PageAndFrameID PickVictim() {
		ALWAYS_ASSERT (numOfBuf != -1);
		ALWAYS_ASSERT (m_frames != NULL);
		ALWAYS_ASSERT (m_directTable != NULL);

		unsigned long cur;

		int frameNo = INVALID_FRAME;
		bool check_frame;
		PageID pid = INVALID_PAGE;
search:
		while(1) {
			cur = m_current.get() % numOfBuf;
			pid = m_frames[cur].GetPageID();

			ALWAYS_ASSERT (cur >= 0 && cur < numOfBuf);

			if(pid != INVALID_PAGE) {
				if( true == m_directTable->trylock(pid) ) {
					if( pid != m_frames[cur].GetPageID() ) {
						m_directTable->unlock(pid);
						continue;
					}
				} else {
					continue;
				}
			}

			// (pid == INVALID_PAGE || (pid != INVALID_PAGE && m_frames[cur].GetPageID() != pid))

			//4
			if(m_frames[cur].IsVictim()) {
				if(m_frames[cur].atom_lock.trylock() ) {
					if(m_frames[cur].IsVictim()) {
						if( INVALID_PAGE == pid && m_frames[cur].IsValid() ) {
							ALWAYS_ASSERT (false);
							pid = m_frames[cur].GetPageID();
							if( true != m_directTable->trylock(pid) ) {
								m_frames[cur].atom_lock.unlock();
								continue;
							}
						}

						ALWAYS_ASSERT (m_frames[cur].IsVictim());
						if(m_frames[cur].IsValid()) {
							// If the page is not empty, write the frame
							// out and remove the (pid, frameNo) pair
							// from the m_directTable.
							m_directTable->Delete(m_frames[cur].GetPageID());
						}
						m_frames[cur].Clear();

						// !Debugged on Feb. 12, 2003.
						// Before return, we should move forward the clock pointer.
						// Otherwise, we would favor the current frame over other m_frames.
						frameNo = cur;
						//m_frames[cur].atom_lock.unlock();

						//m_current = (m_current + 1) % numOfBuf;

						if( INVALID_PAGE != pid ) {
							ALWAYS_ASSERT (!m_directTable->IsValid(pid));
							m_directTable->unlock(pid);
						}

						break;
					} else {
						m_frames[cur].atom_lock.unlock();
					}
				}
			} else {
				if (!m_frames[cur].IsPinned()) {
					ALWAYS_ASSERT (m_frames[cur].IsReferenced());
					m_frames[cur].UnsetReferenced();
					//    if (PartitionStatistics::my_machine_id() == 0) fprintf(stdout, "second chance on frame %ld\n", cur);
				}
				//m_current = (m_current+1) % numOfBuf;
			}

			if( INVALID_PAGE != pid )
				m_directTable->unlock(pid);
		}
		/*
		   if(cur == INVALID_FRAME || !m_frames[cur].IsVictim()) {
		   goto search;
		   }
		   */

		ALWAYS_ASSERT (cur != INVALID_FRAME);
		ALWAYS_ASSERT (m_frames[cur].IsVictim());
		ALWAYS_ASSERT (m_frames[cur].IsLocked());

		// if reach here then no free buffer;
		PageAndFrameID page_and_frame_id;
		page_and_frame_id.pid = pid;
		page_and_frame_id.fid = frameNo;
		return page_and_frame_id;
	}
  private :

	VictimIndex m_current;
	FrameID numOfBuf;
	Turbo_Buffer_Frame* m_frames;
	turbo_direct_table* m_directTable;

	CACHE_PADOUT;
} CACHE_ALIGNED;

#endif
