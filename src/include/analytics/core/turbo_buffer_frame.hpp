#ifndef _TURBO_BUFFER_FRAME_H_
#define _TURBO_BUFFER_FRAME_H_

#include <atomic>
#include <thread>
#include <sstream>
#include <set>

#include "analytics/datastructure/page.hpp"
#include "analytics/util/atom.hpp"

class turbo_buffer_manager;

class Turbo_Buffer_Frame {
	friend class turbo_buffer_manager;

  public:

	Turbo_Buffer_Frame() {
		m_current_page_id = INVALID_PAGE;   // pid == INVALID_PAGE <=> empty frame
		m_data = NULL;
		m_pinCount = 0;
		m_referenced = false;
		buf_mgr = NULL;
	}

	~Turbo_Buffer_Frame() {
		ALWAYS_ASSERT (m_pinCount == 0);
	}

	void Init(Page * pg, turbo_buffer_manager* buf_mgr_);
	void Pin();
	void PinUnsafe();
	void Unpin();
	void UnpinUnsafe();
	void UnpinUnsafeWithoutUpdatingNumFrames(bool& pin_count_zero);
	void UnsetReferenced();

	inline void Clear() {
		ALWAYS_ASSERT( m_pinCount == 0 );
		ALWAYS_ASSERT (atom_lock.IsLocked());
		m_current_page_id = INVALID_PAGE;
		if (m_referenced) {
			UnsetReferenced();
		}
	}

	inline void SetPageID(PageID pid) {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		m_current_page_id = pid;
	}

	inline Page* GetPage() const {
		ALWAYS_ASSERT( m_pinCount > 0 );
		return m_data;
	}
    inline Page* GetPageForClose() const{
        ALWAYS_ASSERT( m_pinCount == 0);
        return m_data;
    }
	inline PageID GetPageID() const {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return m_current_page_id;
	}
	inline bool IsPinned() const {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return (m_pinCount > 0);
	}
	inline bool IsValid() const {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return (m_current_page_id != INVALID_PAGE);
	}
	inline bool IsVictim() {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return (!IsPinned() && !IsReferenced());
	}
	inline bool IsReferenced() {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return m_referenced;
	}
	inline bool IsInMemory() {
		ALWAYS_ASSERT( m_pinCount >= 0 );
		return IsValid();
	}

	inline void Lock() {
		atom_lock.lock();
	}

	inline void Unlock() {
		atom_lock.unlock();
	}

	inline bool IsLocked() {
		return atom_lock.IsLocked();
	}

	atom   atom_lock;
  private:

	Page*	m_data;
	PageID	m_current_page_id;
	int16_t m_pinCount;
	bool m_referenced;
    turbo_buffer_manager* buf_mgr;
};

#endif
