#include "analytics/core/turbo_buffer_frame.hpp"
#include "analytics/core/turbo_buffer_manager.hpp"

void Turbo_Buffer_Frame::Init(Page * pg, turbo_buffer_manager* buf_mgr_) {
    ALWAYS_ASSERT (pg != NULL);
    m_current_page_id = INVALID_PAGE;   // pid == INVALID_PAGE <=> empty frame
    m_data = pg;
    m_pinCount = 0;
    m_referenced = false;
    buf_mgr = buf_mgr_;
}
void Turbo_Buffer_Frame::PinUnsafe() {
    ALWAYS_ASSERT(m_current_page_id != INVALID_PAGE);
    m_pinCount++;
    if (m_pinCount == 1) {
        buf_mgr->NumberOfUnpinnedFrames--;
    }
}
void Turbo_Buffer_Frame::Pin() {
    ALWAYS_ASSERT(m_current_page_id != INVALID_PAGE);
    ALWAYS_ASSERT (atom_lock.IsLocked());
    m_pinCount++;

    if (m_pinCount == 1) {
        buf_mgr->NumberOfUnpinnedFrames--;
    }
}

void Turbo_Buffer_Frame::UnpinUnsafe() {
    ALWAYS_ASSERT (IsValid());
    ALWAYS_ASSERT (m_pinCount > 0);
    m_pinCount--;
    if( m_pinCount == 0 ) {
        buf_mgr->NumberOfUnpinnedFrames++;
        if (!m_referenced) {
            m_referenced = true;
        }
    }
    //ALWAYS_ASSERT(m_pinCount == 0);
}


void Turbo_Buffer_Frame::UnpinUnsafeWithoutUpdatingNumFrames(bool& pin_count_zero) {
    ALWAYS_ASSERT (IsValid());
    ALWAYS_ASSERT (m_pinCount > 0);
    pin_count_zero = false;
    if(--m_pinCount == 0) {
        if (!m_referenced) {
            m_referenced = true;
        }
	pin_count_zero = true;
    }
}
void Turbo_Buffer_Frame::Unpin() {
    ALWAYS_ASSERT (IsValid());
    ALWAYS_ASSERT (m_pinCount > 0);
    ALWAYS_ASSERT (atom_lock.IsLocked());
    m_pinCount--;
    if( m_pinCount == 0 ) {
        buf_mgr->NumberOfUnpinnedFrames++;
        if (!m_referenced) {
            m_referenced = true;
        }
    }
    //ALWAYS_ASSERT(m_pinCount == 0);
}

void Turbo_Buffer_Frame::UnsetReferenced() {
    ALWAYS_ASSERT ( m_pinCount >= 0 );
    INVARIANT (m_referenced);
    m_referenced = false;
}
