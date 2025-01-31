#ifndef PAGE_H
#define PAGE_H

/*
 * Design of the page
 *
 * This class implements two types of slotted page data structure. One for
 * the fixed-sized page and the other for the variable-sized page. Both types 
 * of pages have the same structure as follows:
 *
 * Page Structure
 * Data: (adj_list) growing up from the first position in a page
 * 			 each data can be accessed by using offset
 * Last entry: # of slots
 * Slots: (src_vid, offset) growing up from the last position in a page 
 *        (last-1 ~ last-# of slots)
 *
 * Compared to the fixed-sized page, the variable-sized page has two 
 * additional member variables, "page_size" and "page_capacity",  to 
 * represent the size of the page.
 */

#include "Global.hpp"

#define MIN_PAGEID 0
#define MAX_PAGEID (INT_MAX)

const PageID INVALID_PAGE = -1;
typedef int32_t PageStatType;


struct RecordID {
	PageID page_id;
	int16_t slot_id;
};

struct Slot {
	node_t src_vid;
	uint16_t end_offset;
};

struct Slot32 {
	node_t src_vid;
	node_t end_offset;
	node_t delta_end_offset[2];
};

class Page {
  public:
	Page() {

	}

	/* Get the memory address to the page */
	char* GetData() {
		return data;
	}

	/* Get the entry at the given position */
	node_t& operator[](int64_t entry) {
		node_t* pagedata = (node_t *)data;
		return pagedata[entry];
	}

	/* Get the number of entries in this page */
	node_t& NumEntry() {
		int64_t location = TBGPP_PAGE_SIZE / sizeof(node_t) -1;
		node_t* pagedata = (node_t *)data;
		return pagedata[location];
	}

	/* Get the size of stored data in this page */
    int64_t NumData() {
        Slot* slot_ = GetSlot(NumEntry() - 1);
        return slot_->end_offset;
    }

	/* Get the pointer to a slot at the given index */
	Slot* GetSlot(int64_t slot_index) {
		return (Slot*)(data + TBGPP_PAGE_SIZE - sizeof(node_t) - sizeof(Slot) * (slot_index + 1));
	}
    
	/* Do linear search to find the slot that stores the data of a given vertex */
    inline int32_t FindSlotLinear(int64_t first_slot_index, int64_t last_slot_index, node_t target_vid) {
        for (int idx = first_slot_index; idx <= last_slot_index; idx++) {
            if (GetSlot(idx)->src_vid == target_vid) return idx;
        }
        return -1;
    }

	/* Do binary search to find the slot that stores the data of a given vertex */
    inline int32_t FindSlotBinary(int64_t first_slot_index, int64_t last_slot_index, node_t target_vid) {
        int64_t low = first_slot_index;
        int64_t high = last_slot_index;
        int64_t mid;
        if (GetSlot(low)->src_vid == target_vid) return low;
        while (low <= high) {
            mid = (low + high)/2;
            if (GetSlot(mid)->src_vid == target_vid)
                break;
            else if (GetSlot(mid)->src_vid > target_vid) {
                high = mid - 1;
            } else if (GetSlot(mid)->src_vid < target_vid) {
                low = mid + 1;
            } else {
                ALWAYS_ASSERT(0);
            }
        }
        if (low > high) return -1;
        else return mid;
    }

  private:
	char data[TBGPP_PAGE_SIZE];
};

template <typename SlotType = Slot>
class VariableSizedPage {
  public:

	/* Construct this variable-sized page */
	VariableSizedPage() : page_size (-1), data (nullptr) {
	}

	/* Initialize this variable-sized page */
	void Initialize(int64_t sz, int64_t cp, char* d) {
		page_size = sz;
		page_capacity = cp;
		data = d;
	}

	/* Get the memory address of this page */
	char* GetData() {
		return data;
	}
	
	/* Get the page size */
	int64_t GetSize() {
		return page_size;
	}

	/* Get the capacity of this page */
	int64_t GetCapacity() {
		return page_capacity;
	}

	/* get the entry at the given poisition */
	node_t& operator[](int64_t entry) {
		node_t* pagedata = (node_t *) data;
		return pagedata[entry];
	}

	/* Get the number of entries in this page */
	node_t& NumEntry() {
		ALWAYS_ASSERT (page_size % sizeof(node_t) == 0);
		int64_t location = page_size / sizeof(node_t) -1;
		node_t* pagedata = (node_t *) data;
		return pagedata[location];
	}

	/* Get the pointer to the slot at the given position */
	SlotType* GetSlot(int64_t slot_index) {
		return (SlotType*)(data + page_size - sizeof(node_t) - sizeof(SlotType) * (slot_index + 1));
	}
	
	/* Get ... */
    int64_t GetBeginOffsetOfDeltaInsert(int64_t slot_index) {
        return GetBeginOffset(slot_index) + GetNumEntriesOfCurrent(slot_index);
    }

    int64_t GetBeginOffsetOfDeltaDelete(int64_t slot_index) {
        return GetBeginOffsetOfDeltaInsert(slot_index) + GetNumEntriesOfDeltaInsert(slot_index);
    }
	
	/* Get the begin offset of the data */
    int64_t GetBeginOffset(int64_t slot_index) {
		if (slot_index == 0) {
			return 0;
		} else {
			return GetSlot(slot_index - 1)->delta_end_offset[1];
        }
	}
    
    int64_t GetNumEntriesOfDeltaInsert(int64_t slot_index) {
        return GetSlot(slot_index)->delta_end_offset[0] - GetSlot(slot_index)->end_offset;
    }
	
    int64_t GetNumEntriesOfDeltaDelete(int64_t slot_index) {
        return GetSlot(slot_index)->delta_end_offset[1] - GetSlot(slot_index)->delta_end_offset[0];
    }
    
    int64_t GetNumEntriesOfCurrent(int64_t slot_index) {
        if (slot_index == 0) {
            return GetSlot(slot_index)->end_offset; // - 0
        } else {
            return GetSlot(slot_index)->end_offset - GetSlot(slot_index - 1)->delta_end_offset[1];
        }
    }
	
	/* Get the number of ... */
    int64_t GetNumEntries(int64_t slot_index) {
        if (slot_index == 0) {
            return GetSlot(slot_index)->delta_end_offset[1]; // - 0
		} else {
			return GetSlot(slot_index)->delta_end_offset[1] - GetSlot(slot_index - 1)->delta_end_offset[1];
		}
	}

  private:
	int64_t page_size; /* the size of this vairble-sized page */
	int64_t page_capacity; /* the capacity of this page */
	char*	data; /* the memory address to the buffer space of this page */
};
#endif
