#ifndef __DISK_AIO_UTIL_H__
#define __DISK_AIO_UTIL_H__

/*
* Design of the DiskAioQueue
*
* It is a circular queue
*/

#include <vector>

namespace diskaio
{

template<typename T>
class DiskAioQueue {
private:
	int size; /* the size of the queue */
	T* buf; /* a pointer to memory space for buffer */
	std::vector<T> data; /* a buffer */
	int start; /* a start index */
	int end; /* a end index */
public:

	/*
	* Construct the disk aio queue
	*/
	DiskAioQueue(int size) {
		this->size = size;
		data.resize(size);
		buf = data.data();
		start = 0; end = 0;
	}

	~DiskAioQueue() {
	}

	/*
	* Push the given number of entries
	*/
	int push(T* entries, int num) {
		int n = size - (end - start);
		n = num < n ? num : n;
		if (n == 0) return 0;
		int i = end % size; int j = (end + n) % size;
		if (i < j) {
			memcpy(buf + i, entries, n * sizeof(T));
			end += n;
			return n;
		}
		memcpy (buf, entries + (size - i), (j) * sizeof(T));
		memcpy (buf + i, entries, (size - i) * sizeof(T));
		end += n;
		return n;		
	}

	/*
	* Fetch the given number of entries
	*/
	int fetch(T* entries, int num) {
		int n = end - start;
		n = n < num ? n : num;
		if (n == 0) return 0;
		int i = start % size; int j = (start + n) % size;
		if (i < j) {
			memcpy(entries, buf + i, n * sizeof(T));
			start += n;
			return n;
		}
		memcpy (entries, buf + i, (size - i) * sizeof(T));
		memcpy (entries + (size - i), buf, (j) * sizeof(T));
		start += n;
		return n;
	}

	/*
	* Get the number of entries in the queue
	*/
	int num_entries() {
		return end - start;
	}
};

}
#endif
