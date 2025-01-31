// Written by. juneyoung lee
// 2014.12.01

#ifndef SCOPEDLOCK_H
#define SCOPEDLOCK_H

#include "eXDB_dist_internal.hpp"

template<class T>
class scopedlock {
  private:
	T *olt;

  public:
	scopedlock(T *obj) {
		this->olt = obj;
		obj->lock(core_id::my_core_id());
	}
	~scopedlock() {
		olt->unlock(core_id::my_core_id());
	}

};

#endif
