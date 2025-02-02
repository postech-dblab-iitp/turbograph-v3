#include "analytics/util/timer.hpp"

double get_current_time() {
	timeval t;
	gettimeofday(&t, 0);
	double sec = (double)t.tv_sec + (double)t.tv_usec/1000000;
	return sec;
}

double get_cpu_time() {
	return (double)clock() / CLOCKS_PER_SEC;
}


