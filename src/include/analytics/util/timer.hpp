#pragma once

/*
 * Design of the timer
 *
 * This class provides several functions of timer. It is mainly used to measure 
 * performance by putting a timer before and after the code block to measure time.
 * Each instance of this class has as many timers as N_Timers, and you can access 
 * each timer by giving the timer number as a parameter of the provided functions.
 * Internally, it uses the gettimeofday() function to get the time called.
 */

#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include "iostream"

#define N_Timers 32
//#define DISABLE_TIMER

double get_current_time();
double get_cpu_time();

class turbo_timer {
  public:
	turbo_timer() {
		clear_all_timers();
	}

	void start_timer(int i) {
#ifdef DISABLE_TIMER
    return;
#endif
		_timers[i] = get_current_time();
	}

	void reset_timer(int i) {
#ifdef DISABLE_TIMER
    return;
#endif
		_timers[i] = get_current_time();
		_acc_time[i] = 0;
	}

	double stop_timer(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		_acc_time[i] += elapsed_time;
		return elapsed_time;
	}

	double get_timer_without_stop(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		return elapsed_time;
	}

	double get_timer(int i) {
#ifdef DISABLE_TIMER
    return 0.0;
#endif
		return _acc_time[i];
	}

    void clear_all_timers() {
#ifdef DISABLE_TIMER
      return;
#endif
        for (int i = 0; i < N_Timers; i++) {
            _timers[i] = 0;
            _acc_time[i] = 0;
        }
    }

  private:
	double _timers[N_Timers]; // timer
	double _acc_time[N_Timers]; // accumulated time
};

class cpu_timer {
  public:
	cpu_timer() {
		for (int i = 0; i < N_Timers; i++) {
			_acc_time[i] = 0;
		}
	}

	void start_timer(int i) {
		_timers[i] = get_cpu_time();
	}

	void reset_timer(int i) {
		_timers[i] = get_cpu_time();
		_acc_time[i] = 0;
	}

	double stop_timer(int i) {
		double t = get_cpu_time();
		double elapsed_time = t - _timers[i];
		_acc_time[i] += elapsed_time;
		return elapsed_time;
	}

	double get_timer(int i) {
		return _acc_time[i];
	}

  private:
	double _timers[N_Timers]; // timer
	double _acc_time[N_Timers]; // accumulated time
};

class debug_timer {
  public:
	debug_timer() {
		clear_all_timers();
	}

	void start_timer(int i) {
		_timers[i] = get_current_time();
	}

	void reset_timer(int i) {
		_timers[i] = get_current_time();
		_acc_time[i] = 0;
	}

	double stop_timer(int i) {
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		_acc_time[i] += elapsed_time;
		return elapsed_time;
	}

	double get_timer_without_stop(int i) {
		double t = get_current_time();
		double elapsed_time = t - _timers[i];
		return elapsed_time;
	}

	double get_timer(int i) {
		return _acc_time[i];
	}

    void clear_all_timers() {
        for (int i = 0; i < N_Timers; i++) {
            _timers[i] = 0;
            _acc_time[i] = 0;
        }
    }

  private:
	double _timers[N_Timers]; // timer
	double _acc_time[N_Timers]; // accumulated time
};

