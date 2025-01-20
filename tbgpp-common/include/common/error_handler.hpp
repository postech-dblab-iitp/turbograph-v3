#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string.h>

namespace s62 {

//This structure mirrors the one found in /usr/include/asm/ucontext.h
typedef struct _sig_ucontext {
	unsigned long     uc_flags;
	struct ucontext   *uc_link;
	stack_t           uc_stack;
	struct sigcontext uc_mcontext;
	sigset_t          uc_sigmask;
} sig_ucontext_t;

void crit_err_hdlr(int sig_num, siginfo_t* info, void* ucontext) {
	sig_ucontext_t * uc = (sig_ucontext_t *)ucontext;

	std::cerr << "crit_err_hdlr() is called" << std::endl;

	// Get the address at the time the signal was raised from the EIP (x86)
	void* caller_address = (void *) uc->uc_mcontext.rip;

	std::cerr << "signal " << sig_num
	          << " (" << strsignal(sig_num)
	          << "), address is "
	          << info->si_addr
	          << " from " << caller_address
	          << std::endl;

	void* array[50];
	int size = backtrace(array, 50);

	std::cerr  << __FUNCTION__ << " backtrace returned "
	           << size << " frames\n\n";

	// overwrite sigaction with caller's address
	array[1] = caller_address;

	char** messages = backtrace_symbols(array, size);

	// skip first stack frame(points  here)
	for (int i = 1; i < size && messages != NULL; ++i) {
        fprintf(stderr, "[bt]: (%d) %s\n", i, messages[i]);
		//std::cerr << "[bt]: (" << i << ") " << messages[i] << std::endl;
	}
	std::cerr << std::endl;

	free(messages);

	exit(EXIT_FAILURE);
}

void set_signal_handler() {
	struct sigaction sigact;
	sigact.sa_sigaction = crit_err_hdlr;
	sigact.sa_flags = SA_RESTART | SA_SIGINFO;

	if (sigaction(SIGINT, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGINT
		          << " (" << strsignal(SIGINT) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGILL, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGILL
		          << " (" << strsignal(SIGILL) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGTRAP, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGTRAP
		          << " (" << strsignal(SIGTRAP) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGSEGV
		          << " (" << strsignal(SIGSEGV) << ")\n";
		exit(EXIT_FAILURE);
	}

	if (sigaction(SIGFPE, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGFPE
		          << " (" << strsignal(SIGFPE) << ")\n";
		exit(EXIT_FAILURE);
	}
	if (sigaction(SIGABRT, &sigact, (struct sigaction *)NULL) != 0) {
		std::cerr << "error setting handler for signal " << SIGABRT
		          << " (" << strsignal(SIGABRT) << ")\n";
		exit(EXIT_FAILURE);
	}
}

}