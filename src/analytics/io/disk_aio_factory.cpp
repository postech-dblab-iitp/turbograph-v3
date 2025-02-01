#include "analytics/datastructure/disk_aio_factory.hpp"
#include "analytics/core/turbo_callback.hpp"
#include <list>

#ifndef INVALID_SET_FILE_POINTER
#define INVALID_SET_FILE_POINTER ((DWORD)-1)
#endif

DiskAioFactory* DiskAioFactory::ptr = NULL;

/*
* Construct the aio factory
*/
DiskAioFactory::DiskAioFactory(int & res, int num_aio_disk_threads, int io_depth) {
	INVARIANT(ptr == NULL);
	daio = new diskaio::DiskAio(num_aio_disk_threads, io_depth);
	DiskAioFactory::ptr = this;
	daio->start();
	res = true;
}

/*
* Deconstruct the aio factory
*/
DiskAioFactory::~DiskAioFactory() {
	DiskAioFactory::ptr = NULL;
	#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
	{
		diskaio::DiskAioInterface*& interface = per_thread_aio_interface.get(omp_get_thread_num());
		delete interface;
		interface = NULL;
	}
	delete daio;
}

/*
* Open a file and register it to the aio file system 
*/
int DiskAioFactory::OpenAioFile(const char *file_path, int flag) {
	INVARIANT(daio != NULL);
	int file_id = daio->OpenAioFile(file_path, flag);
	if(file_id < 0) {
        perror("OpenAioFile");
    }
	INVARIANT(file_id >= 0);
    return file_id;
}

/*
* Remove the registered file
*/
void DiskAioFactory::RemoveAioFile(int file_id) {

}

/*
* Close the given file
*/
void DiskAioFactory::CloseAioFile(int file_id, bool rm) {
    ALWAYS_ASSERT(file_id >= 0);
	daio->CloseAioFile(file_id);
	if (rm)
		RemoveAioFile(file_id);
}

/*
* Get the size of the file
*/
std::size_t DiskAioFactory::GetAioFileSize(int file_id) {
    ALWAYS_ASSERT(file_id >= 0);
	size_t fsize = lseek(daio->Getfd(file_id), 0, SEEK_END);
	return fsize;
}

/*
* Get the file descriptor of the file
*/
int DiskAioFactory::Getfd(int file_id) {
    ALWAYS_ASSERT(file_id >= 0);
    return daio->Getfd(file_id);
}

/*
* Create an aio interface of the aio file system
*/
diskaio::DiskAioInterface* DiskAioFactory::CreateAioInterface(int max_num_ongoing, int tid) {
	return daio->CreateAioInterface(max_num_ongoing, tid);
}

/*
* Create aio interfaces for omp threads of the aio file system
*/
void DiskAioFactory::CreateAioInterfaces(int max_num_ongoing, int num_disk_aio_threads) {
	#pragma omp parallel num_threads(UserArguments::NUM_THREADS)
	{
        int tid = omp_get_thread_num();
        int aio_tid = tid % num_disk_aio_threads;
		ALWAYS_ASSERT (per_thread_aio_interface.get(omp_get_thread_num()) == NULL);
		per_thread_aio_interface.get(omp_get_thread_num()) = daio->CreateAioInterface(max_num_ongoing, aio_tid);
		INVARIANT(per_thread_aio_interface.get(omp_get_thread_num()) != NULL);
	}
}

/*
* Get the aio interface of the given omp thread
*/
diskaio::DiskAioInterface* DiskAioFactory::GetAioInterface(int tid) {
	diskaio::DiskAioInterface* my_io = per_thread_aio_interface.get(omp_get_thread_num());
    INVARIANT(my_io);
    return my_io;
}

/*
* Request an asynchronous read request
*/
int DiskAioFactory::ARead(AioRequest& req, diskaio::DiskAioInterface* my_io) {
	if (!my_io)
		my_io = per_thread_aio_interface.get(omp_get_thread_num());
	ALWAYS_ASSERT(my_io != NULL);
	
	/*
	* Before requesting the read request, wait and process the reads previously requested
	*/
    int backoff = 128;
	while (my_io->GetNumOngoing() >= PER_THREAD_MAXIMUM_ONGOING_DISK_AIO / 2) {
		int completed = my_io->WaitForResponses(0);
		if (completed > 0) break;
		usleep (backoff);
		if (backoff <= 256 * 1024) backoff *= 2;
	}

	my_io->Request(req.user_info.db_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_READ, req.user_info);
	return true;
}

/*
* Request an asynchronous write request
*/
int DiskAioFactory::AWrite(AioRequest& req, diskaio::DiskAioInterface* my_io) {
	if (!my_io)
		my_io = per_thread_aio_interface.get(omp_get_thread_num());
	ALWAYS_ASSERT(my_io != NULL);

	/*
	* Before requesting the write request, wait and process the reads previously requested
	*/	
    int backoff = 128;
	while (my_io->GetNumOngoing() >= PER_THREAD_MAXIMUM_ONGOING_DISK_AIO / 2) {
		int completed = my_io->WaitForResponses(0);
		if (completed > 0) break;
		usleep (backoff);
		if (backoff <= 256 * 1024) backoff *= 2;
	}

	my_io->Request(req.user_info.db_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_WRITE, req.user_info);
	return true;
}

/*
* Request an asynchronous append request
*/
int DiskAioFactory::AAppend(AioRequest& req, diskaio::DiskAioInterface* my_io) {
	if (!my_io)
		my_io = per_thread_aio_interface.get(omp_get_thread_num());
	ALWAYS_ASSERT(my_io != NULL);

	my_io->Request(req.user_info.db_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_APPEND, req.user_info);
	/*
	* Wait and process the reads previously requested
	*/
	int backoff = 128;
	while (my_io->GetNumOngoing() >= PER_THREAD_MAXIMUM_ONGOING_DISK_AIO) {
		int completed = my_io->WaitForResponses(0);
		if (completed > 0) break;
		usleep (backoff);
		if (backoff <= 256 * 1024) backoff *= 2;
	}
	return true;
}

int DiskAioFactory::WaitForAllResponses(diskaio::DiskAioInterface** my_io) {
	ALWAYS_ASSERT(my_io != NULL);

    int num_total_to_complete = 0;
    //turbo_timer tim;
    //fprintf(stdout, "WaitForAllResponses GetNumOngoing %ld, %ld\n", my_io[READ_IO]->GetNumOngoing(), my_io[WRITE_IO]->GetNumOngoing());
    std::list<IOMode> io_modes = { WRITE_IO, READ_IO };
    do {
        for (auto& io_mode: io_modes) {
            //if (io_mode == READ_IO) tim.start_timer(0);
            //else tim.start_timer(1);
            int num_to_complete = my_io[io_mode]->GetNumOngoing();
            int backoff = 1;
            while (my_io[io_mode]->GetNumOngoing() > 0) {
                usleep (backoff * 1024);
                my_io[io_mode]->WaitForResponses(0);
                if (backoff <= 16 * 1024) backoff *= 2;
            }
            num_total_to_complete += num_to_complete;
            //if (io_mode == READ_IO) tim.stop_timer(0);
            //else tim.stop_timer(1);
        }
    } while (my_io[READ_IO]->GetNumOngoing() > 0 || my_io[WRITE_IO]->GetNumOngoing() > 0);

    INVARIANT (my_io[READ_IO]->GetNumOngoing() == 0);
    INVARIANT (my_io[WRITE_IO]->GetNumOngoing() == 0);
    //fprintf(stdout, "WaitForAllResponses GetNumOngoing %.3f, %.3f\n", tim.get_timer(0), tim.get_timer(1));
	return num_total_to_complete;
}

/*
* Get the number of ongoing aio requests in the aio file system
*/
int DiskAioFactory::GetNumOngoing(diskaio::DiskAioInterface** my_io) {
	int tid = omp_get_thread_num();
	ALWAYS_ASSERT (tid >= 0 && tid < UserArguments::NUM_THREADS);
	ALWAYS_ASSERT (my_io != NULL);
    int num_ongoing = my_io[READ_IO]->GetNumOngoing() + my_io[WRITE_IO]->GetNumOngoing();
	return num_ongoing;
}

/*
* Return the counters for statistics (e.g., number of read/write requests)
*/
diskaio::DiskAioStats DiskAioFactory::GetStats() {
	return daio->GetStats();
}

/*
* Retset the counters for statistics (e.g., number of read/write requests)
*/
void DiskAioFactory::ResetStats() {
	daio->ResetStats();
}

