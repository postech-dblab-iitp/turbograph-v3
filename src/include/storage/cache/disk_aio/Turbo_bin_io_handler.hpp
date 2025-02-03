#ifndef _TURBO_BIN_IO_HANDLER_H
#define _TURBO_BIN_IO_HANDLER_H

/*
 * Design of the Turbo_bin_io_handler
 *
 * This class provides APIs that can perform I/O to binary files. After setting 
 * the flag value according to the combination of parameters given as the input 
 * of the Open() function, you can open the binary file in the given path and 
 * perform I/O on it. 
 *
 * Another special parameter is o_direct. By setting the parameter to true, the 
 * file is opened in O_DIRECT mode so that the system can directly manage I/O
 * performance without depending on the OS.
 *
 * We use pread() and pwrite() to perform I/O. Reading and writing too much data 
 * at once when performing pread() and pwrite() may not work as we intended. 
 * Accordingly, when performing Read(), Write(), and Append() functions, we use 
 * a loop to perform I/O multiple times with a size of MAX_IO_SIZE_PER_RW.
 */

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <iterator>
#include <streambuf>

#include "storage/cache/common.h"
#include "common/enums/return_status.hpp"

class Turbo_bin_io_handler {
   public:
    Turbo_bin_io_handler() : file_descriptor(-1) { file_mmap = NULL; }

    Turbo_bin_io_handler(const char *file_name,
                         bool create_if_not_exist = false,
                         bool write_enabled = false,
                         bool delete_if_exist = false, bool o_direct = false)
        : file_descriptor(-1)
    {
        OpenFile(file_name, create_if_not_exist, write_enabled, delete_if_exist,
                 o_direct);
        file_mmap = NULL;
    }

    ~Turbo_bin_io_handler()
    {
        if (file_descriptor != -1) {
            int res = close(file_descriptor);
            assert(res == 0);
            file_descriptor = -1;
        }
    }

    void Close(bool rm = false)
    {
        if (file_descriptor != -1) {
            close(file_descriptor);
            file_descriptor = -1;
        }
        if (rm && check_file_existence(file_path)) {
            assert(remove(file_path.c_str()) == 0);
        }
    }

    void Truncate(int64_t length)
    {
        assert(file_descriptor != -1);
        int res = ftruncate64(file_descriptor, length);
        assert(res == 0);
        assert(file_size() == length);
    }

    ReturnStatus OpenFile(const char *file_name,
                          bool create_if_not_exist = false,
                          bool write_enabled = false,
                          bool delete_if_exist = false, bool o_direct = false)
    {
        mode_t old_umask;
        old_umask = umask(0);
        if (delete_if_exist) {
            remove(file_name);
        }

        if (create_if_not_exist && write_enabled && o_direct)
            file_descriptor =
                open(file_name, O_RDWR | O_CREAT | O_DIRECT, 0666);
        else if (create_if_not_exist && write_enabled)
            file_descriptor = open(file_name, O_RDWR | O_CREAT, 0666);
        else if (write_enabled && o_direct)
            file_descriptor = open(file_name, O_RDWR | O_DIRECT, 0666);
        else if (write_enabled)
            file_descriptor = open(file_name, O_RDWR, 0666);
        else
            file_descriptor = open(file_name, O_RDONLY, 0666);
        umask(old_umask);

        if (file_descriptor == -1) {
            fprintf(stdout, "Fail to open file %s\n", file_name);
        }
        assert(file_descriptor != -1);

        off64_t f = lseek64(file_descriptor, 0, SEEK_END);
        file_size_ = f;
        assert(f != -1);
        file_path = std::string(file_name);
        return ReturnStatus::OK;
    }

    ReturnStatus Append(std::int64_t size_to_append, char *data)
    {
        off64_t err = lseek64(file_descriptor, 0, SEEK_END);
        assert(err != -1);

        while (size_to_append > MAX_IO_SIZE_PER_RW) {
            err = write(file_descriptor, (void *)data, MAX_IO_SIZE_PER_RW);
            assert(err == MAX_IO_SIZE_PER_RW);
            assert(err != -1);
            err = lseek64(file_descriptor, 0, SEEK_END);
            assert(err != -1);

            size_to_append -= MAX_IO_SIZE_PER_RW;
            data = &data[MAX_IO_SIZE_PER_RW];
        }
        err = write(file_descriptor, (void *)data, size_to_append);
        assert(err == size_to_append);
        assert(err != -1);
        return ReturnStatus::OK;
    }

    ReturnStatus Read(int64_t offset_to_read, int64_t size_to_read, char *data)
    {
        assert(offset_to_read + size_to_read <= file_size());
        int64_t size_read = 0;
        int64_t tmp_size_to_read = size_to_read;
        while (size_to_read != 0) {
            assert(size_to_read >= 0);
            assert(offset_to_read + size_to_read <= file_size());

            int64_t tmp = pread(file_descriptor, (void *)data,
                                std::min(size_to_read, MAX_IO_SIZE_PER_RW),
                                offset_to_read);
            assert(tmp >= 0);

            size_read += tmp;
            size_to_read -= tmp;
            offset_to_read += tmp;
            data = &data[tmp];
        }
        assert(tmp_size_to_read == size_read);
        return ReturnStatus::OK;
    }

    ReturnStatus Write(int64_t offset_to_write, int64_t size_to_write,
                       char *data)
    {
        int64_t size_written = 0;
        int64_t tmp_size_to_write = size_to_write;
        while (size_to_write != 0) {
            assert(size_to_write >= 0);

            int64_t tmp = pwrite(file_descriptor, (void *)data,
                                 std::min(size_to_write, MAX_IO_SIZE_PER_RW),
                                 offset_to_write);
            assert(tmp >= 0);

            size_written += tmp;
            size_to_write -= tmp;
            offset_to_write += tmp;
            data = &data[tmp];
        }
        assert(tmp_size_to_write == size_written);
        return ReturnStatus::OK;
    }

    char *CreateMmap(bool write_enabled)
    {
        assert(file_mmap == NULL);
        assert(file_descriptor != -1);
        int open_flag, mmap_prot, mmap_flag;
        if (write_enabled) {
            open_flag = O_RDWR;
            mmap_prot = PROT_READ | PROT_WRITE;
            mmap_flag = MAP_SHARED;
        }
        else {
            open_flag = O_RDONLY;
            mmap_prot = PROT_READ;
            mmap_flag = MAP_SHARED;
        }
        int64_t file_size__ = lseek64(file_descriptor, 0, SEEK_END);
        if (file_size__ == 0)
            return NULL;

        assert(file_size__ > 0);
        file_mmap = (char *)mmap64(NULL, file_size__, mmap_prot, mmap_flag,
                                   file_descriptor, 0);
        assert(file_mmap != MAP_FAILED);

        return file_mmap;
    }

    void DestructMmap()
    {
        assert(file_descriptor != -1);
        int64_t file_size__ = lseek64(file_descriptor, 0, SEEK_END);
        assert(file_size__ >= 0);
        assert(munmap(file_mmap, file_size__) == 0);
        file_mmap = NULL;
    }

    int64_t file_size()
    {
        assert(file_descriptor != -1);
        off64_t file_size__ = lseek64(file_descriptor, 0, SEEK_END);
        assert(file_size__ >= 0);
        return file_size__;
    }

    int fdval() { return file_descriptor; }

    std::string get_file_path() { return file_path; }

   private:
    int file_descriptor;
    char *file_mmap;
    std::string file_path;
    int64_t file_size_;
};

#endif