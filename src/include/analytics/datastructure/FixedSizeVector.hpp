#include "analytics/core/Global.hpp"

/*
 * Design of the FixedSizeVector
 */

template <typename T>
class FixedSizeVector {
    public:
        FixedSizeVector() : data_(NULL), capacity_(0), size_(0) {}

        FixedSizeVector(T* d, int64_t c, int64_t s = 0) : data_(d), capacity_(c), size_(s) {
            ALWAYS_ASSERT(c >= s);
        }
        
        FixedSizeVector(SimpleContainer cont) : data_((T*) cont.data), capacity_(cont.capacity / sizeof(T)), size_(cont.size_used / sizeof(T)), cont_(cont) {
            ALWAYS_ASSERT(cont.size_used % sizeof(T) == 0);
        }
       

        static int64_t recursive_size_in_bytes(int64_t c) {
            return (sizeof(T) * c);
        }
        
        template <typename... Args>
        static int64_t recursive_size_in_bytes(int64_t c, Args... args) {
            int64_t sum = sizeof(T) * c;
            return sum + c * T::recursive_size_in_bytes(args...);
        }

        template <typename PTR>
        void recursive_resize(PTR* d, int64_t c) {
            ALWAYS_ASSERT(d != NULL);
            
            data_ = (T*) d;
            capacity_ = c;
            size_ = c;
        }
        
        template <typename PTR, typename... Args>
        void recursive_resize(PTR* d, int64_t c, Args... args) {
            ALWAYS_ASSERT(d != NULL);
            
            data_ = (T*) d;
            capacity_ = c;
            size_ = c;

            char* tmp = (char*) (data_ + capacity_);
            for (int64_t idx = 0; idx < capacity_; idx++) {
                data_[idx].recursive_resize(tmp, args...);
                tmp += T::recursive_size_in_bytes(args...);
            }
        }
        
        inline T& operator[](int64_t pos) {
            ALWAYS_ASSERT(data_ != NULL);
            
#ifndef PERFORMANCE
            if (pos < 0 || pos >= size_) {
                fprintf(stdout, "[%ld](%ld) %ld not in [%ld, %ld]\n", UserArguments::tmp, (int64_t) pthread_self(), pos, 0, size());
                LOG_ASSERT(false);
                abort();
            }
#endif
            return data_[pos];
        }

        void clear() {
            size_ = 0;
        }

        void push_back(T& elem) {
            ALWAYS_ASSERT(size_ < capacity_);
            data_[size_++] = elem;
        }

        T front() {
            ALWAYS_ASSERT(size_ > 0);
            return data_[0];
        }

        T back() {
            ALWAYS_ASSERT(size_ > 0);
            return data_[size_ - 1];
        }

        bool full() {
            return (size_ == capacity_); 
        }

        bool empty() {
            return (size_ == 0); 
        }
        
        void set(T val) {
            ALWAYS_ASSERT(data_ != NULL);
            ALWAYS_ASSERT(capacity_ >= size_);
            ALWAYS_ASSERT(size_ > 0);
            for (int64_t idx = 0; idx < size_; idx++) {
                data_[idx] = val;
            }
        }

        void copy_contents_from(FixedSizeVector<T>& o) {
            ALWAYS_ASSERT(o.size_ < capacity_);
            size_ = o.size_;
            for (int64_t idx = 0; idx < size_; idx++) {
                data_[idx] = o[idx];
            }
        }

        T* data() { return data_; }
        int64_t capacity() { return capacity_; }
        int64_t size() { return size_; }

        void set_container(SimpleContainer cont) { cont_ = cont; }
        SimpleContainer get_container() { return cont_; }

    private:
        T* data_;
        int64_t capacity_;
        int64_t size_;
        SimpleContainer cont_;
};
