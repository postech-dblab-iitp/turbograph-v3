
/*
 * Design of the TG_Vector
 *
 * This class has been implemented to be used for debugging purposes. When 
 * accessing the stl vector data structure, it is used to prevent index out 
 * of range error.
 */

#include "analytics/core/Global.hpp"
#include <vector>

template <typename T>
class TG_Vector: public std::vector<T> {

    public:

        template <class... Args>
        TG_Vector(Args&&... args) : std::vector<T>(std::forward<Args>(args)...) {}

        /* 
        * Get the value at the given position
        * This is the vector for debugging
        */
        inline T& operator[](int64_t pos) {
            if (pos < 0 || pos >= std::vector<T>::size()) {
                fprintf(stdout, "[%ld](%ld) %ld not in [%ld, %ld]\n", UserArguments::tmp, (int64_t) pthread_self(), pos, 0, std::vector<T>::size());
                LOG_ASSERT(false);
                abort();
            }
            //return std::vector<T>::at(pos);
            //return (*static_cast<std::vector<T>*>( this ))[pos];
            return std::vector<T>::at(pos);
        }

};

