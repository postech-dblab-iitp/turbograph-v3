#pragma once

#include <vector>
#include <numeric>
#include <algorithm>

namespace duckdb {

class ResizableBoolVector : public std::vector<bool> {
public:
    // Constructor forwarding to std::vector<bool>
    using std::vector<bool>::vector; 

    // Override the operator[] to provide resizing functionality
    void set(size_t index, bool value) {
        if (index >= this->size()) {
            this->resize(index * 2, false);
        }
        std::vector<bool>::operator[](index) = value;
    }

    // Reset method to set all elements to false
    void reset() {
        std::fill(this->begin(), this->end(), false);
    }
};

}