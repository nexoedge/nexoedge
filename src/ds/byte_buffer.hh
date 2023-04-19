// SPDX-License-Identifier: Apache-2.0

#ifndef __BYTE_BUFFER_HH__
#define __BYTE_BUFFER_HH__

#include <stdint.h>
#include "../common/define.hh"
#include <iostream>

class ByteBuffer {
public:

    ByteBuffer() {
        reset();
    }

    ByteBuffer(bool aligned) {
        reset();
        if (aligned) setAligned();
    }

    ByteBuffer(length_t size, bool aligned) : ByteBuffer(aligned) {
        allocate(size);
    }

    ~ByteBuffer() {
        release();
    }

    // copy constructor
    ByteBuffer(const ByteBuffer &src) {
        copy(src, /* deep copy */ true);
    } 

    // move assignment operator
    ByteBuffer& operator=(ByteBuffer &&src) {
        // skip if moving itself
        if (this == &src) {
            return *this;
        }
        // take over all values
        _data = src._data;
        _size = src._size;
        _aligned = src._aligned;
        _copied = src._copied;
        // reset the source
        src.reset();
        return *this;
    }

    bool setAligned() {
        if (_data != NULL) {
            return false;
        }
        _aligned = true;
        return true;
    }

    bool setUnaligned() {
        if (_data != NULL) {
            return false;
        }
        _aligned = false;
    }

    bool copy(const ByteBuffer &src, bool deepCopy = true) {
        return copyData(src, deepCopy);
    }

    bool copySize(const ByteBuffer &src) {
        if (_data != NULL) { return false; }
        _size = src._size;
        return true;
    }

    bool copyData(const ByteBuffer &src, bool deepCopy = true) {
        if (_data != NULL) { return false; }
        copySize(src);
        if (deepCopy) {
            _aligned = src._aligned;
            if (!allocate(src._size)) {
                reset();
                return false;
            }
            memcpy(_data, src._data, src._size);
        } else {
            _data = src._data;
            _copied = true;
        }
        return true;
    }

    bool setSize(length_t size) {
        if (_data != NULL) { return false; }
        _size = size;
        return true;
    }

    bool allocate(length_t size, bool aligned = false) {
        bool zeroSized = size == 0;

        // try allocating the new buffer
        uint8_t *tmpData = 0;
        try {
            if (aligned) {
                if (posix_memalign((void **) &tmpData, 32, size + (zeroSized? 1 : 0)) != 0)
                    throw std::bad_alloc();
            } else {
                tmpData = new uint8_t[size + (zeroSized? 1 : 0)];
            }
        } catch (std::bad_alloc &e) {
            std::cerr << "Failed to allocate aligned=" << aligned << " data of size " << size;
            return false;
        }

        // release the old buffer
        release();
        // set the new buffer and size
        _data = tmpData;
        _size = size;
        _aligned = aligned;

        return true;
    }

    void release(bool freeData = true) {
        if (!_copied) {
            if (_aligned) {
                free(_data);
            } else {
                delete [] _data;
            }
        }
        reset();
    }

    // return the internal data buffer for modification
    data_t *data() {
        return _data;
    }
    
    // return the internal data buffer but do not allow modification
    const data_t *data() const {
        return _data;
    }
    
    length_t size() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    bool allocated() const {
        return _data != NULL;
    }

    bool aligned() const {
        return _aligned;
    }
    
    void reset() {
        _data = NULL;
        _size = 0;
        _aligned = false;
        _copied = false;
    }


private:
    data_t *_data;
    length_t _size;

    bool _aligned;
    bool _copied;
};

#endif // define __BYTE_BUFFER_HH__

