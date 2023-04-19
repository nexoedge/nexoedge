// SPDX-License-Identifier: Apache-2.0

#ifndef __FINGERPRINT_HH__
#define __FINGERPRINT_HH__

#include <string>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <iomanip>
#include <iostream>

class Fingerprint {
public:
    Fingerprint() {
        reset();
    }

    ~Fingerprint() {}

    void reset() {
        set("", 0);
    }

    void set(const char *bytes, unsigned int length) {
        _bytes = std::string(bytes, length);
    }

    std::string get() const {
        return _bytes;
    }


    //use SHA256 to compute fingerprint for each block
    std::string sha256(const std::string& str);
    
    virtual bool computeFingerprint(const std::string& data, unsigned int length);

    bool operator!=(const Fingerprint &rhs) const {
        return _bytes != rhs._bytes;
    }

    bool operator==(const Fingerprint &rhs) const {
        return _bytes == rhs._bytes;
    }

    bool ifEqual(const Fingerprint &rhs) const {
        return _bytes == rhs._bytes;
    }

    bool operator<(const Fingerprint &rhs) const {
        return _bytes < rhs._bytes;
    }

    virtual std::string toHex() const {
        return toHex((unsigned char *) _bytes.data(), _bytes.size());
    }

    virtual bool unHex(const std::string &hex) {
        size_t length = hex.size() / 2 + (hex.size() % 2);
        unsigned char binary[length];
        bool okay = unHex(hex, binary, length);
        if (okay) {
            _bytes = std::string((char *) binary, length);
        }
        return okay;
    }

    static std::string toHex(const unsigned char *s, const unsigned int &len) {
        unsigned char hex[len * 2];
        std::string ret;
        try {
            boost::algorithm::hex(s, s + len, hex);
            ret = std::string(hex, hex + len * 2);
            boost::algorithm::to_lower(ret);
        } catch (std::exception &e) {
        }
        return ret;
    }

    static bool unHex(const std::string &hex, unsigned char *s, const unsigned int &len) {
        if (len < hex.size() / 2 + (hex.size() % 2))
            return false;
        try {
            boost::algorithm::unhex(hex.begin(), hex.end(), s);
        } catch (std::exception &e) {
            return false;
        }
        return true;
    }

protected:

    std::string _bytes;

};


#endif // define __FINGERPRINT_HH__
