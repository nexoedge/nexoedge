// SPDX-License-Identifier: Apache-2.0

#ifndef __CHECKSUM_CALCULATOR_HH__
#define __CHECKSUM_CALCULATOR_HH__

#include <string>
#include <pthread.h>
#include <openssl/evp.h>
#include <openssl/objects.h>

#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string.hpp>

// alias new APIs for OpenSSL 1.1.0 below
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#define EVP_MD_CTX_new         EVP_MD_CTX_create
#define EVP_MD_CTX_free        EVP_MD_CTX_destroy
#endif // OPENSSL_VERSION_NUMBER < 0x10100000

class ChecksumCalculator {
public:
    ChecksumCalculator() {
        _mdctx = EVP_MD_CTX_new();
        _md = EVP_md_null();
        if (_mdctx == 0)
            throw std::bad_alloc();

        pthread_mutex_init(&_lock, NULL);
        EVP_DigestInit_ex(_mdctx, _md, NULL);
        _finalized = false;
    }

    ChecksumCalculator(const EVP_MD *md) : ChecksumCalculator() {
        _md = md;
        EVP_DigestInit_ex(_mdctx, _md, NULL);
    }

    ~ChecksumCalculator() {
        EVP_MD_CTX_free(_mdctx);
    }

    bool appendData(const unsigned char *data, const size_t length) {
        bool okay = false;

        // do not allow data append once finalized
        if (isFinalized())
            return okay;

        pthread_mutex_lock(&_lock);
        okay = EVP_DigestUpdate(_mdctx, data, length) == 1;
        pthread_mutex_unlock(&_lock);

        return okay;
    }

    bool finalize(unsigned char *digest, unsigned int &length) {
        bool okay = false;

        if (length < (unsigned int) getDigestSize())
            return okay;

        pthread_mutex_lock(&_lock);
        _finalized = EVP_DigestFinal_ex(_mdctx, digest, &length) == 1;
        okay = _finalized;
        pthread_mutex_unlock(&_lock);

        return okay;
    }

    std::string finalizeInHex() {
        std::string hash;
        unsigned int dlen = getDigestSize();
        unsigned char digest[dlen];

        if (finalize(digest, dlen)) {
            hash = toHex(digest, dlen);
        }

        return hash;
    }

    bool isFinalized() {
        return _finalized;
    }

    std::string getType() {
        return OBJ_nid2sn(EVP_MD_type(_md));
    }

    int getDigestSize() {
        return EVP_MD_size(_md);
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
    EVP_MD_CTX *_mdctx;
    const EVP_MD *_md;

    pthread_mutex_t _lock;
    bool _finalized;
};

class MD5Calculator : public ChecksumCalculator {
public:
    MD5Calculator() {
        _md = EVP_md5();
        EVP_DigestInit_ex(_mdctx, _md, NULL);
    }
};

class SHA256Calculator : public ChecksumCalculator {
public:
    SHA256Calculator() {
        _md = EVP_sha256();
        EVP_DigestInit_ex(_mdctx, _md, NULL);
    }
};

#endif //define __CHECKSUM_CALCULATOR_HH__
