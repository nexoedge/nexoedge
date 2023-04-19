// SPDX-License-Identifier: Apache-2.0

#ifndef __FINGERPRINT_SHA256_HH__
#define __FINGERPRINT_SHA256_HH__

#include "fingerprint.hh"
#include <openssl/sha.h>

class SHA256Fingerprint : public Fingerprint {
public:
    bool computeFingerprint(const unsigned char *data, unsigned int length) {
        unsigned char digest[SHA256_DIGEST_LENGTH];
        bool okay = SHA256(data, length, digest) == digest;
        if (okay) {
            _bytes = std::string((char *) digest, SHA256_DIGEST_LENGTH);
        }
        return okay;
    }
};

#endif // ifndef __FINGERPRINT_SHA256_HH__
