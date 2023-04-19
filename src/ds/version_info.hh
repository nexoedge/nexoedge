// SPDX-License-Identifier: Apache-2.0

#ifndef __VERSION_INFO_HH__
#define __VERSION_INFO_HH__

#include <time.h>
#include <openssl/md5.h>

struct VersionInfo {
    int version;                 /**< version number */
    int numChunks;               /**< number of chunks */
    unsigned long int size;      /**< file size */
    time_t mtime;                /**< modification time */
    unsigned char md5[MD5_DIGEST_LENGTH]; /**< file checksum (md5) */
    bool isDeleted;              /**< delete marker */

    VersionInfo() {
        reset();
    }

    ~VersionInfo() {
    }

    void reset() {
        size = 0;
        mtime = 0;
        memset(md5, 0, MD5_DIGEST_LENGTH);
        isDeleted = false;
        numChunks = 0;
    }
};

#endif // define __VERSION_INFO_HH__
