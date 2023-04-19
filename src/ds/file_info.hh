// SPDX-License-Identifier: Apache-2.0

#ifndef __FILE_INFO_HH__
#define __FILE_INFO_HH__

#include <string>
#include <string.h>
#include <time.h>
#include <openssl/md5.h>
#include "version_info.hh"

#include "../common/define.hh"

class FileInfo {
public:
    char *name;                  /**< file name */
    int nameLength;              /**< length of file name */
    unsigned long int size;      /**< file size */
    time_t ctime;                /**< creation time */
    time_t atime;                /**< access time */
    time_t mtime;                /**< modification time */

    int numChunks;               /**< number of chunks */
    int version;                 /**< version number */
    int numVersions;             /**< number of versions, excluding the current version */
    VersionInfo *versions;       /**< information of the versions */

    unsigned char md5[MD5_DIGEST_LENGTH]; /**< file checksum (md5) */

    unsigned char  namespaceId;  /**< file namespace id */
    uint8_t status;              /**< file status in system */
    bool isDeleted;              /**< delete marker */

    std::string storageClass;    /**< storage class */

    FileInfo() {
        reset();
    }

    void reset() {
        name = 0;
        nameLength = 0;
        size = 0;
        namespaceId = INVALID_NAMESPACE_ID;
        ctime = 0;
        atime = 0;
        mtime = 0;
        status = FileStatus::NONE;
        memset(md5, 0, MD5_DIGEST_LENGTH);
        numChunks = 0;
        numVersions = 0;
        versions = 0;
        isDeleted = false;
    }

    ~FileInfo() {
        free(name);
        delete [] versions;
    }
};

#endif // define __FILE_INFO_HH__
