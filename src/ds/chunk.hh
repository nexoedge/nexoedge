// SPDX-License-Identifier: Apache-2.0

#ifndef __CHUNK_HH__
#define __CHUNK_HH__

#include <stdlib.h> // free()
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <openssl/md5.h>

#include "../common/define.hh"
#include "../common/checksum_calculator.hh"

struct Chunk {
    unsigned char  namespaceId;  /**< namespace id */
    boost::uuids::uuid fuuid;    /**< file uuid */
    int chunkId;                 /**< chunk id */
    unsigned char *data;         /**< chunk data */
    int size;                    /**< chunk size */
    bool freeData;               /**< whether to free data upon destruction */

    int fileVersion;             /**< file version number */
    char chunkVersion[CHUNK_VERSION_MAX_LEN];  /**< chunk version number for revert */

    unsigned char md5[MD5_DIGEST_LENGTH]; /**< chunk md5 checksum */

    Chunk() {
        reset();
    }

    ~Chunk() {
        release();
    }

    void copyMeta(const Chunk &src, bool copySize = true) {
        setId(src.namespaceId, src.fuuid, src.chunkId);
        fileVersion = src.fileVersion;
        strncpy(chunkVersion, src.chunkVersion, CHUNK_VERSION_MAX_LEN);
        copyMD5(src);
        if (copySize)
            size = src.size;
    }

    void setId(unsigned char  namespaceIdt, boost::uuids::uuid uuidt, int chunkIdt) {
        namespaceId = namespaceIdt;
        fuuid = uuidt;
        chunkId = chunkIdt;
    }

    void setChunkId(int chunkIdt) {
        chunkId = chunkIdt;
    }

    bool allocateData(int sizet, bool aligned = false) {
        // do not allocate data buffer if size is zero or less (invalid length)
        if (sizet <= 0) return false;

        // skip allocation if existing data buffer is allocated (not shadowed) and is of the requested size, and no buffer alignment is requested
        if (data != NULL && size == sizet && freeData && !aligned) return true;

        // try allocate the new data buffer
        unsigned char *datat = NULL;
        bool ret = 0;
        if (aligned) {
            ret = posix_memalign((void**) &datat, 32, sizet);
        } else {
            datat = (unsigned char *) malloc (sizet);
        }

        // report failure if out-of-memory
        if (datat == NULL || ret != 0) {
            free(datat);
            return false;
        }

        // free any existing data buffer
        if (freeData) free(data);

        data = datat;
        size = sizet;
        freeData = true;

        return true;
    }

    bool copy(const Chunk &src, bool aligned = false) {
        release();
        copyMeta(src);
        if (!allocateData(src.size, aligned)) {
            return false;
        }
        memcpy(data, src.data, size);
        return true;
    }

    bool move(Chunk &src) {
        release();
        copyMeta(src);
        data = src.data;
        size = src.size;
        freeData = src.freeData;
        src.data = 0;
        src.freeData = false;
        return true;
    }

    unsigned char  getNamespaceId() const {
        return namespaceId;
    }

    int getChunkId() const {
        return chunkId;
    }

    boost::uuids::uuid getFileUUID() const {
        return fuuid;
    }

    int getFileVersion() const {
        return fileVersion;
    }

    const char* getChunkVersion() const {
        return chunkVersion;
    }

    // int getChunkVersion() const {
    //     return fileVersion;
    // }

    std::string getChunkName() const {
        return std::to_string(namespaceId) + "_" + boost::uuids::to_string(fuuid) + "_" + std::to_string(fileVersion) + "_" + std::to_string(chunkId);
    }

    bool computeMD5() {
        if (size <= 0)
            return false;
        unsigned int hashLength = MD5_DIGEST_LENGTH;
        MD5Calculator cal;
        cal.appendData(data, size);
        return cal.finalize(md5, hashLength);
    }

    bool verifyMD5() {
        unsigned char curMD5[MD5_DIGEST_LENGTH];
        unsigned int hashLength = MD5_DIGEST_LENGTH;
        MD5Calculator cal;
        cal.appendData(data, size);
        cal.finalize(curMD5, hashLength);
        return memcmp(md5, curMD5, hashLength) == 0;
    }

    void copyMD5(const Chunk &src) {
        memcpy(md5, src.md5, MD5_DIGEST_LENGTH);
    }

    bool matchMeta(const Chunk &in) {
        return 
            chunkId == in.chunkId /* chunk id */
            && memcmp(md5, in.md5, MD5_DIGEST_LENGTH) /* checksum */
            && size == in.size /* size */
        ;
    }

    void resetMD5() {
        memset(md5, 0, MD5_DIGEST_LENGTH);
    }
    
    void reset() {
        namespaceId = INVALID_NAMESPACE_ID;
        fuuid = boost::uuids::nil_uuid();
        chunkId = INVALID_CHUNK_ID;
        fileVersion = 0;
        chunkVersion[0] = 0;
        data = 0;
        size = 0;
        freeData = true;
        resetMD5();
    }

    void release() {
        if (freeData) free(data);
        reset();
    }
};


#endif // define __CHUNK_HH__
