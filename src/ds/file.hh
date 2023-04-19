// SPDX-License-Identifier: Apache-2.0

#ifndef __FILE_HH__
#define __FILE_HH__

#include <map>
#include <vector>
#include <time.h>
#include <string.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sys/mman.h>
#include <openssl/md5.h>

#include "chunk.hh"
#include "../common/define.hh"
#include "coding_meta.hh"

#include "../proxy/dedup/block_location.hh"
#include "../proxy/dedup/fingerprint/fingerprint.hh"

#include "file_info.hh"

#define FILE_DNS_UUID            "c97ee2a4-ae65-4d67-88f3-1790084882f3"

class File {
public:
    File();
    ~File();

    void releaseData();

    // information setter
    bool setName(const char *fn, const int nlen);
    bool setUUID(std::string uuidstr);
    void setTimeStamps(time_t ctime, time_t mtime, time_t atime, time_t tctime = 0); 
    void setStagedInfo(unsigned long int fileSize, const CodingMeta cmeta, std::string sc, time_t stagedts);
    bool setVersion(int ver = 0);

    // copiers of individual part of information
    bool copyName(const File &in, bool shadow = false);
    bool copyName(const FileInfo &in, bool shadow = false);
    bool copyNameToInfo(FileInfo &out, bool shadow = false);
    void copySize(const File &in);
    void copyTimeStamps(const File &in);
    void copyFileChecksum(const File &in);
    void copyVersionControlInfo(const File &in);
    void copyStoragePolicy(const File &in);
    void copyChunkInfo(const File &in, bool shadow = false);
    void copyStagedInfo(const File &in);

    void copyOperationDataRange(const File &in);
    void copyOperationBenchmarkInfo(const File &in);

    // copiers of a combination of information
    bool copyNameAndSize(const File &in);
    bool copyAllMeta(File &in);

    // generator of information
    static boost::uuids::uuid genUUID(const char *name);
    void genUUID();

    bool initChunksAndContainerIds(int num = -1);

    void print();

    void resetStagingStoragePolicy();
    void reset();


    // metadata for storage

    // data object description
    unsigned char namespaceId;     /**< file namespace id */
    boost::uuids::uuid uuid;       /**< file uuid */
    char *name;                    /**< file name */
    int nameLength;                /**< length of file name */
    unsigned long int size;        /**< file size */
    int version;                   /**< file version number */
    time_t ctime;                  /**< creation time */
    time_t atime;                  /**< last access time */
    time_t mtime;                  /**< modification time */
    time_t tctime;                 /**< data check time */
    uint8_t status;                /**< file status in system */
    unsigned char md5[MD5_DIGEST_LENGTH]; /**< file checksum (md5) */
    bool isDeleted;                /**< whether the file is marked as deleted */

    int numStripes;                /**< number of stripes */

    // data location (chunks and containers)
    int numChunks;                 /**< number of chunks */
    int *containerIds;             /**< container ids */
    Chunk *chunks;                 /**< chunks */
    bool *chunksCorrupted;         /**< corrupted chunks */

    // storage policy
    CodingMeta codingMeta;         /**< coding metadata */
    std::string storageClass;      /**< name of storage class */

    // staging
    struct {
        unsigned long int size;    /**< file size */
        CodingMeta codingMeta;     /**< coding metadata */
        std::string storageClass;  /**< storage class */
        time_t mtime;              /**< modification time */
    } staged;


    // metadata for operations
    unsigned long int offset;      /**< file offset */
    unsigned long int length;      /**< file length */

    unsigned char *data;           /**< (raw) file data */
    bool mmapped;                  /**< whether the file data is mmapped */
    
    // for benchmark
    int blockId;                   /**< current no class Block, so manually add block id for identification */
    int stripeId;                  /**< currently no class Stripe, so manually add stripe id for swf stripe identification in chunkManager*/

    // for staging
    bool isFinalStripe;
    int reqId;                     // remark: use id instead of hash

    // for dedup
    std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> > uniqueBlocks; /*<< logical block to fingerprint and physcial location (in-stripe offset) */
    std::map<BlockLocation::InObjectLocation, Fingerprint> duplicateBlocks; /*<< logical block to fingerprint */
    std::vector<std::string> commitIds;

private:
};

#include "file_info.hh"
#include "version_info.hh"

#endif // define __FILE_HH__
