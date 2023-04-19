// SPDX-License-Identifier: Apache-2.0

#ifndef __ALI_CONTAINER_HH__
#define __ALI_CONTAINER_HH__

#include <string>

extern "C" {
#include <oss_c_sdk/oss_api.h>
#include <oss_c_sdk/aos_define.h>
}

#include "container.hh"
#include "../../ds/chunk.hh"

class AliContainer : public Container {
public:
    AliContainer(int id, std::string bucketName, std::string region, std::string keyId, std::string key, unsigned long int capacity);
    ~AliContainer();

    /**
     * See Container::putChunk()
     **/
    bool putChunk(Chunk &chunk);

    /**
     * See Container::getChunk()
     **/
    bool getChunk(Chunk &chunk, bool skipVerification = false);

    /**
     * See Container::deleteChunk()
     **/
    bool deleteChunk(const Chunk &chunk);

    /**
     * See Container::copyChunk()
     **/
    bool copyChunk(const Chunk &src, Chunk &dst);

    /**
     * See Container::moveChunk()
     **/
    bool moveChunk(const Chunk &src, Chunk &dst);

    /**
     * See Container::hasChunk()
     **/
    bool hasChunk(const Chunk &chunk);

    /**
     * See Container::revertChunk()
     **/
    bool revertChunk(const Chunk &chunk);

    /**
     * See Container::verifyChunk()
     **/
    bool verifyChunk(const Chunk &chunk);

    /**
     * See Container::updateUsage()
     **/
    void updateUsage();

private:
    void initOptions(oss_request_options_t *&options, aos_pool_t *&pool);

    /**
     * Generate object path
     * 
     * @param[out] opath      pre-allocated buffer to store the object path
     * @param[in]  chunkName  name of the chunk
     *
     * @return whether the object path can be generated
     **/
    bool genObjectPath(char *opath, std::string chunkName);

    /**
     * Get the size of bucket
     * 
     * @param[in] total       bucket size, if successful
     * @return whether the current size of bucket is obtained successfully
     **/
    bool getTotalSize(unsigned long int &total);

    /**
     * Compare the hash against the proivided  MD5
     * 
     * @param[in] hash         hash in base 64
     * @param[in] md5          MD5 of the chunk
     * @param[in] chunkName    name of the chunk
     * @return whether the hash matches the provided MD5
     **/
    bool compareChecksum(const char *hash, const unsigned char *md5, const std::string &chunkName);

    bool compareChecksumInEtag(const std::string &hash, const unsigned char *md5, const std::string &chunkName);

    /**
     * Copy MD5 in the hash 
     * 
     * @param[in] hash         hash in base64
     * @param[out] md5         buffer to store MD5
     * @return whether the copying is successful
     **/
    bool copyChecksum(const char *hash, unsigned char *md5);

    /**
     * Head the chunk and check certain fields
     * 
     * @param[in] chunk        chunk to check, should contain fields to check against (size and/or checksum)
     * @param[in] forceChecksumCheck   whether to force checksum check regardless of config
     * @param[in] checksumOnly whether to check only if checksum matches
     * @return whether all fields to check are matched
     **/
    bool checkChunk(const Chunk &chunk, bool forceChecksumCheck = false, bool checksumOnly = false);

    std::string _bucketName;       /**< bucket name */
    std::string _endpoint;         /**< url of endpoint for storage api */
    std::string _keyId;            /**< storage key id */
    std::string _key;              /**< storage key */
};

#endif // define __ALI_CONTAINER_HH__
