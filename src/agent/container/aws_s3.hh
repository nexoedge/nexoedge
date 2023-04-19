// SPDX-License-Identifier: Apache-2.0

#ifndef __AWS_CONTAINER_HH__
#define __AWS_CONTAINER_HH__

#include <atomic>
#include <string>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include "container.hh"
#include "../../ds/chunk.hh"

class AwsContainer : public Container {
public:
    AwsContainer(int id, std::string bucketName, std::string region, std::string keyId, std::string key, unsigned long int capacity, std::string endpoint = "", std::string httpProxyIP = "", unsigned short httpProxyPort = 0, bool useHttp = false);
    ~AwsContainer();

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
     * @param[out] total      total size of objects upon successful operation
     * @param[in] needsLock   lock checksum cache for update
     * @return whether the current total is obtained successful
     **/
    bool getTotalSize(unsigned long int &total, bool needsLock = true);

    /**
     * Compare the MD5 against the checksum in raw etag returned by AWS
     *
     * @param[in] etag        raw eTag from AWS (MD5 in hex)
     * @param[in] md5         md5 checksum of the chunk (in binary)
     * @param[in] chunkName   name of the chunk
     * @return whether the provided MD5 checksum matches that in the etag
     **/
    bool compareChecksum(const Aws::String &etag, const unsigned char *md5, const std::string &chunkName = "");

    /**
     * Copy the MD5 in etag
     *
     * @param[in] etag        raw eTag from AWS (MD5 in hex)
     * @param[out] md5        buffer to store md5 checksum (in binary)
     * @return whether the copying is successful
     **/
    bool copyChecksum(const Aws::String &etag, unsigned char *md5);


    Aws::Auth::AWSCredentials _cred;     /**< credentials for accessing aws services */
    Aws::S3::S3Client _client;           /**< aws client to reach aws service */
    Aws::String _bucketName;             /**< aws bucket name */
};

#endif // define __AWS_CONTAINER_HH__
