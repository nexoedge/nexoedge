// SPDX-License-Identifier: Apache-2.0

#ifndef __AZURE_CONTAINER_HH__
#define __AZURE_CONTAINER_HH__

/**
 * Azure storage SDK blob size limit
 * default_single_blob_upload_threshold: 128MB
 * default_single_blob_download_threshold: 32MB
 **/

#include <string>

#include <was/storage_account.h>
#include <was/blob.h>

#include "container.hh"
#include "../../ds/chunk.hh"

class AzureContainer : public Container {
public:
    AzureContainer(int id, std::string bucketName, std::string storageConnectionString, unsigned long int capacity, std::string httpProxyIP = "", unsigned short httpProxyPort = 0);
    ~AzureContainer();

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
     * Generate blob path
     * 
     * @param[out] bpath      pre-allocated buffer to store the blob path
     * @param[in]  chunkName  name of the chunk
     *
     * @return whether the object path can be generated
     **/
    bool genBlobPath(char *bpath, std::string chunkName);

    /**
     * Get the size of bucket
     * 
     * @return bucket size
     **/
    unsigned long int getTotalSize(bool needsLock = true);

    /**
     * Compare the provided hash against MD5 checksum
     * 
     * @param[in] hash        hash in base64
     * @param[in] md5         MD5 of the chunk
     * @param[in] chunkName   name of the chunk
     *
     * @return whether the hash matches the MD5
     **/
    bool compareChecksum(const std::string &hash, const unsigned char *md5, const std::string &chunkName);

    /**
     * Copy the MD5 in hash
     *
     * @param[in] hash        hash in base64
     * @param[out] md5        buffer to store md5 checksum (binary)
     * @return whether the copying is successful
     **/
    bool copyChecksum(const std::string &hash, unsigned char *md5);

    /**
     * Check if chunk attributes match
     *
     * @param[in] chunk       chunk to check, should contain the fields to check (size and/or md5)
     * @param[in] forceChecksumCheck   whether to force checksum checking regardless of config
     * @param[in] checksumOnly         whether to only check if checksum matches
     * @return whether all fields to check match
     **/
    bool checkChunkAttributes(const Chunk &chunk, bool forceChecksumCheck = false, bool checksumOnly = false);

    azure::storage::cloud_storage_account _storageAccount;    /**< azure storage account */
    azure::storage::cloud_blob_client _blobClient;            /**< azure blob storage client */
    azure::storage::cloud_blob_container _blobContainer;      /**< azure blob container */

    azure::storage::operation_context _opCxt;                 /**< azure operation context for requests */
    azure::storage::blob_request_options _reqOpts;            /**< azure request options */
    azure::storage::access_condition _accessCond;             /**< azure (object) access conditions */
};

#endif // define __AZURE_CONTAINER_HH__
