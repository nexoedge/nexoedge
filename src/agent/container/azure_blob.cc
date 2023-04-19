// SPDX-License-Identifier: Apache-2.0

#include <cpprest/streams.h>
#include <cpprest/rawptrstream.h>
#include <glog/logging.h>

#include "../../common/config.hh"
#include "azure_blob.hh"

#define BPATH_MAX (128)

AzureContainer::AzureContainer(int id, std::string bucketName, std::string storageConnectionString, unsigned long int capacity, std::string httpProxyIP, unsigned short httpProxyPort) :
        Container(id, capacity) {
    // storage account
    _storageAccount = azure::storage::cloud_storage_account::parse(storageConnectionString);

    // operation context
    _opCxt = azure::storage::operation_context();
    if (!httpProxyIP.empty()) {
        _opCxt.set_proxy(web::web_proxy(std::string("//").append(httpProxyIP).append(":").append(std::to_string(httpProxyPort))));
    }

    // request options
    _reqOpts = azure::storage::blob_request_options();

    // access condition
    _accessCond = azure::storage::access_condition();

    // Create a blob container
    _blobClient = _storageAccount.create_cloud_blob_client();
    _blobContainer = _blobClient.get_container_reference(_XPLATSTR(bucketName));
   
    // Return value is true if the container did not exist and was successfully created.
    _blobContainer.create_if_not_exists(azure::storage::blob_container_public_access_type(), _reqOpts, _opCxt);

    updateUsage();
}

AzureContainer::~AzureContainer() {
}

bool AzureContainer::genBlobPath(char *bpath, std::string chunkName) {
    return sprintf(bpath, "%s", chunkName.c_str()) < BPATH_MAX;
}

bool AzureContainer::compareChecksum(const std::string &hash, const unsigned char *md5, const std::string &chunkName) {
    std::string md5base64 = utility::conversions::to_base64(std::vector<unsigned char> (md5, md5 + MD5_DIGEST_LENGTH));
    bool matched = hash == md5base64;
    LOG_IF(INFO, !matched) << "Chunk " << chunkName << " checksum mismatched (" << hash << " vs " << md5base64 << ")";
    return matched;
}

bool AzureContainer::copyChecksum(const std::string &hash, unsigned char *md5) {
    std::vector<unsigned char> binary = utility::conversions::from_base64(hash);

    if (md5 == NULL || binary.size() != MD5_DIGEST_LENGTH)
        return false;

    for (int i = 0; i < MD5_DIGEST_LENGTH; i++)
        md5[i] = binary[i];

    return true;
}

bool AzureContainer::putChunk(Chunk &chunk) {
    std::string chunkName = chunk.getChunkName();

    char bpath[BPATH_MAX];
    if (genBlobPath(bpath, chunkName) == false) {
        LOG(ERROR) << "Failed to get blob path name";
        return false;
    }

    // fit the data into a blob stream
    Concurrency::streams::istream cdata(Concurrency::streams::rawptr_buffer<char> ((char *) chunk.data, chunk.size, std::ios::in));
    azure::storage::cloud_block_blob chunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath));

    azure::storage::cloud_metadata meta = azure::storage::cloud_metadata();

    azure::storage::cloud_block_blob oldBlob;
    // create a snapshot
    try {
        oldBlob = chunkBlob.create_snapshot(meta, _accessCond, _reqOpts, _opCxt);
    } catch (azure::storage::storage_exception &e) {
        LOG(ERROR) << "Failed to create snapshot before put " << e.what();
    }

    // upload the blob
    chunkBlob.upload_from_stream(cdata, _accessCond, _reqOpts, _opCxt);

    cdata.close().wait();

    // record the snapshot time for 
    snprintf(chunk.chunkVersion, CHUNK_VERSION_MAX_LEN, "%s", oldBlob.snapshot_time().c_str());

    bool success = true;
    std::string hash = chunkBlob.properties().content_md5();
    // verify checksum
    if (success && Config::getInstance().verifyChunkChecksum()) {
        success = compareChecksum(hash, chunk.md5, chunkName);
    }
    if (success) {
        // copy checksum from response
        copyChecksum(hash, chunk.md5);
        LOG(INFO) << "Put chunk " << chunkName << " as blob " << bpath << " snapshot version = " << chunk.chunkVersion;
    }
    return success;
}

bool AzureContainer::getChunk(Chunk &chunk, bool skipVerification) {
    char bpath[BPATH_MAX];
    if (genBlobPath(bpath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get blob path name";
        return false;
    }
    
    // get the chunk size
    azure::storage::cloud_block_blob chunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath));
    chunkBlob.download_attributes(_accessCond, _reqOpts, _opCxt);
    chunk.size = chunkBlob.properties().size();

    // get the chunk data
    chunk.data = (unsigned char*) malloc (chunk.size * sizeof(unsigned char));
    Concurrency::streams::ostream cdata(Concurrency::streams::rawptr_buffer<char> ((char *) chunk.data, chunk.size));
    concurrency::streams::ostream outStream(cdata);
    chunkBlob.download_to_stream(outStream, _accessCond, _reqOpts, _opCxt);

    // verify checksum
    if (!skipVerification && Config::getInstance().verifyChunkChecksum() && !chunk.verifyMD5())
        return false;

    LOG(INFO) << "Get chunk " << chunk.getChunkName() << " as blob " << bpath;
    return true;
}

bool AzureContainer::deleteChunk(const Chunk &chunk) {
    char bpath[BPATH_MAX];
    if (genBlobPath(bpath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get blob path name";
        return false;
    }

    // delete the blob
    azure::storage::cloud_block_blob chunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath));
    chunkBlob.delete_blob(
        azure::storage::delete_snapshots_option::include_snapshots,
        _accessCond,
        _reqOpts,
        _opCxt
    );

    LOG(INFO) << "Delete chunk " << chunk.getChunkName() << " as blob " << bpath;
    return true;
}

bool AzureContainer::copyChunk(const Chunk &src, Chunk &dst) {
    char srcpath[BPATH_MAX], dstpath[BPATH_MAX];
    if (genBlobPath(srcpath, src.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get source blob path name";
        return false;
    }
    if (genBlobPath(dstpath, dst.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get destination blob path name";
        return false;
    }

    // get source and dest blob
    azure::storage::cloud_block_blob srcChunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(srcpath));
    azure::storage::cloud_block_blob copyChunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(dstpath));
    // copy and get the copied size
    copyChunkBlob.start_copy(srcChunkBlob, _accessCond, _accessCond, _reqOpts, _opCxt);
    copyChunkBlob.download_attributes(_accessCond, _reqOpts, _opCxt);
    dst.size = copyChunkBlob.properties().size();

    std::string hash = copyChunkBlob.properties().content_md5();

    // copy checksum from response
    copyChecksum(hash, dst.md5);

    // verify checksum, delete and report fail if mismatched
    if (Config::getInstance().verifyChunkChecksum() && !compareChecksum(hash, src.md5, dst.getChunkName())) {
        deleteChunk(dst);
        return false;
    } 

    return true;
}

bool AzureContainer::moveChunk(const Chunk &src, Chunk &dst) {
    return copyChunk(src, dst) && deleteChunk(src);
}

bool AzureContainer::hasChunk(const Chunk &chunk) {
    return checkChunkAttributes(chunk);
}

bool AzureContainer::checkChunkAttributes(const Chunk &chunk, bool forceChecksumCheck, bool checksumOnly) {
    char bpath[BPATH_MAX];
    if (genBlobPath(bpath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get blob path name";
        return false;
    }

    azure::storage::cloud_block_blob chunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath));
    try {
        chunkBlob.download_attributes(_accessCond, _reqOpts, _opCxt);
    } catch (std::runtime_error &e) {
        return false;
    }

    return (checksumOnly || chunkBlob.properties().size() == (unsigned int) chunk.size) && // object size
            (
                (!Config::getInstance().verifyChunkChecksum() && !forceChecksumCheck) ||
                compareChecksum(chunkBlob.properties().content_md5(), chunk.md5, chunk.getChunkName())
            ) // object checksum
    ;
}

bool AzureContainer::revertChunk(const Chunk &chunk) {
    char bpath[BPATH_MAX];
    if (genBlobPath(bpath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get current blob path name";
        return false;
    }
    // get source and dest blob
    azure::storage::cloud_block_blob oldChunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath), chunk.chunkVersion);
    azure::storage::cloud_block_blob curChunkBlob = _blobContainer.get_block_blob_reference(_XPLATSTR(bpath));

    try {
        curChunkBlob.start_copy(oldChunkBlob, _accessCond, _accessCond, _reqOpts, _opCxt);
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to revert chunks, operation not supported" << e.what();
        return false;
    }

    LOG(INFO) << "Reverted chunk " << bpath << " to version " << chunk.chunkVersion;

    return true;
}

bool AzureContainer::verifyChunk(const Chunk &chunk) {
    bool matched = false;
    std::string chunkName = chunk.getChunkName();

    matched = checkChunkAttributes(chunk, /* forceChecksumCheck */ true, /* checksumOnly */ true);
    DLOG(INFO) << "Check chunk " << chunkName << " using download attributes request, result = " << matched;

    return matched;
}

unsigned long int AzureContainer::getTotalSize(bool needsLock) {
    unsigned long int total = 0;
    // get a list of all objects
    azure::storage::list_blob_item_iterator it = 
            _blobContainer.list_blobs(
                utility::string_t(), // prefix 
                false, // flat-listing
                azure::storage::blob_listing_details::none,
                0, // max num. of results
                _reqOpts,
                _opCxt
            );
    azure::storage::list_blob_item_iterator prev;
    // sum up the size of all objects
    for (; it != prev; prev = it, it++) {
        try {
            azure::storage::cloud_block_blob chunkBlob = it->as_blob();
            chunkBlob.download_attributes(_accessCond, _reqOpts, _opCxt);
            total += chunkBlob.properties().size();
        } catch (std::runtime_error &e) {
            // skip non-blobs
        }
    }
    return total;
}

void AzureContainer::updateUsage() {
    _usage = getTotalSize();
}
