// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h> // exit()

#include <glog/logging.h>

#include <boost/network/protocol/http/message.hpp>
#include <boost/algorithm/string.hpp>

extern "C" {
#include <oss_c_sdk/aos_http_io.h>
#include <oss_c_sdk/aos_string.h>
#include <apr_base64.h>
}

#include "../../common/config.hh"
#include "alicloud.hh"

#define OBJ_PATH_MAX (128)

AliContainer::AliContainer(int id, std::string bucketName, std::string region, std::string keyId, std::string key, unsigned long int capacity) :
        Container(id, capacity) {

    _bucketName = bucketName;
    _endpoint = "oss-" + region + ".aliyuncs.com";
    _keyId = keyId;
    _key = key;

    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    oss_acl_e oss_acl = OSS_ACL_PRIVATE;
    aos_table_t *repHeaders;

    // set up the bucket
    aos_string_t bucket;
    aos_str_set(&bucket, bucketName.c_str());
    aos_status_t *status = oss_create_bucket(options, &bucket, oss_acl, &repHeaders);

    if (aos_status_is_ok(status)) {
        LOG(INFO) << "Bucket " << bucketName << " created";
    } else {
        LOG(ERROR) << "Failed to create bucket " << bucketName << ", " << status->error_msg;
        aos_pool_destroy(pool);
        exit(1);
    }

    // release resources
    aos_pool_destroy(pool);

    updateUsage();
}

AliContainer::~AliContainer() {
}

void AliContainer::initOptions(oss_request_options_t *&options, aos_pool_t *&pool) {
    // create request
    options = oss_request_options_create(pool);
    options->config = oss_config_create(options->pool);
    // set up credentials
    aos_str_set(&options->config->endpoint, _endpoint.c_str());
    aos_str_set(&options->config->access_key_id, _keyId.c_str());
    aos_str_set(&options->config->access_key_secret, _key.c_str());
    // set other config
    options->config->is_cname = 0;
    options->ctl = aos_http_controller_create(options->pool, 0);
}

bool AliContainer::genObjectPath(char *opath, std::string chunkName) {
    return sprintf(opath, "%s", chunkName.c_str()) < OBJ_PATH_MAX;
}

bool AliContainer::compareChecksum(const char *hash, const unsigned char *md5, const std::string &chunkName) {
    // convert the chunk md5 to base64
    char md5base64[MD5_DIGEST_LENGTH];
    md5base64[aos_base64_encode(md5, MD5_DIGEST_LENGTH, md5base64)] = 0;
    bool matched = std::string(hash) == md5base64;
    LOG_IF(WARNING, !matched) << "Chunk " << chunkName << " checksum (" << hash << " vs " << md5base64 << ")";
    return matched;
}

bool AliContainer::compareChecksumInEtag(const std::string &hash, const unsigned char *md5, const std::string &chunkName) {
    std::string md5Hex = ChecksumCalculator::toHex(md5, MD5_DIGEST_LENGTH);
    boost::to_upper(md5Hex);
    bool matched = hash.length() > 3 && hash.substr(1, hash.length() - 2) == md5Hex;
    LOG_IF(WARNING, !matched) << "Chunk " << chunkName << " checksum (" << hash << " vs " << md5Hex << ")";
    return matched;
}

bool AliContainer::copyChecksum(const char *hash, unsigned char *md5) {
    if (hash == NULL || md5 == NULL)
        return false;
    size_t len = strlen(hash);
    if (len > 24)
        return false;
    return apr_base64_decode_binary(md5, hash) == MD5_DIGEST_LENGTH;
}

bool AliContainer::putChunk(Chunk &chunk) {
    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }

    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // init the object and bucket name
    aos_string_t bucket, object;
    aos_str_set(&bucket, _bucketName.c_str());
    aos_str_set(&object, opath);

    // init a table, include the chunk as an object
    aos_table_t *headers = aos_table_make(pool, 0), *repHeaders;
    apr_table_set(headers, "x-oss-meta-author", "oss");
    aos_list_t buffer;
    aos_list_init(&buffer);
    aos_buf_t *content = aos_buf_pack(options->pool, chunk.data, chunk.size);
    aos_list_add_tail(&content->node, &buffer);

    // upload
    aos_status_t *status = oss_put_object_from_buffer(options, &bucket, &object, &buffer, headers, &repHeaders);

    // check upload status
    bool success = aos_status_is_ok(status);
    const char *md5base64 = 0;
    if (success) {
        md5base64 = apr_table_get(repHeaders, OSS_CONTENT_MD5);
        // verify the chunk checksum
        if (Config::getInstance().verifyChunkChecksum()) {
            success = compareChecksum(md5base64, chunk.md5, chunk.getChunkName());
        }

        // copy the checksum from reponse
        copyChecksum(md5base64, chunk.md5);

        LOG(INFO) << "Put chunk " << chunk.getChunkName() << " as object " << opath;
    } else {
        LOG(ERROR) << "Failed to put chunk " << chunk.getChunkName() << " as object " << opath << ", " << status->error_msg << ", " << status->error_code << ", " << status->code;
    }

    // release resources
    aos_pool_destroy(pool);
    return success;
}

bool AliContainer::getChunk(Chunk &chunk, bool skipVerification) {
    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }
    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // init the object and bucket name
    aos_string_t bucket, object;
    aos_str_set(&bucket, _bucketName.c_str());
    aos_str_set(&object, opath);

    // init a table
    aos_table_t *headers = aos_table_make(pool, 0), *repHeaders;
    aos_list_t buffer;
    aos_list_init(&buffer);
    
    aos_status_t *status = oss_get_object_to_buffer(options, &bucket, &object, headers, /* params */ NULL, &buffer, &repHeaders);

    bool success = true;

    if (aos_status_is_ok(status)) {
        chunk.size = aos_buf_list_len(&buffer);
        chunk.data = (unsigned char *) malloc (chunk.size);
        unsigned long int pos = 0, len;
        aos_buf_t *content;
        aos_list_for_each_entry(aos_buf_t, content, &buffer, node) {
            len = aos_buf_size(content);
            memcpy(chunk.data + pos, content->pos, len);
            pos += len;
        }
        // verify checksum
        if (!skipVerification && Config::getInstance().verifyChunkChecksum()) {
            success = chunk.verifyMD5();
        }
    }

    if (success) {
        LOG(INFO) << "Get chunk " << chunk.getChunkName() << " as object " << opath;
    } else {
        LOG(ERROR) << "Failed to get chunk " << chunk.getChunkName() << " as object " << opath << ", " << status->error_msg;
        success = false;
    }

    // release resources
    aos_pool_destroy(pool);
    return success;
}

bool AliContainer::deleteChunk(const Chunk &chunk) {
    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }
    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // init the object and bucket name
    aos_string_t bucket, object;
    aos_str_set(&bucket, _bucketName.c_str());
    aos_str_set(&object, opath);

    // init a table
    aos_table_t *repHeaders;
    
    aos_status_t *status = oss_delete_object(options, &bucket, &object, &repHeaders);

    if (aos_status_is_ok(status)) {
        LOG(INFO) << "Delete chunk " << chunk.getChunkName() << " as object " << opath;
    } else {
        LOG(WARNING) << "Chunk " << chunk.getChunkName() << " as object " << opath << " not exist for delete, " << status->error_msg;
    }

    // release resources
    aos_pool_destroy(pool);
    return true;
}

bool AliContainer::copyChunk(const Chunk &src, Chunk &dst) {
    char sopath[OBJ_PATH_MAX], dopath[OBJ_PATH_MAX];
    if (genObjectPath(sopath, src.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }
    if (genObjectPath(dopath, dst.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }

    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // set the bucket name
    aos_string_t sbucket, sobject, dobject, dbucket;
    aos_str_set(&sbucket, _bucketName.c_str());
    aos_str_set(&dbucket, _bucketName.c_str());
    aos_str_set(&sobject, sopath);
    aos_str_set(&dobject, dopath);

    // init a table
    aos_table_t *repHeaders;
    
    aos_status_t *status = oss_copy_object(options, &sbucket, &sobject, &dbucket, &dobject, NULL, &repHeaders);
    
    bool okay = aos_status_is_ok(status);
    if (okay) {
        LOG(INFO) << "Copy chunk " << src.getChunkName() << " to " << dst.getChunkName() << " from path " << sopath << " to path " << dopath;

        // ask for the object size
        status = oss_head_object(options, &dbucket, &dobject, NULL, &repHeaders);

        okay = aos_status_is_ok(status);

        const char *md5base64 = okay? apr_table_get(repHeaders, OSS_CONTENT_MD5) : 0;

        // verify checksum
        okay = okay && (!Config::getInstance().verifyChunkChecksum() || compareChecksum(md5base64, src.md5, dst.getChunkName()));

        if (okay) {
            dst.size = atol(apr_table_get(repHeaders, OSS_CONTENT_LENGTH));
            // copy the checksum from reponse
            copyChecksum(md5base64, dst.md5);
        } else {
            deleteChunk(dst);
            LOG(ERROR) << "Failed to get the size of copied chunk " << dst.getChunkName() << ", code = " << status->error_code << " msg = " << status->error_msg;
        }
    } else {
        LOG(ERROR) << "Failed to copy chunk " << src.getChunkName() << " to " << dst.getChunkName() << " from path " << sopath << " to path " << dopath;
    }

    // release resources
    aos_pool_destroy(pool);
    return okay;
}

bool AliContainer::moveChunk(const Chunk &src, Chunk &dst) {
    return copyChunk(src, dst) && deleteChunk(src);
}

bool AliContainer::hasChunk(const Chunk &chunk) {
    return checkChunk(chunk);
}

bool AliContainer::checkChunk(const Chunk &chunk, bool forceChecksumCheck, bool checksumOnly) {
    char opath[OBJ_PATH_MAX];
    if (genObjectPath(opath, chunk.getChunkName()) == false) {
        LOG(ERROR) << "Failed to get object path name";
        return false;
    }

    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // set the bucket name
    aos_string_t bucket, object;
    aos_str_set(&bucket, _bucketName.c_str());
    aos_str_set(&object, opath);

    // init a table
    aos_table_t *headers = aos_table_make(pool, 0), *repHeaders;

    aos_status_t *status = oss_head_object(options, &bucket, &object, headers, &repHeaders);

    char *size= (char*) apr_table_get(repHeaders, OSS_CONTENT_LENGTH);
    bool ret = 
            aos_status_is_ok(status) && // object exists
            (checksumOnly || (size && atoi(size) == chunk.size)) && // object size
            (
                (!Config::getInstance().verifyChunkChecksum() && !forceChecksumCheck) ||
                compareChecksum(apr_table_get(repHeaders, OSS_CONTENT_MD5), chunk.md5, chunk.getChunkName())
            ) // object checksum
    ;

    aos_pool_destroy(pool);
    return ret;
}

bool AliContainer::revertChunk(const Chunk &chunk) {
    LOG(ERROR) << "Failed to revert chunks, operation not supported";
    return false;
}

bool AliContainer::verifyChunk(const Chunk &chunk) {
    bool matched = false;
    std::string chunkName = chunk.getChunkName();

    matched = checkChunk(chunk, /* forceChecksumCheck */ true, /* checksumOnly */ true);
    DLOG(INFO) << "Check chunk " << chunkName << " using HeadObj request, result = " << matched;
    
    return matched;
}

bool AliContainer::getTotalSize(unsigned long int &total) {
    total = 0;

    // init the memory pool
    aos_pool_t *pool = 0;
    oss_request_options_t *options = 0;
    aos_pool_create(&pool, NULL);
    // init config options
    initOptions(options, pool);
    
    // set the bucket name
    aos_string_t bucket;
    aos_str_set(&bucket, _bucketName.c_str());
    // get statistics of bucket
    aos_table_t *repHeaders;
    oss_bucket_stat_t bucketStats;
    aos_status_t *status = oss_get_bucket_stat(options, &bucket, &bucketStats, &repHeaders);

    bool ret = false;
    // get the number of bytes in bucket
    if (aos_status_is_ok(status)) {
        total = bucketStats.storage_in_bytes;
        ret = true;
    }

    // release resources
    aos_pool_destroy(pool);
    return ret;
}

void AliContainer::updateUsage() {
    unsigned long int total = 0;
    if (getTotalSize(total))
        _usage = total;
    else
        LOG(WARNING) << "Failed to update the size of contianer id = " << _id;
}
