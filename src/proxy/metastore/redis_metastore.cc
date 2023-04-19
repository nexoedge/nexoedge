// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h>  // exit(), strtol()
#include <stdio.h> // sprintf()
#include <boost/uuid/uuid_io.hpp>

#include <glog/logging.h>

#include "redis_metastore.hh"
#include "../../common/config.hh"
#include "../../common/define.hh"

#include <openssl/md5.h>
#include <openssl/sha.h>

#define NUM_RESERVED_SYSTEM_KEYS   (8)
#define FILE_LOCK_KEY              "//snccFLock"
#define FILE_PIN_STAGED_KEY        "//snccFPinStaged"
#define FILE_REPAIR_KEY            "//snccFRepair"
#define FILE_PENDING_WRITE_KEY     "//snccFPendingWrite"
#define FILE_PENDING_WRITE_COMP_KEY "//snccFPendingWriteComp"
#define BG_TASK_PENDING_KEY        "//snccFBgTask"
#define DIR_LIST_KEY               "//snccDirList"
#define JL_LIST_KEY                "//snccJournalFSet"

#define MAX_KEY_SIZE (64)
#define NUM_REQ_FIELDS (10)

static std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength);

RedisMetaStore::RedisMetaStore() {
    Config &config = Config::getInstance();
    _cxt = NULL;
    _cxt = redisConnect(config.getProxyMetaStoreIP().c_str(), config.getProxyMetaStorePort());
    if (_cxt == NULL || _cxt->err) {
        if (_cxt) {
            LOG(ERROR) << "Redis connection error " << _cxt->errstr;
            redisFree(_cxt);
        } else {
            LOG(ERROR) << "Failed to allocate Redis context";
        }
        exit(1);
    }
    _taskScanIt = "0";
    _endOfPendingWriteSet = true;
    LOG(INFO) << "Redis metastore connection init";
}

RedisMetaStore::~RedisMetaStore() {
    redisFree(_cxt);
}

bool RedisMetaStore::putMeta(const File &f) {
    std::lock_guard<std::mutex> lk(_lock);

    char filename[PATH_MAX], vfilename[PATH_MAX], vlname[PATH_MAX];
    int nameLength = genFileKey(f.namespaceId, f.name, f.nameLength, filename);
    int vlnameLength = 0;
    std::string prefix = getFilePrefix(filename);
    int curVersion = -1;

    // find the current version
    redisReply *vr = (redisReply*) redisCommand(
        _cxt
        , "HGET %b ver"
        , filename, (size_t) nameLength
    );

    if (vr != NULL && vr->type == REDIS_REPLY_STRING && vr->len == sizeof(int)) {
        memcpy(&curVersion, vr->str, sizeof(int));
    } else if (vr == NULL) {
        LOG(ERROR) << "Failed to get the current version of file " << f.name << " due to Redis connection error";
        freeReplyObject(vr);
        redisReconnect(_cxt);
        return false;
    }

    freeReplyObject(vr);
    vr = 0;

    // backup the metadata of previous version first if versioning is enabled and verison is newer than the current one
    Config &config = Config::getInstance();
    bool keepVersion = !config.overwriteFiles();
    if (keepVersion && curVersion != -1 && f.version > curVersion) {
        int vnameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version - 1, vfilename);
        // TODO clone instead of put after rename
        // TODO these steps need to be an atomic transaction with HMSET, otherwise metadata can be inconsistent
        redisReply *r = (redisReply*) redisCommand(
            _cxt
            , "RENAME %b %b"
            , filename, (size_t) nameLength
            , vfilename, (size_t) vnameLength
        );
        if (r == NULL || strncmp(r->str,"OK", 2) != 0) {
            if (r == NULL)
                redisReconnect(_cxt);
            LOG(ERROR) << "Failed to backup the previous version " << f.version - 1 << " metadata for file " << f.name;
            freeReplyObject(r);
            return false;
        }
        freeReplyObject(r);
        r = (redisReply*) redisCommand(
            _cxt
            , "HMGET %b size mtime md5 dm numC"
            , vfilename, (size_t) vnameLength
        );
        // create a set of versions (version_list [verison] -> "version size timestamp md5 dm") for this file name
        vlnameLength = genFileVersionListKey(f.namespaceId, f.name, f.nameLength, vlname);
        std::string fsummary;
        fsummary.append(std::to_string(f.version - 1)).append(" ");
        if (r && r->type == REDIS_REPLY_ARRAY) {
            size_t total = 5;
            for (size_t i = 0; i < total; i++) {
                if (r->elements >= i && r->element[i]->type == REDIS_REPLY_STRING) {
                    fsummary.append(r->element[i]->str, r->element[i]->len);
                } else {
                    fsummary.append("-");
                }
                if (i + 1 < total) fsummary.append(" ");
            }
        }
        freeReplyObject(r);
        r = (redisReply*) redisCommand(
            _cxt
            , "ZADD %b %d %b"
            , vlname, (size_t) vlnameLength
            , f.version - 1
            , fsummary.c_str(), fsummary.size()
        );
        LOG(INFO) << "File summary of " << vlname << " version " << f.version << " is >" << fsummary.c_str() << "<";
        freeReplyObject(r);
    }

    // operate on previous versions
    if (keepVersion && curVersion != -1 && f.version < curVersion) {
        // check and only allow such operations if the version exists
        vlnameLength = genFileVersionListKey(f.namespaceId, f.name, f.nameLength, vlname);
        vr = (redisReply*) redisCommand(
            _cxt
            , "ZRANGEBYSCORE %b %d %d"
            , vlname, (size_t) vlnameLength
            , f.version, f.version
        );
        if (vr == NULL || vr->type != REDIS_REPLY_ARRAY || vr->elements < 1) {
            LOG(ERROR) << "Failed to find the previous version " << f.version << " record for file " << f.name << ", type " << (int) vr->type << " elements " << vr->elements;
            freeReplyObject(vr);
            if (vr == NULL)
                redisReconnect(_cxt);
            return false;
        }
        // use the versioned file key
        nameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, filename);
    }

    bool isEmptyFile = f.size == 0;
    unsigned char *codingState = isEmptyFile || f.codingMeta.codingState == NULL? (unsigned char *) "" : f.codingMeta.codingState;
    int deleted = isEmptyFile? f.isDeleted : 0;
    size_t numUniqueBlocks = f.uniqueBlocks.size();
    size_t numDuplicateBlocks = f.duplicateBlocks.size();
    redisAppendCommand(
        _cxt
        ,   "HMSET %b"
            " name %b uuid %s size %b numC %b"
            " sc %s cs %b n %b k %b f %b maxCS %b codingStateS %b codingState %b"
            " numS %b ver %b"
            " ctime %b atime %b mtime %b tctime %b"
            " md5 %b"
            " sg_size %b sg_sc %s sg_cs %b sg_n %b sg_k %b sg_f %b sg_maxCS %b sg_mtime %b"
            " dm %d"
            " numUB %b numDB %b"
        , filename, (size_t) nameLength

        , f.name, (size_t) f.nameLength
        , boost::uuids::to_string(f.uuid).c_str()
        , &f.size, (size_t) sizeof(unsigned long int)
        , &f.numChunks, (size_t) sizeof(int)

        , f.storageClass.c_str()
        , &f.codingMeta.coding, (size_t) sizeof(f.codingMeta.coding)
        , &f.codingMeta.n, (size_t) sizeof(f.codingMeta.n)
        , &f.codingMeta.k, (size_t) sizeof(f.codingMeta.k)
        , &f.codingMeta.f, (size_t) sizeof(f.codingMeta.f)
        , &f.codingMeta.maxChunkSize, (size_t) sizeof(f.codingMeta.maxChunkSize)
        , &f.codingMeta.codingStateSize, (size_t) sizeof(f.codingMeta.codingStateSize)
        , codingState, (size_t) f.codingMeta.codingStateSize

        , &f.numStripes, (size_t) sizeof(int) 
        , &f.version, (size_t) sizeof(int)

        , &f.ctime, (size_t) sizeof(time_t)
        , &f.atime, (size_t) sizeof(time_t)
        , &f.mtime, (size_t) sizeof(time_t)
        , &f.tctime, (size_t) sizeof(time_t)

        , &f.md5, MD5_DIGEST_LENGTH

        , &f.staged.size, (size_t) sizeof(unsigned long int)
        , f.staged.storageClass.c_str()
        , &f.staged.codingMeta.coding, (size_t) sizeof(f.staged.codingMeta.coding)
        , &f.staged.codingMeta.n, (size_t) sizeof(f.staged.codingMeta.n)
        , &f.staged.codingMeta.k, (size_t) sizeof(f.staged.codingMeta.k)
        , &f.staged.codingMeta.f, (size_t) sizeof(f.staged.codingMeta.f)
        , &f.staged.codingMeta.maxChunkSize, (size_t) sizeof(f.staged.codingMeta.maxChunkSize)
        , &f.staged.mtime, (size_t) sizeof(time_t)
        , deleted

        , &numUniqueBlocks, (size_t) sizeof(size_t)
        , &numDuplicateBlocks, (size_t) sizeof(size_t)
    );

    // container ids
    char cname[MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++) {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), cname);
        redisAppendCommand(
            _cxt
            , "HMSET %b %s-cid %b %s-size %b %s-md5 %b %s-bad %d"
            , filename, (size_t) nameLength
            , cname
            , &f.containerIds[i], (size_t) sizeof(int)
            , cname
            , &f.chunks[i].size, (size_t) sizeof(int)
            , cname
            , f.chunks[i].md5, MD5_DIGEST_LENGTH
            , cname
            , (f.chunksCorrupted? f.chunksCorrupted[i] : 0)
        );
    }

    // deduplication fingerprints and block mapping
    char bname[MAX_KEY_SIZE];
    size_t bid = 0;
    for (auto it = f.uniqueBlocks.begin(); it != f.uniqueBlocks.end(); it++, bid++) {
        genBlockKey(bid, bname, /* is unique */ true);
        std::string fp = it->second.first.get();
        redisAppendCommand(
            _cxt
            , "HMSET %b %s %b%b%b%b"  // logical offset, length, fingerprint, physical offset
            , filename, (size_t) nameLength
            , bname
            , &it->first._offset, (size_t) sizeof(unsigned long int)
            , &it->first._length, (size_t) sizeof(unsigned int)
            , fp.data(), fp.size()
            , &it->second.second, (size_t) sizeof(int)
        );
    }
    bid = 0;
    for (auto it = f.duplicateBlocks.begin(); it != f.duplicateBlocks.end(); it++, bid++) {
        genBlockKey(bid, bname, /* is unique */ false);
        std::string fp = it->second.get();
        redisAppendCommand(
            _cxt
            , "HMSET %b %s %b%b%b"  // logical offset, length, fingerprint
            , filename, (size_t) nameLength
            , bname
            , &it->first._offset, (size_t) sizeof(unsigned long int)
            , &it->first._length, (size_t) sizeof(unsigned int)
            , fp.data(), fp.size()
        );
    }
    
    char fidKey[MAX_KEY_SIZE + 64];
    int setKey = 0;

    // add uuid-to-file-name maping
    if (genFileUuidKey(f.namespaceId, f.uuid, fidKey) == false) {
        LOG(WARNING) << "File uuid " << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    } else {
        redisAppendCommand(
            _cxt
            , "SET %s %b"
            , fidKey
            , f.name, (size_t) f.nameLength
        );
        setKey += 1;
    }
    // update the corresponding directory prefix set of this file
    redisAppendCommand(
        _cxt
        , "SADD %s %b"
        , prefix.c_str()
        , filename, (size_t) nameLength
    );
    // update global directory list
    redisAppendCommand(
        _cxt
        , "SADD %s %s"
        , DIR_LIST_KEY, prefix.c_str()
    );
    setKey += 2;

    // issue all commands and check their replies
    redisReply *r = 0;
    for (size_t i = 0; i < f.numChunks + numUniqueBlocks + numDuplicateBlocks + 1 + setKey; i++) {
        if (redisGetReply(_cxt, (void**) &r) != REDIS_OK) {
            LOG(ERROR) << "Redis reply with error, " << (r? r->str : "NULL");
            if (r == NULL) {
                redisReconnect(_cxt);
            }
            freeReplyObject(r);
            r = 0;
            return false;
        }
        freeReplyObject(r);
        r = 0;
    }
    return true;
}

bool RedisMetaStore::getMeta(File &f, int getBlocks) {
    std::lock_guard<std::mutex> lk(_lock);

    char filename[PATH_MAX];
    int nameLength = genFileKey(f.namespaceId, f.name, f.nameLength, filename);

    size_t numUniqueBlocks = 0, numDuplicateBlocks = 0;

    // a version is specified
    if (f.version != -1) {
        redisReply *r = (redisReply *) redisCommand(
            _cxt
            , "HGET %b ver"
            , filename, (size_t) nameLength
        );
        // check if the version is the latest (current) one
        int version = -1;
        if (r != NULL && r->type == REDIS_REPLY_STRING) {
            memcpy(&version, r->str, sizeof(int));
        }
        // if it is not the current one, find the metadata using versioned key instead
        if (version != f.version)
            nameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, filename);
        freeReplyObject(r);
    }

    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "HMGET %b"
        " size numC numS uuid sc"
        " cs n k f maxCS"
        " codingStateS codingState ver ctime atime"
        " mtime tctime md5 sg_size sg_sc"
        " sg_cs sg_n sg_k sg_f sg_maxCS"
        " sg_mtime dm numUB numDB"
        , filename, (size_t) nameLength
    );

    // check if get is successful
    if (r == NULL) {
        redisReconnect(_cxt);
        LOG(WARNING) << "Failed to get metadata for file " << f.name;
        return false;
    }

    LOG_IF(ERROR, r->type != REDIS_REPLY_ARRAY || r->elements < NUM_REQ_FIELDS) << "Not enough field for file metadata (" << r->elements << ", " << r->type << ")";

    // reply is not as expected, no / corrupted file metadata
    if (r->type != REDIS_REPLY_ARRAY || r->elements <= NUM_REQ_FIELDS) {
        LOG(INFO) << "Unexpected metadata found (file not exist?), file [" << filename << "]";
        freeReplyObject(r);
        r = 0;
        return false;
    }

#define check_and_copy_string(__FIELD__, __IDX__, __LEN__) \
        do { \
            if (r->element[__IDX__]->type != REDIS_REPLY_STRING || (size_t) r->element[__IDX__]->len < (size_t) __LEN__) { \
                __FIELD__ = ""; \
            } \
            __FIELD__ = std::string(r->element[__IDX__]->str, r->element[__IDX__]->len); \
        } while(0)

#define check_and_copy_field(__FIELD__, __IDX__, __LEN__) \
        do { \
            if (r->element[__IDX__]->type != REDIS_REPLY_STRING || (size_t) r->element[__IDX__]->len < (size_t) __LEN__) { \
                DLOG(ERROR) << "Failed to parse field " << __IDX__ << " from metadata of file " << filename << " (" << nameLength << "), reply type " << (int) r->element[__IDX__]->type << ", length " << (int) r->element[__IDX__]->len; \
                freeReplyObject(r); \
                r = 0; \
                return false; \
            } \
            memcpy(__FIELD__, r->element[__IDX__]->str, __LEN__); \
        } while(0)

#define check_and_copy_field_at_offset(__FIELD__, __IDX__, __OFS__, __LEN__) \
        do { \
            if (r->element[__IDX__]->type != REDIS_REPLY_STRING || (size_t) r->element[__IDX__]->len < (size_t) __OFS__ + __LEN__) { \
                DLOG(ERROR) << "Failed to parse field " << __IDX__ << " from metadata of file " << filename << " (" << nameLength << "), reply type " << (int) r->element[__IDX__]->type << ", length " << (int) r->element[__IDX__]->len; \
                freeReplyObject(r); \
                r = 0; \
                return false; \
            } \
            memcpy(__FIELD__, r->element[__IDX__]->str + __OFS__, __LEN__); \
        } while(0)

#define check_and_copy_or_set_field(__FIELD__, __IDX__, __LEN__, __DEFAULT_VAL__) \
        do { \
            if (r->elements <= __IDX__ || r->element[__IDX__]->type != REDIS_REPLY_STRING || (size_t) r->element[__IDX__]->len < (size_t) __LEN__) \
                *__FIELD__ = __DEFAULT_VAL__; \
            else \
                memcpy(__FIELD__, r->element[__IDX__]->str, __LEN__); \
        } while(0)

#define check_and_convert_or_set_field(__FIELD__, __IDX__, __LEN__, __CONV__, __DEFAULT_VAL__) \
        do { \
            if (r->elements <= __IDX__ || r->element[__IDX__]->type != REDIS_REPLY_STRING || (size_t) r->element[__IDX__]->len < (size_t) __LEN__) { \
                *__FIELD__ = __DEFAULT_VAL__; \
            } else { \
                *__FIELD__ = __CONV__(r->element[__IDX__]->str); \
            } \
        } while(0)


    check_and_copy_field(&f.size, 0, sizeof(unsigned long int));
    check_and_copy_field(&f.numChunks, 1, sizeof(int));
    check_and_copy_field(&f.numStripes, 2, sizeof(int));
    if (f.setUUID(std::string(r->element[3]->str, r->element[3]->len)) == false) {
        LOG(ERROR) << "Invalid UUID in metadata " << r->element[3]->str;
        freeReplyObject(r);
        r = 0;
        return false;
    }
    // storage policy
    check_and_copy_string(f.storageClass, 4, 1);
    check_and_copy_field(&f.codingMeta.coding, 5, sizeof(f.codingMeta.coding));
    check_and_copy_field(&f.codingMeta.n, 6, sizeof(f.codingMeta.n));
    check_and_copy_field(&f.codingMeta.k, 7, sizeof(f.codingMeta.k));
    check_and_copy_field(&f.codingMeta.f, 8, sizeof(f.codingMeta.f));
    check_and_copy_field(&f.codingMeta.maxChunkSize, 9, sizeof(f.codingMeta.maxChunkSize));
    check_and_copy_field(&f.codingMeta.codingStateSize, 10, sizeof(f.codingMeta.codingStateSize));
    if (f.codingMeta.codingStateSize > 0) {
        f.codingMeta.codingState = new unsigned char [f.codingMeta.codingStateSize];
        check_and_copy_field(f.codingMeta.codingState, 11, f.codingMeta.codingStateSize);
    }
    // version control
    check_and_copy_field(&f.version, 12, sizeof(int));
    check_and_copy_or_set_field(&f.ctime, 13, sizeof(time_t), 0);
    check_and_copy_or_set_field(&f.atime, 14, sizeof(time_t), 0);
    check_and_copy_or_set_field(&f.mtime, 15, sizeof(time_t), 0);
    check_and_copy_or_set_field(&f.tctime, 16, sizeof(time_t), 0);
    // checksum
    check_and_copy_or_set_field(f.md5, 17, MD5_DIGEST_LENGTH, 0);
    // staging
    check_and_copy_or_set_field(&f.staged.size, 18, sizeof(f.staged.size), INVALID_FILE_OFFSET);
    check_and_copy_string(f.staged.storageClass, 19, 1);
    check_and_copy_field(&f.staged.codingMeta.coding, 20, sizeof(f.staged.codingMeta.coding));
    check_and_copy_field(&f.staged.codingMeta.n, 21, sizeof(f.staged.codingMeta.n));
    check_and_copy_field(&f.staged.codingMeta.k, 22, sizeof(f.staged.codingMeta.k));
    check_and_copy_field(&f.staged.codingMeta.f, 23, sizeof(f.staged.codingMeta.f));
    check_and_copy_field(&f.staged.codingMeta.maxChunkSize, 24, sizeof(f.staged.codingMeta.maxChunkSize));
    check_and_copy_or_set_field(&f.staged.mtime, 25, sizeof(time_t), 0);
    // deletion mark
    check_and_convert_or_set_field(&f.isDeleted, 26, 1, atoi, 0);
    // blocks under deduplication
    check_and_copy_or_set_field(&numUniqueBlocks, 27, sizeof(size_t), 0);
    check_and_copy_or_set_field(&numDuplicateBlocks, 28, sizeof(size_t), 0);

    freeReplyObject(r);
    r = 0;

    // get container ids and attributes
    if (!f.initChunksAndContainerIds()) {
        LOG(ERROR) << "Failed to allocate space for container ids";
        return false;
    }

    char cname[MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++) {
        genChunkKeyPrefix(i, cname);
        redisAppendCommand(
            _cxt
            , "HMGET %b %s-cid %s-size %s-md5 %s-bad"
            , filename, (size_t) nameLength
            , cname
            , cname
            , cname
            , cname
        );
    }


    for (int i = 0; i < f.numChunks; i++) {
        if (redisGetReply(_cxt, (void**) &r) != REDIS_OK) {
            LOG(ERROR) << "Redis reply with error, " << (r? r->str : "NULL");
            if (r == NULL) {
                redisReconnect(_cxt);
            }
            freeReplyObject(r);
            r = 0;
            return false;
        }

        if (r->type != REDIS_REPLY_ARRAY || r->elements < 2) {
            freeReplyObject(r);
            r = 0;
            LOG(ERROR) << "Not enough field for chunk metadata (" << r->elements << ", " << r->type << ")";
            return false;
        }

        check_and_copy_field(&f.containerIds[i], 0, sizeof(int));
        check_and_copy_field(&f.chunks[i].size, 1, sizeof(int));
        check_and_copy_or_set_field(f.chunks[i].md5, 2, MD5_DIGEST_LENGTH, 0);
        f.chunksCorrupted[i] = r->elements <= 3? false : (bool) atoi(r->element[3]->str);
        f.chunks[i].setId(f.namespaceId, f.uuid, i);
        f.chunks[i].data = 0;
        f.chunks[i].freeData = true;
        f.chunks[i].fileVersion = f.version;
        freeReplyObject(r);
        r = 0;
    }

    // get block attributes for deduplication
    BlockLocation::InObjectLocation loc;
    Fingerprint fp;
    char bname[MAX_KEY_SIZE];
    if (getBlocks == 1 || getBlocks == 3) { // unique blocks

        int pOffset = 0;
        for (size_t i = 0; i < numUniqueBlocks; i++) {
            genBlockKey(i, bname, /* is unique */ true);
            redisAppendCommand(
                _cxt
                , "HMGET %b %s"
                , filename, (size_t) nameLength
                , bname
            );
        }

        int noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        int hasFpOfs = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;
        int lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH + sizeof(int);
        for (size_t i = 0; i < numUniqueBlocks; i++) {
            if (redisGetReply(_cxt, (void**) &r) != REDIS_OK) {
                LOG(ERROR) << "Redis reply with error, " << (r? r->str : "NULL");
                if (r == NULL) {
                    redisReconnect(_cxt);
                }
                freeReplyObject(r);
                r = 0;
                return false;
            }
            if (r->type != REDIS_REPLY_ARRAY || r->elements != 1) {
                LOG(ERROR) << "Failed to get metadata for block " << i << " of file " << f.name << ", type = " << r->type << " num = " << r->elements;
                freeReplyObject(r);
                r = 0;
                return false;
            }
            check_and_copy_field_at_offset(&loc._offset, 0, 0, sizeof(unsigned long int));
            check_and_copy_field_at_offset(&loc._length, 0, sizeof(unsigned long int), sizeof(unsigned int));
            if (r->element[0]->len >= lengthWithFp) {
                fp.set(r->element[0]->str + noFpOfs, SHA256_DIGEST_LENGTH);
                check_and_copy_field_at_offset(&pOffset, 0, hasFpOfs, sizeof(int));
            } else {
                check_and_copy_field_at_offset(&pOffset, 0, noFpOfs, sizeof(int));
            }
            auto followIt = f.uniqueBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.uniqueBlocks.emplace_hint(followIt, std::make_pair(loc, std::make_pair(fp, pOffset)));

            freeReplyObject(r);
            r = 0;
        }
    }
    if (getBlocks == 2 || getBlocks == 3) { // duplicate blocks
        for (size_t i = 0; i < numDuplicateBlocks; i++) {
            genBlockKey(i, bname, /* is unique */ false);
            redisAppendCommand(
                _cxt
                , "HMGET %b %s"
                , filename, (size_t) nameLength
                , bname
            );
        }

        int noFpOfs = sizeof(unsigned long int) + sizeof(unsigned int);
        int lengthWithFp = sizeof(unsigned long int) + sizeof(unsigned int) + SHA256_DIGEST_LENGTH;
        for (size_t i = 0; i < numDuplicateBlocks; i++) {
            if (redisGetReply(_cxt, (void**) &r) != REDIS_OK) {
                LOG(ERROR) << "Redis reply with error, " << (r? r->str : "NULL");
                if (r == NULL) {
                    redisReconnect(_cxt);
                }
                freeReplyObject(r);
                r = 0;
                return false;
            }
            if (r->type != REDIS_REPLY_ARRAY || r->elements != 1) {
                LOG(ERROR) << "Failed to get metadata for block " << i << " of file " << f.name << ", type = " << r->type << " num = " << r->elements;
                freeReplyObject(r);
                r = 0;
                return false;
            }
            check_and_copy_field_at_offset(&loc._offset, 0, 0, sizeof(unsigned long int));
            check_and_copy_field_at_offset(&loc._length, 0, sizeof(unsigned long int), sizeof(unsigned int));
            if (r->element[0]->len >= lengthWithFp) {
                fp.set(r->element[0]->str + noFpOfs, SHA256_DIGEST_LENGTH);
            }
            auto followIt = f.duplicateBlocks.end(); // hint is the item after the element to insert for c++11, and before the element for c++98
            f.duplicateBlocks.emplace_hint(followIt, std::make_pair(loc, fp));

            freeReplyObject(r);
            r = 0;
        }
    }

#undef check_and_copy_field
#undef check_and_copy_field_at_offset
#undef check_and_copy_or_set_field
#undef check_and_convert_or_set_field
#undef check_and_copy_string

    return true;
}

bool RedisMetaStore::deleteMeta(File &f) {

    char filename[PATH_MAX], vfilename[PATH_MAX], vlname[PATH_MAX];
    int nameLength = genFileKey(f.namespaceId, f.name, f.nameLength, filename);
    int vlnameLength = genFileVersionListKey(f.namespaceId, f.name, f.nameLength, vlname);
    int vnameLength = 0;

    int versionToDelete = f.version;

    std::string prefix = getFilePrefix(filename);

    Config &config = Config::getInstance();

    bool isVersioned = !config.overwriteFiles();
    bool lazyDeletion = false;
    bool ret = true;

    DLOG(INFO) << "Delete file " << f.name << " version " << f.version;

    if (!getMeta(f)) {
        LOG(WARNING) << "Deleting a non-existing file " << f.name;
        return false;
    }

    // versioning enabled and version not speicified, add a deleter marker
    if ((isVersioned || lazyDeletion) && versionToDelete == -1) {
        f.isDeleted = true;
        f.size = 0;
        f.version += 1;
        f.numChunks = 0;
        f.numStripes = 0;
        f.mtime = time(NULL);
        memset(f.md5, 0, MD5_DIGEST_LENGTH);
        ret = putMeta(f);
        // tell the caller not to remove the data
        f.version = -1;
        DLOG(INFO) << "Remove the current version " << f.version << " of file " << f.name;
        return ret;
    }

    // delete a specific version
    if (isVersioned && versionToDelete != -1) {
        int curVersion = -1, numVersions = 0, versionToRemove = -1;
        // find the current version
        redisReply *vr = (redisReply*) redisCommand(
            _cxt
            , "HGET %b ver" 
            , filename, (size_t) nameLength
        );
        if (vr == NULL || vr->type != REDIS_REPLY_STRING) {
            LOG(ERROR) << "Failed to find current version number of file " << f.name << " with previous version " << f.version;
            if (vr == NULL) {
                redisReconnect(_cxt);
            }
            freeReplyObject(vr);
            return false;
        }
        memcpy(&curVersion, vr->str, sizeof(int));
        freeReplyObject(vr);
        // find the number of versions
        vr = (redisReply*) redisCommand(
            _cxt
            , "ZCARD %b"
            , vlname, (size_t) vlnameLength
        );
        if (vr != NULL && vr->type == REDIS_REPLY_INTEGER) {
            numVersions = vr->integer;
        }
        freeReplyObject(vr);
        vr = 0;
        if (curVersion == f.version) { // delete the current version
            // rename the 2nd latest version as the latest one
            if (numVersions > 0) {
                // find the 2nd latest version
                vr = (redisReply*) redisCommand(
                    _cxt
                    , "ZREVRANGEBYSCORE %b +inf -inf WITHSCORES LIMIT 0 1"
                    , vlname, (size_t) vlnameLength
                );
                if (vr == NULL || vr->type != REDIS_REPLY_ARRAY || vr->elements < 2 || vr->element[0]->type != REDIS_REPLY_STRING) {
                    LOG(ERROR) << "Failed to find 2nd latest version of file " << f.name << " for replacing the current version";
                    if (vr == NULL) {
                        redisReconnect(_cxt);
                    }
                    freeReplyObject(vr);
                    return false;
                }
                versionToRemove = atoi(vr->element[0]->str);
                freeReplyObject(vr);
                // rename 2nd latest version as the current one
                vnameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, versionToRemove, vfilename);
                vr = (redisReply*) redisCommand(
                    _cxt
                    , "RENAME %b %b"
                    , vfilename, (size_t) vnameLength
                    , filename, (size_t) nameLength
                );
                if (vr == NULL || strncmp(vr->str, "OK", 2) != 0) {
                    LOG(ERROR) << "Failed to rename 2nd latest version of file " << f.name << " to the current version, reply = " << (void*) vr << " result " << (vr? vr->str : "NIL");
                    if (vr == NULL) {
                        redisReconnect(_cxt);
                    }
                    freeReplyObject(vr);
                    return false;
                }
                freeReplyObject(vr);
                DLOG(INFO) << "Update the current version of file " << f.name << " to " << versionToRemove;
            }
        } else { // operates on the previous versions
            if (numVersions == 0) {
                // no previous versions to operate on
                return false;
            } else {
                // remove the metadata record and version list record of specified version
                versionToRemove = f.version;
            }
        }
        if (versionToRemove != -1) {
            // remove the version from version list
            vr = (redisReply*) redisCommand(
                _cxt
                , "ZREMRANGEBYSCORE %b %d %d"
                , vlname, (size_t) vlnameLength
                , versionToRemove, versionToRemove
            );
            DLOG(INFO) << "Remove version " << versionToRemove << " from version list of file " << f.name;
            freeReplyObject(vr);
            // remove old version if not renamed to the current one
            if (curVersion != f.version) {
                vnameLength = genVersionedFileKey(f.namespaceId, f.name, f.nameLength, f.version, vfilename);
                vr = (redisReply*) redisCommand(
                    _cxt
                    , "DEL %b"
                    , vfilename, (size_t) vnameLength
                );
                freeReplyObject(vr);
            }
            // let the caller handle the data (deletion), without removing the reverted index
            return true;
        }
    }

    std::lock_guard<std::mutex> lk(_lock);
    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "DEL %b"
        , filename, (size_t) nameLength
    );

    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer <= 0) {
        LOG(ERROR) << "Failed to delete file metadata of file " << f.name;
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        freeReplyObject(r);
        r = 0;
        return false;
    }
    freeReplyObject(r);
    r = 0;

    char fidKey[MAX_KEY_SIZE + 64];

    // TODO remove workaround for renamed file
    f.genUUID();

    if (!genFileUuidKey(f.namespaceId, f.uuid, fidKey)) {
        LOG(WARNING) << "File uuid" << boost::uuids::to_string(f.uuid) << " is too long to generate a reverse key mapping";
    } else {
        r = (redisReply *) redisCommand(
            _cxt
            , "DEL %s"
            , fidKey
        );
    }
    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer <= 0) {
        LOG(WARNING) << "Failed to delete reverse mapping of file " << f.name << " (" << fidKey;
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        //ret = false;
    }
    freeReplyObject(r);
    r = 0;

    // remove the file name from the file list of its directory;
    // check the number of files remains in the directory;
    // remove the directory from the directory list if the directory has no files left
    std::string script = 
        "local ret = redis.call('SREM', KEYS[1], ARGV[1]); \
        local val = redis.call('SCARD', KEYS[1]); \
        if val == 0 then \
            return redis.call('SREM', KEYS[2], KEYS[1]); \
        end \
        return ret;"
    ;
    // remove file from prefix set
    r = (redisReply *) redisCommand(
        _cxt
        , "EVAL %s 2 %s %s %b"
        , script.c_str()
        , prefix.c_str()
        , DIR_LIST_KEY
        , filename, (size_t) nameLength
    );

    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer <= 0) {
        LOG(WARNING) << "Failed to delete the prefix record (" << prefix << ") of file " << f.name << " (" << filename << ")";
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        //ret = false;
    }
    freeReplyObject(r);
    r = 0;

    return ret;
}

bool RedisMetaStore::renameMeta(File &sf, File &df) {
    // file names
    char sfname[PATH_MAX], dfname[PATH_MAX];
    int snameLength = genFileKey(sf.namespaceId, sf.name, sf.nameLength, sfname);
    int dnameLength = genFileKey(df.namespaceId, df.name, df.nameLength, dfname);
    std::string sprefix = getFilePrefix(sfname);
    std::string dprefix = getFilePrefix(dfname);

    // file uuids
    char sfidKey[MAX_KEY_SIZE + 64], dfidKey[MAX_KEY_SIZE + 64];
    sf.genUUID();
    df.genUUID();
    if (!genFileUuidKey(sf.namespaceId, sf.uuid, sfidKey))
        return false;
    if (!genFileUuidKey(df.namespaceId, df.uuid, dfidKey))
        return false;

    // update file names
    std::lock_guard<std::mutex> lk(_lock);
    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "RENAMENX %b %b"
        , sfname, (size_t) snameLength
        , dfname, (size_t) dnameLength
    );
    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer != 1) {
        LOG(ERROR) << "Failed to rename file from " << sf.name << " (" << (int) sf.namespaceId << ") to " << df.name << " (" << (int) df.namespaceId << "), " << (r == NULL || r->type != REDIS_REPLY_INTEGER? "error" : "target name already exists");
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        freeReplyObject(r);
        r = 0;
        return false;
    }

    freeReplyObject(r);
    r = 0;

    // create a uuid key to the new file name
    r = (redisReply *) redisCommand(
        _cxt
        , "SET %s %b"
        , dfidKey
        , dfname, (size_t) dnameLength
    );

    DLOG(INFO) << "Add reverse mapping (" << dfidKey << ") for file " << dfname;

    if (r != NULL && r->type != REDIS_REPLY_ERROR) {
        freeReplyObject(r);
        r = 0;
        // also update uuids 
        r = (redisReply *) redisCommand(
            _cxt
            , "DEL %s"
            , sfidKey
        );
    } else {
        freeReplyObject(r);
        r = 0;
        // undo the rename of file
        r = (redisReply *) redisCommand(
            _cxt
            , "RENAME %b %b"
            , dfname, (size_t) dnameLength
            , sfname, (size_t) snameLength
        );
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        freeReplyObject(r);
        r = 0;
        return false;
    }

    freeReplyObject(r);
    r = 0;

    r = (redisReply *) redisCommand(
        _cxt
        , "HSET %b uuid %s"
        , dfname, (size_t) dnameLength
        , boost::uuids::to_string(df.uuid).c_str()
    );

    if (r == NULL || r->type == REDIS_REPLY_ERROR) {
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        // undo the rename of file
        r = (redisReply *) redisCommand(
            _cxt
            , "RENAME %b %b"
            , dfname, (size_t) dnameLength
            , sfname, (size_t) snameLength
        );
        freeReplyObject(r);
        r = 0;
        return false;
    }

    freeReplyObject(r);
    r = 0;

    // remove file from prefix set
    r = (redisReply *) redisCommand(
        _cxt
        , "SREM %s %b"
        , sprefix.c_str()
        , sfname, (size_t) snameLength 
    );

    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer <= 0) {
        LOG(ERROR) << "Failed to delete the prefix record of source file " << sfname << " (" << sfidKey;
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    }

    freeReplyObject(r);
    r = 0;

    // add file to new prefix set
    r = (redisReply *) redisCommand(
        _cxt
        , "SADD %s %b"
        , dprefix.c_str()
        , dfname, (size_t) dnameLength
    );

    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer <= 0) {
        LOG(ERROR) << "Failed to add the prefix record of dest file " << dfname << " (" << dfidKey;
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    }

    freeReplyObject(r);
    r = 0;

    // TODO update the background task pending list

    return true;
}

bool RedisMetaStore::updateTimestamps(const File &f) {
    std::lock_guard<std::mutex> lk(_lock);

    char fname[PATH_MAX];
    int fnameLength = genFileKey(f.namespaceId, f.name, f.nameLength, fname);

    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "HMSET %b atime %b mtime %b tctime %b"
        , fname, (size_t) fnameLength
        , &f.atime, (size_t) sizeof(time_t)
        , &f.mtime, (size_t) sizeof(time_t)
        , &f.tctime, (size_t) sizeof(time_t)
    );

    if (r == NULL || r->type != REDIS_REPLY_STATUS || r->len != 2 || strncmp("OK", r->str, 2) != 0) {
        LOG(ERROR) << "Failed to update timestamps of file " << f.name << " (" << (int) f.namespaceId << "), " << (r == NULL || r->type != REDIS_REPLY_STATUS? "error" : "reply is not \"OK\"");
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        freeReplyObject(r);
        r = 0;
        return false;
    }

    freeReplyObject(r);
    r = 0;

    return true;
}

int RedisMetaStore::updateChunks(const File &f, int version) {
    std::lock_guard<std::mutex> lk(_lock);

    char fname[PATH_MAX];
    //int nameLength = genFileKey(f.namespaceId, f.name, f.nameLength, fname);
    genFileKey(f.namespaceId, f.name, f.nameLength, fname);

    // check the version and set the chunk metadata if match
    std::string script = 
        "local v = struct.unpack('I', redis.call('hget', KEYS[1], 'ver')); \
            if v == tonumber(ARGV[1]) then \
                return redis.call("
    ;
    script.append("'HMSET'");
    script.append(",'");
    script.append(fname);
    script.append("'");
    char cname[MAX_KEY_SIZE];
    for (int i = 0; i < f.numChunks; i++) {
        genChunkKeyPrefix(f.chunks[i].getChunkId(), cname);
        script.append(",'");
        script.append(cname);
        script.append("-cid',struct.pack('I',");
        script.append(std::to_string(f.containerIds[i]));
        script.append("),'");
        script.append(cname);
        script.append("-size',struct.pack('I',");
        script.append(std::to_string(f.chunks[i].size));
        script.append(")");
    }
    script.append("); \
            else \
                return 1; \
            end; \
        return 2"
    );
    DLOG(INFO) << "Lua Script: " << script;
    // container ids
    redisReply *r = (redisReply *) redisCommand(
            _cxt, 
            "EVAL %s 1 %s %d"
            , script.c_str()
            , fname
            , f.version
    );
    int ret = 0;
    if (!(r != NULL && r->type == REDIS_REPLY_STATUS && strcmp(r->str,"OK") == 0)) {
        if (r != NULL && r->type == REDIS_REPLY_INTEGER)
            ret = r->integer;
        else
            ret = 2;
        LOG(ERROR) << "Failed to operate on metadata of file " << f.name << " (" << fname << ") in background, type = " << (r? r->type : -1) << " int = " << (r? r->integer : -1) << " str = " << (r && (r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_STATUS || r->type == REDIS_REPLY_ERROR)? r->str : "NULL");
    }
    freeReplyObject(r);
    r = 0;
    return ret;
}

bool RedisMetaStore::getFileName(boost::uuids::uuid fuuid, File &f) {
    std::lock_guard<std::mutex> lk(_lock);

    char fidKey[MAX_KEY_SIZE + 64];
    if (!genFileUuidKey(f.namespaceId, fuuid, fidKey))
        return false;
    return getFileName(fidKey, f);
}

unsigned int RedisMetaStore::getFileList(FileInfo **list, unsigned char namespaceId, bool withSize, bool withTime, bool withVersions, std::string prefix) {
    std::lock_guard<std::mutex> lk(_lock);

    if (namespaceId == INVALID_NAMESPACE_ID)
        namespaceId = Config::getInstance().getProxyNamespaceId();

    std::string sprefix;
    sprefix.append(std::to_string(namespaceId)).append("_").append(prefix);
    sprefix = getFilePrefix(sprefix.c_str());
    DLOG(INFO) << "prefix = " << prefix << " sprefix = " << sprefix;

    int numFiles = 0;
    redisReply *r = 0;
    if (prefix == "" || prefix.back() != '/') {
        // search all keys
        r = (redisReply *) redisCommand(
            _cxt,
            "KEYS %d_%s*",
            (int) namespaceId, 
            prefix.c_str()
        );
    } else {
        // search prefix set
        r = (redisReply *) redisCommand(
            _cxt,
            "SMEMBERS %s",
            sprefix.c_str()
        );
    }
    if (r != NULL && r->type != REDIS_REPLY_ERROR) {
        if (r->elements > 0)
            *list = new FileInfo[r->elements];
        for (size_t i = 0; i < r->elements; i++) {
            if (r->element[i]->type != REDIS_REPLY_STRING)
                continue;
            if (isSystemKey(r->element[i]->str))
                continue;
            // full name in form of "namespaceId_filename"
            if (!getNameFromFileKey(
                r->element[i]->str, r->element[i]->len,
                &list[0][numFiles].name,
                list[0][numFiles].nameLength,
                list[0][numFiles].namespaceId
            )) {
                continue;
            }
            FileInfo &cur = list[0][numFiles];
            // get file size and time if requested
            if (withSize || withTime || withVersions) {
                redisReply *metar = (redisReply *) redisCommand(
                    _cxt,
                    "HMGET %s size ctime atime mtime ver dm md5 numC sg_size sg_mtime sc",
                    r->element[i]->str
                );
                if (metar == NULL || metar->type != REDIS_REPLY_ARRAY || metar->elements < 1) {
                    LOG(WARNING) << "Cannot get file size and time of file " << cur.name << ", reply type = " << (metar == NULL? -1 : metar->type);
                    freeReplyObject(metar);
                    metar = 0;
                    continue;
                } else {
                    unsigned long int stagedSize = 0; 
                    // size
                    if (metar->element[0]->type == REDIS_REPLY_STRING && metar->element[0]->len == sizeof(unsigned long int))
                        memcpy(&cur.size, metar->element[0]->str, sizeof(unsigned long int));
                    else
                        cur.size = 0;
                    // creation time
                    if (metar->elements >= 2 && metar->element[1]->type == REDIS_REPLY_STRING && metar->element[1]->len == sizeof(time_t))
                        memcpy(&cur.ctime, metar->element[1]->str, sizeof(time_t));
                    else
                        cur.ctime = 0;
                    // last access time
                    if (metar->elements >= 3 && metar->element[2]->type == REDIS_REPLY_STRING && metar->element[2]->len == sizeof(time_t))
                        memcpy(&cur.atime, metar->element[2]->str, sizeof(time_t));
                    else
                        cur.atime = 0;
                    // last modify time
                    if (metar->elements >= 4 && metar->element[3]->type == REDIS_REPLY_STRING && metar->element[3]->len == sizeof(time_t))
                        memcpy(&cur.mtime, metar->element[3]->str, sizeof(time_t));
                    else
                        cur.mtime = 0;
                    // file version 
                    if (metar->elements >= 5 && metar->element[4]->type == REDIS_REPLY_STRING && metar->element[4]->len == sizeof(int))
                        memcpy(&cur.version, metar->element[4]->str, sizeof(int));
                    else
                        cur.version = 0;
                    // delete marker
                    if (metar->elements >= 6 && metar->element[5]->type == REDIS_REPLY_STRING && metar->element[5]->len == 1)
                        cur.isDeleted = atoi(metar->element[5]->str);
                    else
                        cur.isDeleted = 0;
                    // md5 checksum
                    if (metar->elements >= 7 && metar->element[6]->type == REDIS_REPLY_STRING && metar->element[6]->len == MD5_DIGEST_LENGTH)
                        memcpy(&cur.md5, metar->element[6]->str, MD5_DIGEST_LENGTH);
                    // number of chunks
                    if (metar->elements >= 8 && metar->element[7]->type == REDIS_REPLY_STRING && metar->element[7]->len == sizeof(int))
                        memcpy(&cur.numChunks, metar->element[7]->str, sizeof(int));
                    // staged size
                    if (metar->elements >= 9 && metar->element[8]->type == REDIS_REPLY_STRING && metar->element[8]->len == sizeof(unsigned long int))
                        memcpy(&stagedSize, metar->element[8]->str, sizeof(unsigned long int));
                    // staged last modified time
                    if (metar->elements >= 10 && metar->element[9]->type == REDIS_REPLY_STRING && metar->element[9]->len == sizeof(time_t)) {
                        time_t mtime = 0;
                        memcpy(&mtime, metar->element[9]->str, sizeof(time_t));
                        // use staged file info if staged file is more updated
                        if (mtime > cur.mtime) {
                            cur.mtime = mtime;
                            cur.atime = mtime;
                            cur.size = stagedSize;
                        }
                    }
                    if (metar->elements >= 11 && metar->element[10]->type == REDIS_REPLY_STRING) {
                        cur.storageClass = std::string(metar->element[10]->str, metar->element[10]->len);
                    }
                }
                freeReplyObject(metar);
                metar = 0;
            }
            // do not add delete marker to the list unless for queries on versions
            if (!withVersions && cur.isDeleted) {
                continue;
            }
            if (withVersions && cur.version > 0) {
                char vlname[PATH_MAX];
                int vlnameLength = genFileVersionListKey(cur.namespaceId, cur.name, cur.nameLength, vlname);
                redisReply *metar = (redisReply *) redisCommand(
                    _cxt
                    , "ZRANGE %b 0 %d"
                    , vlname, vlnameLength
                    , cur.version
                );
                if (metar == NULL || metar->type != REDIS_REPLY_ARRAY || metar->elements < 1) {
                    DLOG(INFO) << "No version summary " << cur.name << ", reply type = " << (metar == NULL? -1 : metar->type);
                    if (metar == NULL) {
                        redisReconnect(_cxt);
                    }
                } else {
                    size_t total = metar->elements;
                    cur.numVersions = total;
                    try {
                        cur.versions = new VersionInfo[total];
                        for (size_t vi = 0; vi < total; vi++) {
                            if (metar->element[vi]->type != REDIS_REPLY_STRING)
                                continue;
                            char *ofs = metar->element[vi]->str;
                            char *end = metar->element[vi]->str + metar->element[vi]->len;
                            for (int vj = 0; vj < 6 && ofs < end; vj++) {
                                if (ofs[0] != '-') { // if the field is available (not blanked)
                                    switch (vj) {
                                    case 0: // version number
                                        cur.versions[vi].version = atoi(ofs);
                                        break;
                                    case 1: // size
                                        memcpy(&cur.versions[vi].size, ofs, sizeof(unsigned long int));
                                        break;
                                    case 2: // mtime
                                        memcpy(&cur.versions[vi].mtime, ofs, sizeof(time_t));
                                        break;
                                    case 3: // md5
                                        memcpy(cur.versions[vi].md5, ofs, MD5_DIGEST_LENGTH);
                                        break;
                                    case 4: // delete mark
                                        cur.versions[vi].isDeleted = atoi(ofs);
                                        break;
                                    case 5: // number of chunks 
                                        memcpy(&cur.versions[vi].numChunks, ofs, sizeof(int));
                                        break;
                                    }
                                }
                                // find the next whitespace
                                ofs = vj == 5? NULL : (char*) memchr(ofs, ' ', metar->element[vi]->len - (metar->element[vi]->str - ofs));
                                if (ofs == NULL || ofs >= end) {
                                    break;
                                }
                                // skip the whitespace
                                ofs += 1;
                            }
                            DLOG(INFO) << "Add version " << cur.versions[vi].version << " size " << cur.versions[vi].size << " mtime " << cur.versions[vi].mtime << " deleted " << cur.versions[vi].isDeleted << " to version list of file " << cur.name; 
                        }
                    } catch (std::exception &e) {
                        LOG(ERROR) << "Cannot allocate memory for " << total << " version records";
                        cur.versions = 0;
                    }
                }
                freeReplyObject(metar);
                metar = 0;
            }
            numFiles++;
        }
    }
    if (r == NULL) {
        redisReconnect(_cxt);
    }
    freeReplyObject(r);
    r = 0;
    return numFiles;
}

unsigned int RedisMetaStore::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix, bool skipSubfolders) {
    std::lock_guard<std::mutex> lk(_lock);
    
    // generate the prefix for pattern-based directory searching
    prefix.append("a");
    char filename[PATH_MAX];
    genFileKey(namespaceId, prefix.c_str(), prefix.size(), filename);
    std::string pattern = getFilePrefix(filename, /* no ending slash */ true).append("*");

    std::string cursor = "0";
    unsigned long int count = 0;

    redisReply *r = 0;
    do {
        r = (redisReply*) redisCommand(
            _cxt
            , "SSCAN %s %s MATCH %s"
            , DIR_LIST_KEY
            , cursor.c_str()
            , pattern.c_str()
        );

        if (r == NULL || r->type != REDIS_REPLY_ARRAY || r->elements != 2 || r->element[1]->type != REDIS_REPLY_ARRAY) {
            LOG(ERROR) << "Failed to scan metadata store for folders, r = " << (void *) r << " type = " << (r? r->type : -1) << " elements " << (r? r->elements : -1);
            if (r == NULL) {
                redisReconnect(_cxt);
            }
            freeReplyObject(r);
            return count;
        }


        // update cursor
        cursor = r->element[0]->str;

        DLOG(INFO) << "Cursor = " << cursor << " num elements " << r->element[1]->elements;

        // add the matching folders
        redisReply **listr = r->element[1]->element;
        ssize_t pfsize = pattern.size();
        for (size_t i = 0; i < r->element[1]->elements; i++) {
            // avoid abnormal string with length < pfsize - 1
            if (listr[i]->len < pfsize - 1)
                continue;
            // skip subfolders
            if (skipSubfolders && strchr(listr[i]->str + pfsize - 1, '/') != 0)
                continue;
            DLOG(INFO) << "Add " << std::string(listr[i]->str + pfsize - 1, listr[i]->len - pfsize + 1) << " to the result of " << pattern;
            list.push_back(std::string(listr[i]->str + pfsize - 1, listr[i]->len - pfsize + 1));
            count++;
        }

        freeReplyObject(r);
        r = 0;

    } while (cursor != "0");

    freeReplyObject(r);
    r = 0;

    return count;
}

unsigned long int RedisMetaStore::getMaxNumKeysSupported() {
    // max = (1 << 32) - 1 - NUM_RESERVED_SYSTEM_KEYS, but we store also uuid for each file
    return (unsigned long int) (1 << 31) - NUM_RESERVED_SYSTEM_KEYS / 2 - (NUM_RESERVED_SYSTEM_KEYS % 2); 
}

unsigned long int RedisMetaStore::getNumFiles() {
    std::lock_guard<std::mutex> lk(_lock);
    unsigned long int count = 0;
    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "DBSIZE"
    );
    if (r == NULL || r->type != REDIS_REPLY_INTEGER) {
        LOG(ERROR) << "Failed to get file count";
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    } else {
        count = r->integer;
        freeReplyObject(r);
        r = 0;
        r = (redisReply *) redisCommand(
            _cxt
            , "SCARD %s"
            , DIR_LIST_KEY
        );
        // exclude keys for directory sets
        if (r != NULL && r->type == REDIS_REPLY_INTEGER) {
            count -= r->integer;
        }
        freeReplyObject(r);
        r = 0;
        r = (redisReply *) redisCommand(
            _cxt
            , "KEYS //sncc*"
        );
        // exclude system keys
        if (r != NULL && r->type == REDIS_REPLY_ARRAY) {
            count -= r->elements;
        }
        count /= 2;
    }

    freeReplyObject(r);
    r = 0;
    return count;
}

unsigned long int RedisMetaStore::getNumFilesToRepair() {
    std::lock_guard<std::mutex> lk(_lock);
    // pop up files to repair
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "SCARD %s",
        FILE_REPAIR_KEY
    );

    bool okay = r != NULL && (r->type == REDIS_REPLY_NIL || r->type == REDIS_REPLY_INTEGER);
    
    unsigned long int count = okay? r->integer : -1;

    if (r == NULL) {
        redisReconnect(_cxt);
    }

    freeReplyObject(r);
    r = 0;

    return count;
}

int RedisMetaStore::getFilesToRepair(int numFiles, File files[]) {
    std::lock_guard<std::mutex> lk(_lock);

    // pop up files to repair
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "SPOP %s %d",
        FILE_REPAIR_KEY,
        numFiles
    );

    // retry with legacy command upon error, SPOP only supports multiple items for Redis >=3.2
    if (r != NULL && r->type == REDIS_REPLY_ERROR) {
        freeReplyObject(r);
        r = 0;
        r = (redisReply *) redisCommand(
            _cxt,
            "SPOP %s",
            FILE_REPAIR_KEY
        );
    }

    bool okay = r != NULL && (r->type == REDIS_REPLY_NIL || r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_ARRAY);

    int numFilesToRepair = 0;
    if (okay && r->type == REDIS_REPLY_ARRAY) {
        size_t i = 0;
        // copy the names to repair
        for (;i < r->elements && i < (size_t) numFiles; i++) {
            free(files[numFilesToRepair].name);
            if (r->element[i]->type != REDIS_REPLY_STRING)
                continue;
            if (!getNameFromFileKey(r->element[i]->str, r->element[i]->len, &files[numFilesToRepair].name, files[numFilesToRepair].nameLength, files[numFilesToRepair].namespaceId, &files[numFilesToRepair].version))
                break;
            numFilesToRepair++;
        }
        // put the extra files back back to queue (best effort)
        for (;i < r->elements; i++) {
            redisReply *br = (redisReply *) redisCommand(
                _cxt,
                "SADD %s %s"
                FILE_REPAIR_KEY,
                r->element[i]->str
            );
            freeReplyObject(br);
            br = 0;
        }
    } else if (okay && r->type == REDIS_REPLY_STRING) {
        if (getNameFromFileKey(r->str, r->len, &files[0].name, files[0].nameLength, files[0].namespaceId, &files[0].version)) {
            DLOG(INFO) << "Going to repair file " << files[0].name << " version " << files[0].version << " (" << r->str << ")";
            numFilesToRepair = 1;
        } else {
            // not enough memory, skip the repair for time being
            redisReply *br = (redisReply *) redisCommand(
                _cxt,
                "SADD %s %s"
                FILE_REPAIR_KEY,
                r->str
            );
            freeReplyObject(br);
            br = 0;
        }
    } else if (okay && r->type == REDIS_REPLY_NIL) {
        DLOG(INFO) << "No files pending for repair";
    } else {
        LOG(ERROR) << "Failed to get files to repair, " <<
            (r == NULL? "failed to get a reply" : " reply type = ") <<
            (r != NULL? r->type : 0) << " " << 
            (r != NULL && r->type == REDIS_REPLY_ERROR? r->str : "(NIL)");
    }

    if (r == NULL) {
        redisReconnect(_cxt);
    }

    freeReplyObject(r);
    r = 0;
    return numFilesToRepair;
}

bool RedisMetaStore::markFileAsRepaired(const File &file) {
    return markFileRepairStatus(file, false);
}

bool RedisMetaStore::markFileAsNeedsRepair(const File &file) {
    return markFileRepairStatus(file, true);
}

bool RedisMetaStore::markFileRepairStatus(const File &file, bool needsRepair) {
    return markFileStatus(file, FILE_REPAIR_KEY, needsRepair, "repair");
}

bool RedisMetaStore::markFileAsPendingWriteToCloud(const File &file) {
    return markFileStatus(file, FILE_PENDING_WRITE_KEY, true, "pending write to cloud");
}

bool RedisMetaStore::markFileAsWrittenToCloud(const File &file, bool removePending) {
    return markFileStatus(file, FILE_PENDING_WRITE_COMP_KEY, false, "pending completing write to cloud") && 
            (!removePending || markFileStatus(file, FILE_PENDING_WRITE_KEY, false, "pending write to cloud"));
}

bool RedisMetaStore::markFileStatus(const File &file, const char *listName, bool set, const char *opName) {
    std::lock_guard<std::mutex> lk(_lock);
    char filename[PATH_MAX];
    int nameLength = genVersionedFileKey(file.namespaceId, file.name, file.nameLength, file.version, filename);
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "%s %s %b",
        set? "SADD" : "SREM",
        listName,
        filename, (size_t) nameLength
    );

    bool ret = r != NULL && r->type == REDIS_REPLY_INTEGER;
    if (!ret) {
        LOG(ERROR) << "Failed to " << (set? "add" : "remove") << " file " << file.name << " from the " << opName << " list, " << (r != NULL ? "reply is invalid" : "failed to get reply"); 
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    } else if (r->integer != 1) {
        DLOG(INFO) << "File " << file.name << "(" << filename << ")" << (set? " already" : " not") << " in the " << opName << " list"; 
    } else if (r->integer == 1) {
        DLOG(INFO) << "File " << file.name << "(" << filename << ")" << (set? " added to" : " removed from") << " the " << opName << " list"; 
    } else {
        DLOG(INFO) << "File " << file.name << "(" << filename << ")" << (set? " adding " : " removing result ") << r->integer << "key = " <<  filename; 
    }
        
    freeReplyObject(r);
    r = 0;
    return ret;
}

int RedisMetaStore::getFilesPendingWriteToCloud(int numFiles, File files[]) {
    std::lock_guard<std::mutex> lk(_lock);

    int num = 0;

    redisReply *r = (redisReply *) redisCommand(
            _cxt,
            "SCARD %s_copy"
            , FILE_PENDING_WRITE_KEY
    );

    bool okay = r != NULL && r->type == REDIS_REPLY_INTEGER;
    bool empty = okay && r->integer == 0;

    freeReplyObject(r);
    r = 0;

    // mark end of set iteration
    if (empty && !_endOfPendingWriteSet) {
        _endOfPendingWriteSet = true;
        return num;
    }

    // refill the set for scan
    if (empty) {
        r = (redisReply *) redisCommand(
                _cxt,
                "SDIFFSTORE %s_copy %s %s_not_exists"
                , FILE_PENDING_WRITE_KEY
                , FILE_PENDING_WRITE_KEY
                , FILE_PENDING_WRITE_KEY
        );

        okay = r != NULL && r->type == REDIS_REPLY_INTEGER;
        empty = okay && r->integer == 0;

        freeReplyObject(r);
        r = 0;

        // cannot find any file, or no file is pending, skip checking
        if (!okay || empty) {
            return num;
        }
    }

    // mark the set scanning is in-progress
    _endOfPendingWriteSet = false;

    // try to pop a file name for write
    r = (redisReply *) redisCommand(
            _cxt,
            "SPOP %s_copy"
            , FILE_PENDING_WRITE_KEY
    );

    okay = r != NULL && r->type == REDIS_REPLY_STRING;

    std::string key;
    if (okay)
        key.assign(r->str, r->len);
        
    freeReplyObject(r);
    r = 0;

    if (!okay) {
        if (r == NULL) {
            redisReconnect(_cxt);
        }
        return num;
    }

    // mark the file as pending to complete for write
    r = (redisReply *) redisCommand(
            _cxt,
            "SMOVE %s %s %s"
            , FILE_PENDING_WRITE_KEY
            , FILE_PENDING_WRITE_COMP_KEY
            , key.c_str()
    );

    okay = r != NULL && r->type == REDIS_REPLY_INTEGER && r->integer == 1;

    if (okay && getNameFromFileKey(key.data(), key.length(), &files[0].name, files[0].nameLength, files[0].namespaceId, &files[0].version)) {
        num++;
    }

    if (r == NULL) {
        redisReconnect(_cxt);
    }

    freeReplyObject(r);
    r = 0;
    return num;
}

bool RedisMetaStore::updateFileStatus(const File &file) {
    std::lock_guard<std::mutex> lk(_lock);
    char filename[PATH_MAX];
    int nameLength = genFileKey(file.namespaceId, file.name, file.nameLength, filename);
    bool ret = false;
    redisReply *r = 0;
    if (file.status == FileStatus::PART_BG_TASK_COMPLETED) {
        // decrement number of task by 1, and remove the file is the number of pending task drops to 0
        std::string script;
        script.append(
            "local v = redis.call('zincrby', KEYS[1], -1, ARGV[1]); \
            if v ~= false and v == '0' then \
                redis.call('zrem', KEYS[1], ARGV[1]); \
            end; \
            return v ~= false;"
        );
        r = (redisReply *) redisCommand(
            _cxt,
            "EVAL %s 1 %s %s"
            , script.c_str()
            , BG_TASK_PENDING_KEY
            , filename, nameLength
        );
        ret = r != NULL && r->type == REDIS_REPLY_INTEGER && r->integer == 1;
        if (ret)
            DLOG(INFO) << "File (task completed) " << file.name << " status updated";
    } else if (file.status == FileStatus::BG_TASK_PENDING) {
        // increment number of task by 1
        r = (redisReply *) redisCommand(
            _cxt,
            "ZINCRBY %s 1 %b"
            , BG_TASK_PENDING_KEY
            , filename, (size_t) nameLength
        );
        ret = r != NULL && r->type == REDIS_REPLY_STRING;
        if (ret)
            DLOG(INFO) << "File (task pending) " << file.name << " status updated bg task = " << r->str;
    } else if (file.status == FileStatus::ALL_BG_TASKS_COMPLETED) {
        r = (redisReply *) redisCommand(
            _cxt,
            "ZREM %s %b"
            , BG_TASK_PENDING_KEY
            , filename, (size_t) nameLength
        );
        ret = r != NULL && r->type == REDIS_REPLY_INTEGER && r->integer == 1;
        if (ret) {
            DLOG(INFO) << "File (all task completed) " << file.name << " status updated";
        }
    }
    freeReplyObject(r);
    r = 0;

    // update the last task check time
    time_t tctime = time(NULL);
    r = (redisReply *) redisCommand(
        _cxt ,
        "HSET %b tctime %b"
        , filename, (size_t) nameLength
        , &tctime, (size_t) sizeof(tctime)
    );

    // report failure
    if (ret == false) {
        LOG(ERROR) << "Failed to update status of file " << file.name << ", [" << (r == 0? -1 : r->type) << "] " << (r == 0? "(NIL)" : r->str);
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    }

    freeReplyObject(r);
    r = 0;

    return ret;
}

bool RedisMetaStore::getNextFileForTaskCheck(File &file) {
    std::lock_guard<std::mutex> lk(_lock);
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "ZSCAN %s %s COUNT 1"
        , BG_TASK_PENDING_KEY
        , _taskScanIt.c_str()
    );

    // the expected return value is 
    // when there is something in the set: [0: next iterator (str), 1: [0: current element, 1: score, ...]]
    // when there is nothing in the set: [0: next iterator (str), 1: nil]

    bool ret = false;

    if (r == NULL || r->type != REDIS_REPLY_ARRAY || r->elements != 2 || r->element[0]->type != REDIS_REPLY_STRING) {
        LOG(ERROR) << "Failed to get a valid reply for next file to check";
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    } else {
        // update the next iterator
        _taskScanIt.assign(r->element[0]->str);
        // get the metadata of the file
        if (r->element[1]->type == REDIS_REPLY_ARRAY && r->element[1]->elements >= 2 && r->element[1]->element[0]->type == REDIS_REPLY_STRING) {
            // fill in the file size
            char *key = r->element[1]->element[0]->str;
            char *d = strchr(key, '_');
            if (d != 0) {
                file.nameLength = r->element[1]->element[0]->len - (d - key + 1);
                file.name = (char *) malloc (file.nameLength + 1);
                memcpy(file.name, d + 1, file.nameLength);
                file.name[file.nameLength] = 0;
                file.namespaceId = atoi(key);
                DLOG(INFO) << "Next file to check: " << file.name << ", " << (int) file.namespaceId;
                ret = true;
            }
        }
    }
    freeReplyObject(r);
    r = 0;
    return ret;
}

bool RedisMetaStore::lockFile(const File &file) {
    std::lock_guard<std::mutex> lk(_lock);
    return getLockOnFile(file, true);
}

bool RedisMetaStore::unlockFile(const File &file) {
    std::lock_guard<std::mutex> lk(_lock);
    return getLockOnFile(file, false);
}

std::tuple<int, std::string, int> extractJournalFieldKeyParts(const char *field, size_t fieldLength) {
    std::string fieldKey(field, fieldLength);

    // expected format 'c<chunk_id>-<type>-<container_id>', e.g., c00-op-1
    size_t delimiter1 = fieldKey.find("-");
    size_t delimiter2 = fieldKey.find("-", delimiter1 + 1);
    if (delimiter1 == std::string::npos || delimiter2 == std::string::npos) {
        return std::make_tuple(INVALID_CHUNK_ID, "", INVALID_CONTAINER_ID);
    }

    int chunkId, containerId;
    std::string type;
    // first part is 'c[0-9]+'
    chunkId = strtol(fieldKey.substr(1, delimiter1-1).c_str(), NULL, 10);
    // second part is a string
    type = fieldKey.substr(delimiter1 + 1, delimiter2-delimiter1-1);
    // third part is a '[0-9]+'
    containerId = strtol(fieldKey.substr(delimiter2 + 1).c_str(), NULL, 10);

    return std::make_tuple(chunkId, type, containerId);
}

bool RedisMetaStore::addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite) {
    std::lock_guard<std::mutex> lk(_lock);

    char key[MAX_KEY_SIZE];
    int keyLength = genFileJournalKey(file.namespaceId, file.name, file.nameLength, file.version, key);

    char filename[PATH_MAX];
    int nameLength = genVersionedFileKey(file.namespaceId, file.name, file.nameLength, file.version, filename);

    const char *opType = isWrite? "w" : "d";
    const char *status = "pre";

    char cname[MAX_KEY_SIZE];
    genChunkKeyPrefix(chunk.getChunkId(), cname);

    redisReply *r = NULL;
    // set the file journal
    // first, set all previous write to delete
    int cursor = 0;
    bool skipAdding = false;
    do {
        // keep scanning previous for records
        r = (redisReply*) redisCommand(
            _cxt
            , "HSCAN %b %d MATCH %s-op*"
            , key, (size_t) keyLength
            , cursor
            , cname
        );
        if (r == NULL || r->type != REDIS_REPLY_ARRAY || r->elements < 1) {
            LOG(ERROR) << "Failed to add the journal record of chunk " << chunk.getChunkId() << " of file " << file.name << " with namespace " << (int) file.namespaceId;
            freeReplyObject(r);
            if (r == NULL) redisReconnect(_cxt);
            return false;
        }
        int numModifiedRecords = 0;
        int containerIdMatchedIdx = -1;
        int extractedContainerId = 0;
        // the reply has two parts
        // the first part [0] is the cursor for next scan
        // the second part [1] is an array of key and value results
        cursor = r->element[0]->integer;
        for (size_t ei = 0; r->elements >= 2 && ei < r->element[1]->elements; ei++) {
            auto &value = r->element[1]->element[ei];

            // must be a string
            if (value->type != REDIS_REPLY_STRING) {
                continue;
            }

            // modify all pending write operations to check into delete
            if (ei % 2 == 1 && strncmp(value->str, "w", 1) == 0) {
                auto &preValue = r->element[1]->element[ei-1];
                // must be a string
                if (preValue->type != REDIS_REPLY_STRING) {
                    continue;
                }
                redisAppendCommand(
                    _cxt
                    , "HSET %b %b %s"
                    , key, (size_t) keyLength
                    , preValue->str, preValue->len
                    , "d"
                );
                // if any previous write is to be superseded by a deletion
                if (extractedContainerId == containerId && !isWrite) {
                    containerIdMatchedIdx = numModifiedRecords;
                }
                numModifiedRecords++;
            } else if (ei % 2 == 0) {
                std::tie(std::ignore, std::ignore, extractedContainerId) = extractJournalFieldKeyParts(value->str, value->len);
            }
        }
        freeReplyObject(r);

        bool allCompleted = true;
        for (int ri = 0; ri < numModifiedRecords; ri++) {
            bool opCompleted = redisGetReply(_cxt, (void**) &r) == REDIS_OK;
            allCompleted = allCompleted && opCompleted;
            // mark that a previous write is changed into a deletion
            if (containerIdMatchedIdx == ri && opCompleted) {
                skipAdding = true;
            }
            freeReplyObject(r);
            r = NULL;
        }
        if (!allCompleted) {
            LOG(ERROR) << "Failed to add the journal record of chunk " << chunk.getChunkId() << " of file " << file.name << " with namespace " << (int) file.namespaceId << ", cannot update existing records to avoid duplicated chunks.";
            return false;
        }
        DLOG(INFO) << "Modified " << numModifiedRecords << " records before adding a journal record of file " << file.name << " with namespace " << (int) file.namespaceId;
    } while (cursor != 0);

    if (skipAdding) return true;

    // second, set the latest record
    std::string script = 
         "local e2 = redis.call('HMSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8]); \
         if e2['ok'] == 'OK' then \
            return redis.call('SADD', KEYS[2], ARGV[9]); \
         end \
         return -1; \
    ";
    r = (redisReply*) redisCommand(
        _cxt
        , "EVAL %s 2 %b %s %s-size-%d %b %s-md5-%d %b %s-op-%d %s %s-status-%d %s %b"
        , script.c_str()
        , key, (size_t) keyLength /* KEYS[1] */
        , JL_LIST_KEY /* KEYS[2] */
        , cname, containerId
        , &chunk.size, sizeof(int)
        , cname, containerId
        , chunk.md5, (size_t) MD5_DIGEST_LENGTH
        , cname, containerId
        , opType
        , cname, containerId
        , status
        , filename, (size_t) nameLength /* ARGV[9] */
    );
    if (r == NULL || r->type != REDIS_REPLY_INTEGER || r->integer == -1) {
        freeReplyObject(r);
        LOG(ERROR) << "Failed to add the journal record of chunk " << chunk.getChunkId() << " of file " << file.name << " with namespace " << (int) file.namespaceId;
        if (r == NULL) redisReconnect(_cxt);
        return false;
    }
    freeReplyObject(r);
    return true;
}

bool RedisMetaStore::updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId) {
    char key[MAX_KEY_SIZE];
    int keyLength = genFileJournalKey(file.namespaceId, file.name, file.nameLength, file.version, key);

    char filename[PATH_MAX];
    int nameLength = genVersionedFileKey(file.namespaceId, file.name, file.nameLength, file.version, filename);

    const char *opType = isWrite? "w" : "d";
    const char *status = "post";

    char cname[MAX_KEY_SIZE];
    genChunkKeyPrefix(chunk.getChunkId(), cname);

    redisReply *r = NULL;
    bool success = false;
    if (deleteRecord) {
        // delete the fields; if no field is left, remove the file from the set of files with journal
        std::string script = 
            "redis.call('HDEL', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4]); \
            local e2 = redis.call('HLEN', KEYS[1]); \
            if e2 == 0 then \
                return redis.call('SREM', KEYS[2], KEYS[3]); \
            end \
            return 2;"
        ;
        r = (redisReply *) redisCommand(
            _cxt
            , "EVAL %s 3 %b %s %b %s-size-%d %s-md5-%d %s-op-%d %s-status-%d"
            , script.c_str()
            , key, (size_t) keyLength
            , JL_LIST_KEY
            , filename, (size_t) nameLength
            , cname, containerId
            , cname, containerId
            , cname, containerId
            , cname, containerId
        );
        success = r != NULL && r->type == REDIS_REPLY_INTEGER && r->integer > 0; 
    } else {
        // update the file journal if the fields already exist
        std::string script = 
            "local e1 = redis.call('HEXISTS', KEYS[1], ARGV[1]); \
            local e2 = redis.call('HEXISTS', KEYS[1], ARGV[2]); \
            if e1 == 1 and e2 == 1 then \
                return redis.call('HMSET', KEYS[1], ARGV[1], ARGV[3], ARGV[2], ARGV[4]); \
            end \
            return "";"
        ;
        r = (redisReply*) redisCommand(
            _cxt
            , "EVAL %s 1 %b %s-op-%d %s-status-%d %s %s"
            , script.c_str()
            , key, (size_t) keyLength
            , cname, containerId
            , cname, containerId
            , opType, status
        );
        success = r != NULL && (r->type == REDIS_REPLY_STRING || r->type == REDIS_REPLY_STATUS) && strncmp(r->str, "OK", r->len) == 0;
        LOG(INFO) << "Update journal using script = " << script << ", reply = " << r->str;
    }

    if (!success) {
        freeReplyObject(r);
        LOG(ERROR) << "Failed to " << (deleteRecord? "delete" : "update" ) << " the journal record of chunk " << chunk.getChunkId() << " of file " << file.name << " with namespace " << (int) file.namespaceId << " version " << file.version << " in container " << containerId;
        if (r == NULL) redisReconnect(_cxt);
        return false;
    }

    freeReplyObject(r);
    return true;
}

void RedisMetaStore::getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records) {
    std::lock_guard<std::mutex> lk(_lock);

    char key[MAX_KEY_SIZE];
    int keyLength = genFileJournalKey(file.namespaceId, file.name, file.nameLength, file.version, key);

    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "HGETALL %b"
        , key, (size_t) keyLength
    );
    if (r == NULL) {
        LOG(ERROR) << "Failed to get the journal of file " << file.name << " in namespace " << (int) file.namespaceId;
        redisReconnect(_cxt);
        return;
    }

    DLOG(INFO) << "File " << file.name << " version " << file.version << " in namespace " << file.namespaceId << " number of chunk journal records = " << r->elements << ".";

    std::map<std::pair<int /* chunk id */, int /* container id */>, int> chunk2listIndex;
    int lastIndex = 0;

    int chunkId = INVALID_CHUNK_ID, containerId = INVALID_CONTAINER_ID;
    std::string type;
    for (size_t i = 0; i < r->elements; i++) {
        if (i % 2 == 0) { // field key
            // must be a string
            if (r->element[i]->type != REDIS_REPLY_STRING) {
                continue;
            }

            // extract the chunk id, field type, and container id
            std::tie(chunkId, type, containerId) = extractJournalFieldKeyParts(r->element[i]->str, r->element[i]->len);
            if (chunkId == INVALID_CHUNK_ID || type.empty() || containerId == INVALID_CONTAINER_ID) {
                continue;
            }

            // allocate a new slot if the chunk is new
            auto chunkKey = std::make_pair(chunkId, containerId);
            if (chunk2listIndex.count(chunkKey) == 0) {
                chunk2listIndex.insert(std::make_pair(std::make_pair(chunkId, containerId), lastIndex++));
                records.resize(chunk2listIndex.size());
                // set chunk id and container id
                auto &rec = records[lastIndex-1];
                std::get<0>(rec).setChunkId(chunkId);
                std::get<1>(rec) = containerId;
            }
        } else if (chunkId !=INVALID_CHUNK_ID && containerId != INVALID_CONTAINER_ID && !type.empty()) {
            // find the record in the list and set the corresponding field value
            auto chunkKey = std::make_pair(chunkId, containerId);
            auto listIndexIt = chunk2listIndex.find(chunkKey);
            if (listIndexIt != chunk2listIndex.end()) {
                auto &record = records.at(listIndexIt->second);
                if (type.compare("md5") == 0) {
                    memcpy(std::get<0>(record).md5, r->element[i]->str, MD5_DIGEST_LENGTH);
                } else if (type.compare("size") == 0) {
                    memcpy(&std::get<0>(record).size, r->element[i]->str, sizeof(int));
                } else if (type.compare("status") == 0) { // whether the record is pre-operation
                    std::get<3>(record) = strncmp(r->element[i]->str,"pre", 3) == 0;
                } else if (type.compare("op") == 0) { // whether the operation is a write
                    std::get<2>(record) = strncmp(r->element[i]->str,"w", 1) == 0;
                }
            }

            // reset the parts
            chunkId = INVALID_CHUNK_ID;
            containerId = INVALID_CONTAINER_ID;
            type.clear();
        }
    }
    
    freeReplyObject(r);
}

int RedisMetaStore::getFilesWithJounal(FileInfo **list) {
    std::lock_guard<std::mutex> lk(_lock);

    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "SMEMBERS %s"
        , JL_LIST_KEY
    );

    if (r == NULL || r->type != REDIS_REPLY_ARRAY) {
        if (r == NULL) redisReconnect(_cxt);
        LOG(ERROR) << "Failed to get the list of files with journals, r = " << (void*) r << " reply type = " << (int)(r? r->type : -1) << ".";
        freeReplyObject(r);
        return -1;
    }

    // early return for an empty list
    if (r->elements == 0)
        return 0;

    // parse the list
    int numFiles = 0;
    *list = new FileInfo[r->elements];

    for (size_t i = 0; i < r->elements; i++) {
        FileInfo *info = &(*list)[numFiles];
        if (r->element[i]->type != REDIS_REPLY_STRING)
            continue;
        if (!getNameFromFileKey(r->element[i]->str, r->element[i]->len, &info->name, info->nameLength, info->namespaceId, &info->version))
            continue;
        numFiles++;
    }

    freeReplyObject(r);

    return numFiles;
}

bool RedisMetaStore::fileHasJournal(const File &file) {
    std::lock_guard<std::mutex> lk(_lock);

    char filename[PATH_MAX];
    int nameLength = genVersionedFileKey(file.namespaceId, file.name, file.nameLength, file.version, filename);

    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "SISMEMBER %s %b"
        , JL_LIST_KEY
        , filename, (size_t) nameLength
    );

    if (r == NULL) { 
        redisReconnect(_cxt);
        return false;
    }

    bool exists = r->type == REDIS_REPLY_INTEGER && r->integer == 1;

    freeReplyObject(r);

    return exists;
}

int RedisMetaStore::genFileKey(unsigned char namespaceId, const char *name, int nameLength, char key[]) {

    return snprintf(key, PATH_MAX, "%d_%*s", namespaceId, nameLength, name);
}

int RedisMetaStore::genVersionedFileKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[]) {
    return snprintf(key, PATH_MAX, "/%d_%*s\n%d", namespaceId, nameLength, name, version);
}

int RedisMetaStore::genFileVersionListKey(unsigned char namespaceId, const char *name, int nameLength, char key[]) {
    return snprintf(key, PATH_MAX, "//vl%d_%*s", namespaceId, nameLength, name);
}

bool RedisMetaStore::genFileUuidKey(unsigned char namespaceId, boost::uuids::uuid uuid, char key[]) {
    return snprintf(key, MAX_KEY_SIZE + 64, "//fu%d-%s", namespaceId, boost::uuids::to_string(uuid).c_str()) <= MAX_KEY_SIZE;
} 

int RedisMetaStore::genChunkKeyPrefix(int chunkId, char prefix[]) {
    return snprintf(prefix, MAX_KEY_SIZE, "c%d", chunkId);
}

int RedisMetaStore::genFileJournalKeyPrefix(char key[], unsigned char namespaceId) {
    if (namespaceId == 0) {
        return snprintf(key, MAX_KEY_SIZE, "//jl");
    }
    return snprintf(key, MAX_KEY_SIZE, "//jl_%d", namespaceId);
}

int RedisMetaStore::genFileJournalKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[]) {
    int prefixLength = genFileJournalKeyPrefix(key, namespaceId);
    return snprintf(key + prefixLength, MAX_KEY_SIZE - prefixLength, "_%*s_%d", nameLength, name, version) + prefixLength;
}

const char *RedisMetaStore::getBlockKeyPrefix(bool unique) {
    return unique? "ub" : "db";
}

int RedisMetaStore::genBlockKey(int blockId, char prefix[], bool unique) {
    return snprintf(prefix, MAX_KEY_SIZE, "%s%d", getBlockKeyPrefix(unique), blockId);
}

bool RedisMetaStore::getNameFromFileKey(const char *str, size_t len, char **name, int &nameLength, unsigned char &namespaceId, int *version) {
    // full name in form of "namespaceId_filename"
    int ofs = isVersionedFileKey(str)? 1 : 0;
    std::string fullname(str + ofs, len - ofs);
    size_t dpos = fullname.find_first_of("_");
    if (dpos == std::string::npos)
        return false;
    size_t epos = fullname.find_first_of("\n");
    if (epos == std::string::npos) {
        epos = len;
    } else if (version) {
        *version = atoi(str + ofs + epos + 1);
    }

    // fill in the namespace id, file name length and file name
    std::string namespaceIdStr(fullname, 0, dpos);
    namespaceId = strtol(namespaceIdStr.c_str(), NULL, 10) % 256;
    *name = (char *) malloc (epos - dpos);
    nameLength = epos - dpos - 1;
    memcpy(*name, str + ofs + dpos + 1, nameLength);
    (*name)[nameLength] = 0;

    return true;
}


bool RedisMetaStore::getFileName(char name[], File &f) {
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "GET %s",
        name
    );
    bool success = !(r == NULL || r->type != REDIS_REPLY_STRING);
    if (!success) {
        LOG(ERROR) << "Failed to get file name of " << name;
    } else {
        f.nameLength = r->len;
        f.name = (char *) malloc (r->len + 1);
        strncpy(f.name, r->str, r->len);
        f.name[r->len] = 0;
    }
    if (r == NULL) {
        redisReconnect(_cxt);
    }
    freeReplyObject(r);
    r = 0;
    return success;
}

bool RedisMetaStore::isSystemKey(const char *key) {
    return (
        strncmp("//", key, 2) == 0 ||
        false
    );
}

bool RedisMetaStore::isVersionedFileKey(const char *key) {
    return (
        strncmp("/", key, 1) == 0 ||
        false
    );
}

std::string RedisMetaStore::getFilePrefix(const char name[], bool noEndingSlash) {
    const char *slash = strrchr(name, '/'), *us = strchr(name, '_');
    std::string prefix("//pf_");
    // file on root directory, or root directory (ends with one '/')
    if (slash == NULL || us + 1 == slash) {
        prefix.append(name, us - name + 1);
        return noEndingSlash? prefix : prefix.append("/");
    }
    // sub-directory
    return prefix.append(name, slash - name);
}

bool RedisMetaStore::getLockOnFile(const File &file, bool lock) {
    return lockFile(file, lock, FILE_LOCK_KEY, "lock");
}

bool RedisMetaStore::pinStagedFile(const File &file, bool lock) {
    return lockFile(file, lock, FILE_PIN_STAGED_KEY, "pin");
}

bool RedisMetaStore::lockFile(const File &file, bool lock, const char *type, const char *name) {
    char filename[PATH_MAX];
    int nameLength = genFileKey(file.namespaceId, file.name, file.nameLength, filename);
    redisReply *r = (redisReply *) redisCommand(
        _cxt,
        "%s %s %b",
        lock? "SADD" : "SREM",
        type,
        filename, (size_t) nameLength
    );

    bool ret = r != NULL && r->type == REDIS_REPLY_INTEGER && r->integer == 1;
    if (!ret) {
        LOG(ERROR) << "Failed to " << (lock? "" : "un") << name << " file " << file.name << ", " << (r != NULL ? (r->type == REDIS_REPLY_INTEGER? "repeated operation" : "reply is invalid") : "failed to get reply"); 
        if (r == NULL) {
            redisReconnect(_cxt);
        }
    }

    freeReplyObject(r);
    r = 0;
    return ret;
}

