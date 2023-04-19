// SPDX-License-Identifier: Apache-2.0

#include "file.hh"
#include <glog/logging.h>

File::File() {
    reset();
}

File::~File() {
    free(name);
    releaseData();
    delete [] containerIds;
    delete [] chunks;
    delete [] chunksCorrupted;
}

void File::releaseData() {
    if (mmapped) {
        munmap(data, size);
    } else {
        free(data);
    }
    data = 0;
}

bool File::setName(const char *fn, const int nlen) {
    // check if length is greater than 0, but less than path_max
    if (nlen <= 0 || nlen > PATH_MAX) {
        DLOG(ERROR) << "Failed to set file name with length " << nlen;
        return false;
    }
    nameLength = nlen;

    // null-terminated file name
    name = (char *) malloc (nameLength + 1);
    if (name == 0) {
        LOG(ERROR) << "Failed to create buffer for file name with length " << nameLength;
        nameLength = 0;
        return false;
    }
    memcpy(name, fn, nameLength);
    name[nameLength] = 0;

    return true;
}

bool File::setUUID(std::string uuidstr) {
    boost::uuids::string_generator sgen;
    try {
        uuid = sgen(uuidstr);
    } catch (boost::exception &e) {
        return false;
    }
    return true;
}

void File::setTimeStamps(time_t ct, time_t mt, time_t at, time_t tct) {
    ctime = ct;
    atime = at;
    mtime = mt;
    tctime = tct;
}

bool File::setVersion(int ver) {
    version = ver;
    return true;
}

void File::setStagedInfo(unsigned long int fileSize, const CodingMeta cmeta, std::string sc, time_t stagedts) {
    staged.size = fileSize;
    staged.codingMeta.copyMeta(cmeta);
    staged.storageClass = sc;
    staged.mtime = stagedts;
}

bool File::copyName(const File &in, bool shadow) {
    if (shadow) {
        nameLength = in.nameLength;
        name = in.name;
        namespaceId = in.namespaceId;
        //uuid = in.uuid;
        genUUID();
        return true;
    }

    // name
    nameLength = in.nameLength;
    if (name != 0)
        free(name);
    name = (char *) malloc (nameLength+1);
    if (name == NULL)
        return false;
    memcpy(name, in.name, nameLength);
    name[nameLength] = 0;
    // namespace id
    namespaceId = in.namespaceId;
    // UUID
    //uuid = in.uuid;
    genUUID();

    return true;
}

bool File::copyName(const FileInfo &in, bool shadow) {
    if (!shadow) {
        free(name);
        name = (char *) malloc (in.nameLength + 1);
        if (name == NULL) {
            return false;
        }
        memcpy(name, in.name, in.nameLength);
        name[in.nameLength] = '\0';
    } else {
        name = in.name;
    }
    namespaceId = in.namespaceId;
    nameLength = in.nameLength;
    version = in.version;

    return true;
}

bool File::copyNameToInfo(FileInfo &out, bool shadow) {
    if (!shadow) {
        free(out.name);
        out.name = (char *) malloc (nameLength + 1);
        if (out.name == NULL) {
            return false;
        }
        memcpy(out.name, name, nameLength);
        out.name[nameLength] = '\0';
    } else {
        out.name = name;
    }
    out.namespaceId = namespaceId;
    out.nameLength = nameLength;
    out.version = version;

    return true;
}

void File::copySize(const File &in) {
    size = in.size;
}

void File::copyTimeStamps(const File &in) {
    ctime = in.ctime;
    atime = in.atime;
    mtime = in.mtime;
    tctime = in.tctime;
}

void File::copyFileChecksum(const File &in) {
    memcpy(md5, in.md5, MD5_DIGEST_LENGTH);
}

void File::copyVersionControlInfo(const File &in) {
    version = in.version;
}

void File::copyStoragePolicy(const File &in) {
    storageClass = in.storageClass;
    codingMeta.copyMeta(in.codingMeta, /* parameters only */ true);
}

void File::copyChunkInfo(const File &in, bool shadow) {
    numChunks = in.numChunks;
    if (shadow) {
        containerIds = in.containerIds;
        chunks = in.chunks;
        chunksCorrupted = in.chunksCorrupted;
    } else {
        initChunksAndContainerIds(numChunks);
        memcpy(containerIds, in.containerIds, sizeof(int) * numChunks);
        memcpy(chunksCorrupted, in.chunksCorrupted, sizeof(bool) * numChunks);
        for (int i = 0; i < numChunks; i++) {
            chunks[i].copyMeta(in.chunks[i]);
        }
    }
}

void File::copyStagedInfo(const File &in) {
    staged = in.staged;
}

void File::copyOperationDataRange(const File &in) {
    offset = in.offset;
    length = in.length;
}

void File::copyOperationBenchmarkInfo(const File &in) {
    blockId = in.blockId;
    stripeId = in.stripeId;
}

bool File::copyNameAndSize(const File &in) {
    copyName(in);
    copySize(in);
    copyTimeStamps(in);
    // request id
    reqId = in.reqId;
    // md5
    memcpy(md5, in.md5, MD5_DIGEST_LENGTH);
    return true;
}

bool File::copyAllMeta(File &in) {
    copyName(in);
    copySize(in);
    copyFileChecksum(in);
    copyTimeStamps(in);
    copyVersionControlInfo(in);
    copyChunkInfo(in);
    copyStoragePolicy(in);
    codingMeta.copyMeta(in.codingMeta);
    copyStagedInfo(in);

    copyOperationDataRange(in);
    return true;
}

bool File::initChunksAndContainerIds(int num) {
    // use the pre-set number of chunks in file
    if (num == -1) {
        num = numChunks;
    }
    // number of chunks must be positive
    if (num < 0)
        return false;
    try {
        // container ids
        if (containerIds)
            delete [] containerIds;
        containerIds = num > 0 ? new int[num] : 0;
        // chunks
        if (chunks)
            delete [] chunks;
        chunks = num > 0 ? new Chunk[num] : 0;
        // corrupted chunks markers
        if (chunksCorrupted)
            delete [] chunksCorrupted;
        chunksCorrupted = num > 0 ? new bool[num] : 0;
        memset(chunksCorrupted, 0, num);
        numChunks = num;
    } catch (std::bad_alloc &e) {
        delete [] containerIds;
        delete [] chunks;
        delete [] chunksCorrupted;
        containerIds = 0;
        chunks = 0;
        chunksCorrupted = 0;
        return false;
    }

    return true;
}

boost::uuids::uuid File::genUUID(const char *name) {
    boost::uuids::string_generator sgen;
    boost::uuids::uuid nuuid = sgen(FILE_DNS_UUID);
    boost::uuids::name_generator ngen(nuuid);
    return ngen(name);
}

void File::genUUID() {
    uuid = genUUID(name);
}

void File::resetStagingStoragePolicy() {
    staged.storageClass = "";
    staged.codingMeta.reset();
}

void File::reset() {
    namespaceId = INVALID_NAMESPACE_ID;
    uuid = boost::uuids::nil_uuid();
    name = 0;
    nameLength = 0;
    size = 0;
    version = -1;
    numStripes = 0;
    offset = INVALID_FILE_OFFSET;
    length = INVALID_FILE_LENGTH;
    data = 0;
    mmapped = false;
    numChunks = 0;
    containerIds = 0;
    chunksCorrupted = 0;
    chunks = 0;
    ctime = 0;
    atime = 0;
    mtime = 0;
    tctime = 0;
    status = FileStatus::NONE;
    memset(md5, 0, MD5_DIGEST_LENGTH);
    resetStagingStoragePolicy();

    reqId = -1;
    blockId = -1;
    stripeId = -1;
    isFinalStripe = false;

    staged.size = INVALID_FILE_OFFSET;
    staged.mtime = 0;
}

void File::print() {
    LOG(INFO) << "File summary: "
        << " name = " << name
        << " namespace id = " << (int) namespaceId
        << " version = " << version
        << " size = " << size
        << " num of chunks = " << numChunks 
        << " coding = " << (int) codingMeta.coding
        << " created = " << ctime
        << " last modified = " << mtime
        << " last accessed = " << atime
    ;
}
