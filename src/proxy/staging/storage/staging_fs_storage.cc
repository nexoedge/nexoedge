// SPDX-License-Identifier: Apache-2.0

#include <glog/logging.h>

#include "staging_fs_storage.hh"
#include "../../../common/config.hh"
#include "../../../common/benchmark/benchmark.hh"

/* Staging file name */ 
#define OLD_FILE_TIME_MAX_LEN (unsigned char)(128)
#define OLD_FILE_EXT ".ncloudstaged"
#define STAGED_EXT "_staged_"
#define READ_CACHE_EXT "_readcache_"
#define PIN_EXT "_pin_"
#define STAGING_TAG "<STAGING> "

StagingFsStorage::StagingFsStorage() {
    // config
    Config &config = Config::getInstance();
    _url = config.getProxyStagingStorageURL();

    // write file lock buffer (uncomment the second line instead if needed)
    _req2SWFLBufferMap = 0;
    //_req2SWFLBufferMap = new std::map<int, SWriteFLockBuffer *>();
}

StagingFsStorage::~StagingFsStorage() {
    // write file lock buffer
    delete _req2SWFLBufferMap;
}

std::string StagingFsStorage::parseName(const File &in) {
    return parseName(in.name, in.nameLength);
}

std::string StagingFsStorage::parseName(const FileInfo &in) {
    return parseName(in.name, in.nameLength);
}

std::string StagingFsStorage::parseName(const char *fname, int length) {
    std::string name (fname, length);
    size_t pos = name.find("/");
    while (pos != std::string::npos) {
        name.at(pos) = '\n';
        pos = name.find("/", pos + 1);
    }
    return name;
}

int StagingFsStorage::getStagedFilename(const File &in, char *out) {
    std::string name = parseName(in);
    return snprintf(out, PATH_MAX, "%s/%s%s%s", _url.c_str(), std::to_string(in.namespaceId).c_str(), STAGED_EXT, name.c_str());
}

int StagingFsStorage::getStagedFilename(const FileInfo &in, char *out) {
    std::string name = parseName(in);
    return snprintf(out, PATH_MAX, "%s/%s%s%s", _url.c_str(), std::to_string(in.namespaceId).c_str(), STAGED_EXT, name.c_str());
}

int StagingFsStorage::getReadCacheFilename(const File &in, char *out) {
    std::string name = parseName(in);
    return snprintf(out, PATH_MAX, "%s/%s%s%s", _url.c_str(), std::to_string(in.namespaceId).c_str(), READ_CACHE_EXT, name.c_str());
}

int StagingFsStorage::getPinFilename(const File &in, char *out) {
    std::string name = parseName(in);
    return snprintf(out, PATH_MAX, "%s/%s%s%s", _url.c_str(), std::to_string(in.namespaceId).c_str(), PIN_EXT, name.c_str());
}

int StagingFsStorage::isStagedFile(const struct dirent *d) {
    const char *pattern = "[0-9]+_staged_";
    return fnmatch(pattern, d->d_name, FNM_FILE_NAME);
}

int StagingFsStorage::cleanIdleFiles(time_t idleTime) {
    int numFilesCleaned = 0;

    struct dirent **list = 0;
    int numFiles = 0;

    numFiles = scandir(_url.c_str(), &list, /* filename matching */ isStagedFile, /* sorting */ NULL);
    if (numFiles < 0) {
        LOG(ERROR) << STAGING_TAG << "Failed to scan directory for idle files, " << strerror(errno);
        return numFilesCleaned;
    }

    while (numFiles--) {
        char fpath[PATH_MAX];
        char *name = list[numFiles]->d_name;
        snprintf(fpath, PATH_MAX, "%s/%s", _url.c_str(), name);

        // skip special entries
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
            continue;
        // check if the file has expiry if not expire immdediately
        if (idleTime > 0) {
            struct stat sbuf;
            if (stat(fpath, &sbuf) != 0)
                continue;
            // skip if not expired yet
            if (sbuf.st_atim.tv_sec + idleTime > time(NULL))
                continue;
        }
        // figure out the namespace id and file name
        char *nepos = strchr(name, '_');
        if (nepos == 0)
            continue;
        char *tepos = strchr(nepos + 1, '_');
        if (tepos == 0)
            continue;
        // construct the internal file structure
        File f;
        *nepos = '\0';
        f.namespaceId = atoi(name);
        f.name = tepos + 1;
        f.nameLength = strlen(tepos + 1);
        // avoid file pinning while deleting
        std::lock_guard<std::mutex> lk(_pinFileLock);
        numFilesCleaned += !isFilePinned(f, /* needsLock */ false) && deleteFile(f);
        // avoid double free
        f.name = 0;
    }

    free(list);
    
    return numFilesCleaned;
}

bool StagingFsStorage::openFile(const File &f) {
    char fpath[PATH_MAX];
    getStagedFilename(f, fpath);

    // check and return if lock exists
    std::lock_guard<std::mutex> lk(_fileLock.lock);
    if (_fileLock.map.find(fpath) != _fileLock.map.end())
        return true;

    // if no lock exists, new one
    bool success = false;
    std::tie(std::ignore, success) = _fileLock.map.insert(std::make_pair(std::string(fpath), new std::mutex()));

    return success;
}

bool StagingFsStorage::closeFile(const File &f) {
    char fpath[PATH_MAX];
    getStagedFilename(f, fpath);

    // check and return false if lock does not exist
    std::lock_guard<std::mutex> lk(_fileLock.lock);
    auto it = _fileLock.map.find(fpath);
    if (it == _fileLock.map.end())
        return false;

    delete it->second;
    _fileLock.map.erase(it);

    return true;
}

bool StagingFsStorage::writeFile(const File &f, bool isReadFromAgents, bool isTruncated) {

    char fpath[PATH_MAX];
    if (!isReadFromAgents) {
        getStagedFilename(f, fpath);
    } else {
        getReadCacheFilename(f, fpath);
    }
    
    DLOG(INFO) << STAGING_TAG << "Start to write to Staging storage, source: " << (isReadFromAgents ? "Agents" : "Client") 
            << ", filename: " << fpath << ", size: " << f.size << ", offset: " << f.offset << ", length: " << f.length;

    // rename the existing file
    std::string ofpath(fpath);

    // open the file for overwrite
    FILE *file = NULL;

    _fileLock.lock.lock();
    char lpath[PATH_MAX];
    getStagedFilename(f, lpath);
    auto fileLock = _fileLock.map.find(lpath);
    if (fileLock != _fileLock.map.end()) {
        fileLock->second->lock();
    }
    _fileLock.lock.unlock();

    if (!boost::filesystem::is_regular_file(ofpath)) {
        // file don't exist, create and open it
        file = fopen(fpath, "w");
    } else {
        // staged file exists, and it's not the READ_CACHE
        // Note: the order of stripes from READ_CACHE is random, so the file has to be open with "r+"
        if (isReadFromAgents) {
            file = fopen(fpath, "r+");
        } else {
            if (f.offset == 0 && isTruncated) {
                // backup/delete the old file
                char currentTime[OLD_FILE_TIME_MAX_LEN];
                snprintf(currentTime, OLD_FILE_TIME_MAX_LEN, "%ld", time(NULL));
                getOldFilePath(ofpath, fpath, currentTime);
                if (rename(fpath, ofpath.c_str()) != 0) {
                    if (fileLock != _fileLock.map.end()) {
                        fileLock->second->unlock();
                    }
                    LOG(ERROR) << STAGING_TAG << "<Failed to backup file " << f.name << " to " << ofpath << " before write";
                    return false;
                }
                if (Config::getInstance().overwriteFiles()) {
                    DLOG(INFO) << "<STAGING> overwrite file: " << f.name << ", deleted filename: " << ofpath;
                    remove(ofpath.c_str());
                }
                // open fpath as a new file
                file = fopen(fpath, "w");
            } else {
                // file exists and continue to modify it
                file = fopen(fpath, "r+");
            }
        }
    }

    if (file == NULL) {
        if (fileLock != _fileLock.map.end()) {
            fileLock->second->unlock();
        }
        return false;
    }

    // lock the file before writing
    flock(fileno(file), LOCK_EX);
    fseek(file, f.offset, SEEK_SET);

    /***************************************** NOTE:  *****************************************/
    /*********************** Uncomment to use grouped flock/fseek *****************************/

    // initSWFLockBuffer(f, isReadFromAgents);
    // SWriteFLockBuffer *buffer = _req2SWFLBufferMap->at(f.reqId);
    // if (buffer == NULL) {
    //     LOG(ERROR) << "<STAGING> Failed to find FILE *, filename: " << f.name << ", reqId: " << f.reqId;
    //     cleanSWFLockBuffer(f);
    //     return false;
    // }
    // // open the file for overwrite
    // FILE *file = buffer->file;
    // fseek(file, f.offset, SEEK_SET);

    /******************************** NOTE END ***********************************************/

    // TagPt(start): fwrite
    TagPt fwrite_time;
    fwrite_time.markStart();

    unsigned long int written = 0;
    while (written < f.length) {
        size_t ret = fwrite(f.data + written, 1, f.length - written, file);
        if (ret < 0) {
            if (fileLock != _fileLock.map.end()) {
                fileLock->second->unlock();
            }
            //cleanSWFLockBuffer(f);
            return false;
        }
        written += ret;
    }

    // // TagPt(end): fwrite
    // fwrite_time.markEnd();
    // double fileSizeMB = (f.length * 1.0 / (1 << 20));
    // DLOG(INFO) << "<STAGING> Write file (fwrite), size: " << fileSizeMB << " MB, "
    //     << "time: " << fwrite_time.usedTime() << "s, "
    //     << "speed: " << (fileSizeMB / (fwrite_time.usedTime() + 1e-7)) << "MB/s, "
    //     << "startTime: " << fwrite_time.getStart() << ", endTime: " << fwrite_time.getEnd();

    /*********** NOTE: if not call fsync, the performance will be much better ****************/
    /*********** Comment the following lines makes the speed much faster ****************/

    // // make sure the file has been written to disk
    // fflush(file);
    // fsync(fileno(file));

    /******************************** NOTE END ***********************************************/

    
    /******************************** NOTE  ***************************************************/
    /*********************** Uncomment to use grouped flock/fseek *****************************/
    
    // updateSWFLockBuffer(f);
    // cleanSWFLockBuffer(f);

    /******************************** NOTE END ***********************************************/
    
    // TagPt(end): fwrite
    fwrite_time.markEnd();
    double fileSizeMB = (f.length * 1.0 / (1 << 20));
    DLOG(INFO) << "<STAGING> Write file (fwrite), size: " << fileSizeMB << " MB, "
        << "time: " << fwrite_time.usedTime() << "s, "
        << "speed: " << (fileSizeMB / (fwrite_time.usedTime() + 1e-7)) << "MB/s, "
        << "startTime: " << fwrite_time.getStart() << ", endTime: " << fwrite_time.getEnd();

    // unlock file after write
    flock(fileno(file), LOCK_UN);
    fclose(file);
    
    if (fileLock != _fileLock.map.end()) {
        fileLock->second->unlock();
    }
    // show current file size
    // struct stat sbuf;
    // lstat(fpath, &sbuf);

    // DLOG(INFO) << "<STAGING> Complete write to Staging Storage, " 
    //         << "current file (" << fpath << ") size: " << sbuf.st_size << " bytes"
    //         << ", filename: " << fpath << ", size: " << f.size 
    //         << ", offset: " << f.offset << ", length: " << f.length;

    return true;
}

unsigned long int StagingFsStorage::readFile(const File &f) {
    char fpath[PATH_MAX];
    getStagedFilename(f, fpath);

    DLOG(INFO) << "<STAGING> Start to read from Staging Storage, " 
            << "filename: " << fpath << ", size: " << f.size << ", offset: " << f.offset << ", length: " << f.length;

    // open the file for overwrite
    FILE *file = NULL;

    file = fopen(fpath, "r");

    if (file == NULL) {
        DLOG(INFO) << "<STAGING> Cannot find file in Staging Storage, filename: " << fpath;
        return INVALID_FILE_LENGTH;
    }

    // lock file for read
    flock(fileno(file), LOCK_SH);

    // TODO: verify file size checking is needed
    // unsigned long int fsize = 0;
    // fseek(file, 0, SEEK_END);
    // fsize = ftell(file);
    // rewind(file);
    // if (f.size != fsize) {
    //     LOG(INFO) << "<STAGING> Mismatch file size (file size in Staging Storage, file size in metadata), filename: " << fpath;
    //     return false;
    // }

    unsigned long int read = 0;
    // offset the file
    fseek(file, f.offset, SEEK_SET);

    const unsigned long int blockSize = 4 << 20; // 4MB

    size_t ret = 1;
    while (ret != 0) {
        size_t unit = f.length - read >= blockSize? blockSize : 1;
        ret = fread(f.data + read, unit, (f.length - read) / unit, file);
        if (ret < 0) {
            flock(fileno(file), LOCK_UN);
            fclose(file);
            return INVALID_FILE_LENGTH;
        }
        read += ret * unit;
    }

    // unlock file after read
    flock(fileno(file), LOCK_UN);
    fclose(file);

     // return current file size
    // struct stat sbuf;
    // lstat(fpath, &sbuf);
    // DLOG(INFO) << "<STAGING> Current file (" << fpath << ") size: " << sbuf.st_size << " bytes";

    // DLOG(INFO) << "<STAGING> Complete read from Staging, " 
    //         << "filename: " << fpath << ", size: " << f.size << ", offset: " << f.offset << ", length: " << f.length;

    return read;

}

bool StagingFsStorage::deleteFile(const File &f) {
    
    char fpath[PATH_MAX];

    // delete old version of files
    std::string wildcard(f.name);

    // pattern: <namespaceId>_<filename>_<time>.<OLD_FILE_EXT>
    std::string pattern = std::to_string(f.namespaceId).c_str();
    pattern += "_";
    pattern += f.name;
    pattern += "_[0-9]*";
    pattern += OLD_FILE_EXT;

    DIR *dir;
    struct dirent *ptr;
    if ((dir = opendir(_url.c_str())) == NULL){
        DLOG(INFO) << "<STAGING> Cannot open Staging Storage, folder: " << _url;
        return false;
    }

    while ((ptr = readdir(dir)) != NULL) {
        // delete old staged files
        if (!fnmatch(pattern.c_str(), std::string(ptr->d_name).c_str(), FNM_PATHNAME)) {
            snprintf(fpath, PATH_MAX, "%s/%s", _url.c_str(), ptr->d_name);
            DLOG(INFO) << "<STAGING> Found and delete file: " << fpath;
            if (access(fpath, F_OK) != 0) {
                // the file not exists
                continue;
            }
            if (remove(fpath) != 0) {
                LOG(ERROR) << "<STAGING> Error deleting the old version file from Staging storage, filename: " << fpath;
            }
        }
    }

    closedir(dir);

    // delete read cache
    getReadCacheFilename(f, fpath);
    if (access(fpath, F_OK) == 0) {
        if (remove(fpath) != 0) {
            LOG(ERROR) << "<STAGING> Error deleting the read cache file from Staging storage, filename: " << fpath;
            return false;
        }
    }

    // delete pin file
    getPinFilename(f, fpath);
    if (access(fpath, F_OK) == 0) {
        if (remove(fpath) != 0) {
            LOG(WARNING) << "<STAGING> Error deleting pin file from Staging storage, filename: " << fpath;
        }
    }

    // delete file
    getStagedFilename(f, fpath);
    if (access(fpath, F_OK) != 0) {
        // the file not exists
        return true;
    } else {
        LOG(INFO) << "<STAGING> deleting the file from Staging storage, filename: " << fpath;
    }

    if (remove(fpath) != 0) {
        LOG(ERROR) << "<STAGING> Error deleting the file from Staging storage, filename: " << fpath;
        return false;
    }

    return true;
}

bool StagingFsStorage::commitReadCacheFile(const File &f) {
    std::lock_guard<std::mutex> lk(_pinFileLock);
    char fpath[PATH_MAX], rcfpath[PATH_MAX];
    getStagedFilename(f, fpath);
    getReadCacheFilename(f, rcfpath);

    // only move read cache as staged if not pinned for user write
    return !isFilePinned(f, /* needsLock */ false) && rename(rcfpath, fpath) == 0;
}

bool StagingFsStorage::discardReadCacheFile(const File &f) {
    char rcfpath[PATH_MAX];
    getReadCacheFilename(f, rcfpath);

    return unlink(rcfpath) == 0;
}

bool StagingFsStorage::pinFile(const File &f) {
    std::lock_guard<std::mutex> lk(_pinFileLock);
    char pfpath[PATH_MAX];
    getPinFilename(f, pfpath);

    // try creating lock file
    int fd = 0;
    fd = open(pfpath, O_CREAT | O_RDWR, 0644);

    // check if pin file can be created
    if (fd <= 0) {
        LOG(ERROR) << "<STAGING> Failed to create pin file for file " << f.name << ", " << strerror(errno);
        return false;
    }
        
    flock(fd, LOCK_UN);
    close(fd);

    return true;
}

bool StagingFsStorage::unpinFile(const File &f) {
    std::lock_guard<std::mutex> lk(_pinFileLock);
    char pfpath[PATH_MAX];
    getPinFilename(f, pfpath);
    return unlink(pfpath) == 0;
}

bool StagingFsStorage::isFilePinned(const File &f, bool needsLock) {
    if (needsLock)
        _pinFileLock.lock();

    char pfpath[PATH_MAX];
    getPinFilename(f, pfpath);

    struct stat sbuf;

    bool pinned = stat(pfpath, &sbuf) == 0;

    if (needsLock)
        _pinFileLock.unlock();

    return pinned;
}

bool StagingFsStorage::getFileInfo(FileInfo &info) {
    char fpath[PATH_MAX];
    getStagedFilename(info, fpath);
    DLOG(INFO) << "Staged file " << fpath;
    
    struct stat sbuf;

    bool exists = stat(fpath, &sbuf) == 0;

    if (exists) {
        info.atime = sbuf.st_atime;
        info.mtime = sbuf.st_mtime;
        info.ctime = sbuf.st_ctime;
        info.size = sbuf.st_size;
        DLOG(INFO) << "Staged file " << info.name << " ctime = " << info.ctime << " mtime = " << info.mtime << " atime = " << info.atime << " size = " << info.size << " time now = " << time(NULL);
    }

    return exists;
}

void StagingFsStorage::getOldFilePath(std::string &ofpath, const char *fpath, const char *ctime) {
    ofpath.clear();
    ofpath = fpath;
    ofpath += "_";
    ofpath += ctime;
    ofpath += OLD_FILE_EXT;
}

bool StagingFsStorage::initSWFLockBuffer(const File &f, bool isReadFromAgents) {

    // search reqId in Map
    std::map<int, SWriteFLockBuffer *>::iterator it = _req2SWFLBufferMap->find(f.reqId);
    if (it != _req2SWFLBufferMap->end()) {
        // DLOG(INFO) << "<STAGING> Found existing MetaBuffer" << " ( reqId: " << f.reqId << ", name: " << f.name << ")";
        return true;
    }

    SWriteFLockBuffer *buffer = new SWriteFLockBuffer(); // create buffer
    std::lock_guard<std::mutex> lk(buffer->_lock);

    // file path in Staging storage
    char fpath[PATH_MAX];
    if (!isReadFromAgents) {
        snprintf(fpath, PATH_MAX, "%s/%s_%s", _url.c_str(), std::to_string(f.namespaceId).c_str(), f.name);
    } else {
        snprintf(fpath, PATH_MAX, "%s/%s_%s%s", _url.c_str(), std::to_string(f.namespaceId).c_str(), f.name, READ_CACHE_EXT);
    }
    
    // DLOG(INFO) << "<STAGING> Start to write to Staging storage, source: " << (isReadFromAgents ? "Agents" : "Client") 
    //         << ", filename: " << fpath << ", size: " << f.size << ", offset: " << f.offset << ", length: " << f.length;

    // rename the existing file
    std::string ofpath(fpath);

    // open the file for overwrite
    FILE *file = NULL;

    if (!boost::filesystem::is_regular_file(ofpath)) {
        // file don't exist, create and open it
        file = fopen(fpath, "w");
    } else {
        // staged file exists, and it's not the READ_CACHE
        // Note: the order of stripes from READ_CACHE is random, so the file has to be open with "r+"
        if (isReadFromAgents) {
            file = fopen(fpath, "r+");
        } else {
            // backup/delete the old file
            char currentTime[OLD_FILE_TIME_MAX_LEN];
            snprintf(currentTime, OLD_FILE_TIME_MAX_LEN, "%ld", time(NULL));
            getOldFilePath(ofpath, fpath, currentTime);
            if (rename(fpath, ofpath.c_str()) != 0) {
                LOG(ERROR) << "<STAGING> Failed to backup file " << f.name << " to " << ofpath << " before write";
                return false;
            }
            if (Config::getInstance().overwriteFiles()) {
                DLOG(INFO) << "<STAGING> overwrite file: " << f.name << ", deleted filename: " << ofpath;
                remove(ofpath.c_str());
            }
            // open fpath as a new file
            file = fopen(fpath, "w");
        }
    }

    if (file == NULL) {
        return false;
    }

    // lock the file before writing
    flock(fileno(file), LOCK_EX);

    buffer->file = file;
    buffer->numWrittenStripes = 0;
    _req2SWFLBufferMap->insert(std::pair<int, SWriteFLockBuffer *>(f.reqId, buffer));
    DLOG(INFO) << "<STAGING> Init MetaBuffer ( reqId: " << f.reqId << ", filename: " << f.name << ")";

    return true;
}

bool StagingFsStorage::updateSWFLockBuffer(const File &f) {
    // search reqId in Map
    std::map<int, SWriteFLockBuffer *>::iterator it = _req2SWFLBufferMap->find(f.reqId);
    if (it == _req2SWFLBufferMap->end()) {
        // DLOG(INFO) << "<STAGING> MetaBuffer not exists" << " ( reqId: " << f.reqId << ", name: " << f.name << ")";
        return false;
    }

    SWriteFLockBuffer *buffer = it->second;
    std::lock_guard<std::mutex> lk(buffer->_lock);

    buffer->numWrittenStripes += 1;

    // fflush and fsync
    fflush(buffer->file);
    fsync(fileno(buffer->file));

    return true;
}

void StagingFsStorage::cleanSWFLockBuffer(const File &f) {

    // search reqId in Map
    std::map<int, SWriteFLockBuffer *>::iterator it = _req2SWFLBufferMap->find(f.reqId);
    if (it == _req2SWFLBufferMap->end()) {
        // DLOG(INFO) << "<STAGING> MetaBuffer not exists" << " ( reqId: " << f.reqId << ", name: " << f.name << ")";
        return;
    }

    SWriteFLockBuffer *buffer = it->second;
    FILE *file = buffer->file;
    std::lock_guard<std::mutex> lk(buffer->_lock);

    // check if file write finished
    if (buffer->numWrittenStripes != f.numStripes) {
        return;
    }

    // fsync
    // fsync(fileno(buffer->file));

    // unlock file after write
    flock(fileno(file), LOCK_UN);
    fclose(file);

    // remove buffer
    delete buffer;
    _req2SWFLBufferMap->erase(it); // remove in Map
    DLOG(INFO) << "<STAGING> Clean SWriteFLockBuffer success";

    return;
}

