// SPDX-License-Identifier: Apache-2.0

#include <stdio.h> // ftell(), rewind(), sprintf()
#include <string.h> // strlen()
#include <string>
#include <time.h>
#include <unistd.h>
#include <linux/limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/file.h> // flock()
#include <boost/filesystem.hpp>

#include <boost/timer/timer.hpp>

#include <glog/logging.h>

#include "../../common/config.hh"
#include "fs.hh"

FsContainer::FsContainer(int id, const char *dir, unsigned long int capacity) :
        Container(id, capacity) {
    strcpy(_dir, dir);
    // create the directory for chunk files
    mkdir(dir, 0755);
    updateUsage();

    _running = true;

    // background cleaning thread
    pthread_cond_init(&_chunkCleanUp.cond, NULL);
    pthread_mutex_init(&_chunkCleanUp.lock, NULL);
    pthread_create(&_chunkCleanUp.th, NULL, FsContainer::cleanUpOldChunks, (FsContainer *) this);
}

FsContainer::~FsContainer() {
    // signal the background cleaning thread to terminate now
    _running = false;
    pthread_cond_signal(&_chunkCleanUp.cond);
    pthread_join(_chunkCleanUp.th, NULL);
    pthread_cond_destroy(&_chunkCleanUp.cond);
    pthread_mutex_destroy(&_chunkCleanUp.lock);
}

bool FsContainer::getChunkPath(char *fpath, std::string chunkName) {
    return snprintf(fpath, PATH_MAX, "%s/%s", _dir, chunkName.c_str()) < PATH_MAX;
}

void FsContainer::getOldChunkPath(std::string &ofpath, char *fpath, const char *ctime) {
    ofpath.clear();
    ofpath = fpath;
    ofpath += ".";
    ofpath += ctime;
}

bool FsContainer::putChunk(Chunk &chunk) {
    char fpath[PATH_MAX];
    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    std::string ofpath(fpath);
    // backup the chunk first if exists
    if (boost::filesystem::is_regular_file(ofpath)) {
        // use the current time as the version of the previous chunk
        snprintf(chunk.chunkVersion, CHUNK_VERSION_MAX_LEN, "%ld", time(NULL));
        // generate the old chunk's path
        getOldChunkPath(ofpath, fpath, chunk.chunkVersion);
        // move chunk
        if (rename(fpath, ofpath.c_str()) != 0) {
            LOG(ERROR) << "Failed to backup chunk " << fpath << " to " << ofpath << " before write";
            return false;
        }
    } else {
        // no previous version found
        chunk.chunkVersion[0] = 0;
    }

    boost::timer::cpu_timer mytimer;

    // open (and truncate) the file for write
    FILE *chunkFile = fopen(fpath, "w");
    if (chunkFile == NULL) {
        return false;
    }

    // lock file for write
    flock(fileno(chunkFile), LOCK_EX);

    ssize_t written = 0;
    while (written < chunk.size) {
        ssize_t ret = 0;
        ret = fwrite(chunk.data + written, 1, chunk.size - written, chunkFile);
        if (ret <= 0) {
            LOG(ERROR) << "Failed to write chunk data " << chunk.getChunkName() << " error = " << strerror(errno);
            flock(fileno(chunkFile), LOCK_UN);
            fclose(chunkFile);
            return false;
        }
        written += ret;
    }

    if (Config::getInstance().getAgentFlushOnClose()) {
        fflush(chunkFile);
        fsync(fileno(chunkFile));
    }

    // benchmark
    double elapsed = mytimer.elapsed().wall * 1.0 / 1e9;
    DLOG(INFO) << "<WRITE> Write chunk, size: " << (chunk.size * 1.0 / (1 << 20)) << " MB, time: " << elapsed << " s, speed: " << (chunk.size * 1.0 / (1 << 20)) / elapsed << " MB/s";


    // unlock file after write
    flock(fileno(chunkFile), LOCK_UN);

    // close the chunk file
    fclose(chunkFile);

    // check if all chunk data is successfully written
    bool success = written == chunk.size;
    // read chunk data back to get checksum, and verify the checksum if needed
    Chunk readChunk;
    readChunk.copyMeta(chunk);
    success = success && (getChunkInternal(readChunk) || !Config::getInstance().verifyChunkChecksum());

    if (success) {
        readChunk.computeMD5();
        chunk.copyMD5(readChunk);
        elapsed = mytimer.elapsed().wall * 1.0 / 1e9;
        LOG(INFO) << "Put chunk " << chunk.getChunkName() << " to path " << fpath << " size " << (chunk.size * 1.0 / (1 << 20)) << " MB in " << elapsed << "s, " << (chunk.size * 1.0 / (1 << 20)) / elapsed << " MB/s";
    }

    return success;
}

bool FsContainer::getChunk(Chunk &chunk, bool skipVerification) {
    char fpath[PATH_MAX];
    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    bool success = getChunkInternal(chunk, skipVerification);
    LOG_IF(INFO, success) << "Get chunk " << chunk.getChunkName() << " from path " << fpath;
    return success;
}

bool FsContainer::getChunkInternal(Chunk &chunk, bool skipVerification) {
    char fpath[PATH_MAX];
    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    if (!readChunkFile(fpath, chunk)) {
        DLOG(WARNING) << "Failed to read chunk file";
        return false;
    }

    // verify checksum if needed
    return skipVerification || !Config::getInstance().verifyChunkChecksum() || chunk.verifyMD5();
}

bool FsContainer::readChunkFile(const char fpath[], Chunk &chunk) {
    FILE *chunkFile = fopen(fpath, "r");
    if (chunkFile == NULL) {
        LOG(ERROR) << "Failed to open chunk file " << fpath;
        return false;
    }

    boost::timer::cpu_timer mytimer;

    // lock file for read
    flock(fileno(chunkFile), LOCK_SH);

    // get chunk (file) size
    unsigned long int fsize = 0, read = 0;
    fseek(chunkFile, 0, SEEK_END);
    fsize = ftell(chunkFile);
    rewind(chunkFile);
    chunk.size = fsize;

    // get chunk (file) data
    chunk.data = (unsigned char*) malloc (fsize * sizeof(char));
    size_t ret = 1;
    while (ret != 0) {
        ret = fread(chunk.data + read, 1, fsize - read, chunkFile);
        if (ret < 0) {
            flock(fileno(chunkFile), LOCK_UN);
            fclose(chunkFile);
            return false;
        }
        read += ret;
    }

    // unlock file after read
    flock(fileno(chunkFile), LOCK_UN);

    fclose(chunkFile);

    double elapsed = mytimer.elapsed().wall * 1.0 / 1e9;
    LOG(INFO) << "Get chunk " << chunk.getChunkName() << " to path " << fpath << " size " << (chunk.size * 1.0 / (1 << 20)) << " MB in " << elapsed << "s, " << (chunk.size * 1.0 / (1 << 20)) / elapsed << " MB/s";

    return true;
}

bool FsContainer::deleteChunk(const Chunk &chunk) {
    char fpath[PATH_MAX];
    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    unlink(fpath);
    LOG(INFO) << "Delete chunk " << chunk.getChunkName() << " at path " << fpath;

    return true;
}

bool FsContainer::copyChunk(const Chunk &src, Chunk &dst) {
    char sfpath[PATH_MAX], dfpath[PATH_MAX];
    if (getChunkPath(sfpath, src.getChunkName()) == false)
        return false;
    if (getChunkPath(dfpath, dst.getChunkName()) == false)
        return false;
    
    unsigned long int copyBlockSize = Config::getInstance().getCopyBlockSize();
    char buffer[copyBlockSize];
    FILE *srcFile = fopen(sfpath, "r");
    FILE *dstFile = fopen(dfpath, "w");

    if (srcFile == NULL || dstFile == NULL)
        return false;

    // lock files for read/write
    flock(fileno(srcFile), LOCK_SH);
    flock(fileno(dstFile), LOCK_EX);

    size_t ret = 0;
    int size = 0;
    while (1) {
        ret = fread(buffer, 1, copyBlockSize, srcFile);
        if (ret <= 0)
            break;
        if (fwrite(buffer, 1, ret, dstFile) < ret) {
            LOG(ERROR) << "Failed to copy a file (not enough storage space?)";
            break;
        }
        size += ret;
    }

    // unlock files
    flock(fileno(srcFile), LOCK_UN);
    flock(fileno(dstFile), LOCK_UN);

    fclose(srcFile);
    fclose(dstFile);

    // check if the whole chuck is copied
    bool success = size == src.size;

    // always compute and copy the MD5 of the copied chunk, and verify the checksum if needed
    Chunk readChunk;
    readChunk.copyMeta(dst);
    success = success && (getChunkInternal(readChunk) || !Config::getInstance().verifyChunkChecksum());

    // remove newly copied chunk if (checksum verification) failed
    if (!success) {
        deleteChunk(dst);
    } else {
        // mark the size copied
        dst.size = size;
        // mark the md5 of the copied chunk
        readChunk.computeMD5();
        dst.copyMD5(readChunk);
        LOG(INFO) << "Copy chunk " << src.getChunkName() << " to " << dst.getChunkName() << " from path " << sfpath << " to path " << dfpath;
    }

    return success;
}

bool FsContainer::moveChunk(const Chunk &src, Chunk &dst) {
    char sfpath[PATH_MAX], dfpath[PATH_MAX];
    if (getChunkPath(sfpath, src.getChunkName()) == false)
        return false;
    if (getChunkPath(dfpath, dst.getChunkName()) == false)
        return false;

    struct stat sbuf;
    if (stat(sfpath, &sbuf) != 0) 
        return false;
    
    bool success = rename(sfpath, dfpath) == 0;

    // read the chunk for checksum computation if the request is successful
    Chunk readChunk;
    readChunk.copyMeta(dst);
    success = success && (getChunkInternal(readChunk) || !Config::getInstance().verifyChunkChecksum());

    if (success) {
        // mark the size moved
        dst.size = sbuf.st_size;
        // mark the md5 of the moved chunk
        readChunk.computeMD5();
        dst.copyMD5(readChunk);
        LOG(INFO) << "Move chunk " << src.getChunkName() << " to " << dst.getChunkName() << " from path " << sfpath << " to path " << dfpath;
    } else { // revert the change if (checksum verification) failed
        rename(dfpath, sfpath);
    }

    return success;
}

bool FsContainer::hasChunk(const Chunk &chunk) {
    char fpath[PATH_MAX];
    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    Chunk readChunk;
    readChunk.copyMeta(chunk);
    bool checksumPassed = !Config::getInstance().verifyChunkChecksum() || getChunk(readChunk);

    struct stat sbuf;
    return stat(fpath, &sbuf) == 0 && chunk.size == sbuf.st_size && checksumPassed;
}

bool FsContainer::revertChunk(const Chunk &chunk) {
    char fpath[PATH_MAX];
    std::string ofpath, tfpath;

    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    getOldChunkPath(ofpath, fpath, chunk.chunkVersion);
    getOldChunkPath(tfpath, fpath, "0");

    rename(fpath, tfpath.c_str());
    bool okay = rename(ofpath.c_str(), fpath) == 0;
    if (!okay) {
        rename(tfpath.c_str(), fpath);
        LOG(ERROR) << "Failed to revert chunk " << fpath << " back to version "  << chunk.chunkVersion << " (" << ofpath << ")";
    } else {
        unlink(tfpath.c_str());
    }

    return okay;
}

bool FsContainer::verifyChunk(const Chunk &chunk) {
    char fpath[PATH_MAX];
    std::string ofpath, tfpath;

    if (getChunkPath(fpath, chunk.getChunkName()) == false)
        return false;

    bool matched = false;
    Chunk readChunk;
    readChunk.copyMeta(chunk);
    // either verified when reading chunk data back (if checksum verification is enabled), or manual verification
    matched = getChunkInternal(readChunk) && (Config::getInstance().verifyChunkChecksum() || readChunk.verifyMD5());
    LOG_IF(WARNING, !matched) << "Check chunk " << fpath << " by reading data and computing checksum, result = " << matched;

    return matched;
        
}

bool FsContainer::getTotalSize(unsigned long int &total, bool needsLock) {
    total = 0;
    // sum up the size of all files
    try {
        for (boost::filesystem::directory_entry &f : boost::filesystem::recursive_directory_iterator(boost::filesystem::path(_dir))) {
            if (boost::filesystem::is_regular_file(f.path()) && !isOldChunks(f.path().c_str()))
                total += boost::filesystem::file_size(f.path());
        }
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to list directory " << _dir << ", " << e.what();
        return false;
    }
    return true;
}

bool FsContainer::isOldChunks(const char *fpath) {
    // find the flie name in the path
    const char *idx = strrchr(fpath, '/');
    // check if the file name contains a '.', names of current versions only contain '-', '_', and alphanumerics
    return strchr(idx == NULL? fpath : idx, '.') != NULL;
}

void FsContainer::updateUsage() {
    unsigned long int total = 0;
    if (getTotalSize(total)) {
        _usage = total;
    } else {
        LOG(WARNING) << "Failed to update usage for container id = " << _id;
    }
}

void *FsContainer::cleanUpOldChunks(void *arg) {
    FsContainer *container = (FsContainer *) arg;
    struct timespec nextSchTime;

    const time_t timeout = 60; // timeout for checking and cleaning chunks (1min)

    pthread_mutex_lock(&container->_chunkCleanUp.lock);
    do {
        clock_gettime(CLOCK_REALTIME, &nextSchTime);
        nextSchTime.tv_sec += timeout;
        // wait for signal or timeout before next checking and cleaning
        pthread_cond_timedwait(&container->_chunkCleanUp.cond, &container->_chunkCleanUp.lock, &nextSchTime);
        // check and clean up
        bool removed = false;
        do {
            // work-around for exception throw of invalid pointer after file removal
            removed = false;
            try {
                // go over the directory
                for (boost::filesystem::directory_entry &f : boost::filesystem::recursive_directory_iterator(boost::filesystem::path(container->_dir))) {
                    //LOG(INFO) << "Clean up check path " << f.path();
                    // skip (1) non-regular files, (2) files that are not old ones, and (3) old chunks that is yet expired (10mins)
                    if (!boost::filesystem::is_regular_file(f.path()) ||
                        !isOldChunks(f.path().c_str()) ||
                        boost::filesystem::last_write_time(f.path()) + timeout * 10 > time(NULL))
                        continue;
                    // clean the chunk
                    LOG(INFO) << "Clean chunk at " << f.path() << " of size " << boost::filesystem::file_size(f.path());
                    unlink(f.path().c_str());
                    removed = true;
                }
            } catch (std::exception &e) {
                //LOG(ERROR) << "Failed to list directory " << container->_dir << " for cleaning up old chunks, " << e.what();
            }
        } while (removed);
    } while (container->_running);
    pthread_mutex_unlock(&container->_chunkCleanUp.lock);

    LOG(WARNING) << "FS container clean up thread exists now";

    return 0;
}
