// SPDX-License-Identifier: Apache-2.0

#ifndef __STAGING_STORAGE_HH__
#define __STAGING_STORAGE_HH__

#include <dirent.h>
#include <boost/filesystem.hpp>
#include <fnmatch.h>

#include <sys/file.h>
#include <unistd.h> // lstat(), unlink()
#include <sys/types.h> // lstat()
#include <sys/stat.h> // mkdir(), lstat()
#include <boost/timer/timer.hpp>
#include <map>
#include <mutex>

#include "../../../ds/file.hh"

class StagingStorage {

public:
    StagingStorage() {};
    virtual ~StagingStorage() {};

    // normal file operations
    virtual bool openFile(const File &f) = 0;
    virtual bool closeFile(const File &f) = 0;
    virtual bool writeFile(const File &f, bool isReadFromAgents, bool isTruncated = true) = 0;
    virtual unsigned long int readFile(const File &f) = 0;
    virtual bool deleteFile(const File &f) = 0;

    // read cache operations
    virtual bool commitReadCacheFile(const File &f) = 0;
    virtual bool discardReadCacheFile(const File &f) = 0;

    // for file pinning
    virtual bool pinFile(const File &f) = 0;
    virtual bool unpinFile(const File &f) = 0;
    virtual bool isFilePinned(const File &f, bool needsLock = true) = 0;

    // file metadata
    virtual bool getFileInfo(FileInfo &info) = 0;

    // cleaning
    virtual int cleanIdleFiles(time_t idleTime) = 0;

private:
};

#endif //__STAGING_STORAGE_HH__
