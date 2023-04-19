// SPDX-License-Identifier: Apache-2.0

#ifndef __STAGING_FS_STORAGE_HH__
#define __STAGING_FS_STORAGE_HH__

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
#include "staging_storage.hh"

class StagingFsStorage : public StagingStorage {

public:
    StagingFsStorage();
    ~StagingFsStorage();

    // normal file operations
    bool openFile(const File &f);
    bool closeFile(const File &f);
    bool writeFile(const File &f, bool isReadFromAgents, bool isTruncated = true);
    unsigned long int readFile(const File &f);
    bool deleteFile(const File &f);

    // read cache operations
    bool commitReadCacheFile(const File &f);
    bool discardReadCacheFile(const File &f);

    // for file pinning
    bool pinFile(const File &f);
    bool unpinFile(const File &f);
    bool isFilePinned(const File &f, bool needsLock = true);

    // file metadata
    bool getFileInfo(FileInfo &info);

    // cleaning
    int cleanIdleFiles(time_t idleTime);

private:
    std::string _url;
    std::mutex _pinFileLock;                        /**< lock for pinning file (threads) */

    typedef struct SWriteFLockBuffer {
        FILE *file;
        int numWrittenStripes;
        std::mutex _lock;
    } SWriteFLockBuffer;

    std::map<int, SWriteFLockBuffer*> *_req2SWFLBufferMap; /** map <request, metadata buffer> */
    struct {
        std::map<std::string, std::mutex*> map;
        std::mutex lock;
    } _fileLock;

    void getOldFilePath(std::string &ofpath, const char *fpath, const char *ctime);
    // staging write file lock buffer
    bool initSWFLockBuffer(const File &f, bool isReadFromAgents);
    bool updateSWFLockBuffer(const File &f);
    void cleanSWFLockBuffer(const File &f);

    bool pinFile_(const File &f, bool isPin);
    std::string parseName(const File &in);
    std::string parseName(const FileInfo &in);
    std::string parseName(const char *fname, int length);
    int getStagedFilename(const File &in, char *out);
    int getStagedFilename(const FileInfo &in, char *out);
    int getReadCacheFilename(const File &in, char *out);
    int getPinFilename(const File &in, char *out);
    static int isStagedFile(const struct dirent *d);

};

#endif //__STAGING_FS_STORAGE_HH__
