// SPDX-License-Identifier: Apache-2.0

#include "staging.hh"
#include "storage/staging_fs_storage.hh"

#define SECONDS_PER_DAY (24 * 60 * 60)

/*********************************** public functions ************************************/

Staging::Staging() {
    // config
    Config &config = Config::getInstance();

    if (!config.proxyStagingEnabled()) {
        LOG(ERROR) << "<STAGING> Staging not enabled: " << config.proxyStagingEnabled();
    }
    
    // init submodules
    _running = true;

    _storage = new StagingFsStorage();

    pthread_create(&_act, NULL, cleanIdleFiles, this);
}

Staging::~Staging() {
    // free submodules
    _running = false;
    delete _storage;
    pthread_join(_act, NULL);
}

bool Staging::openFileForWrite(const File &f) {
    return _storage->openFile(f);
}

bool Staging::closeFileForWrite(const File &f) {
    return _storage->closeFile(f);
}

bool Staging::writeFile(File &f, bool readFromCloud, bool isTruncated) {
    // if file is pinned for user write, refuse to write cloud version back to staging
    if (_storage->isFilePinned(f) && readFromCloud)
        return false;
    return _storage->writeFile(f, readFromCloud, isTruncated);
}

bool Staging::commitReadCache(File &f) {
    // avoid file being pinned by write when renaming
    return _storage->commitReadCacheFile(f);
}

bool Staging::abortReadCache(File &f) {
    return _storage->discardReadCacheFile(f);
}

bool Staging::readFile(File &f) {
    bool success = false;
    bool hasData = f.data != 0;
    // allocate buffer locally if needed
    if (!hasData) {
        DLOG(INFO) << "Allocate f.length " << f.length;
        f.data = (unsigned char *) malloc (f.length == 0? 1 : f.length);
    }
    success = f.length == 0;
    if (f.data && !success) {
        DLOG(INFO) << "<READ2> offset = " << f.offset << " length = " << f.length << " data " << (void*) f.data;
        // if no data is requested, no need to read at all
        f.length = _storage->readFile(f);
        // clean up if buffer is allocated locally
        if (!hasData && f.length == INVALID_FILE_LENGTH) {
            free(f.data);
            f.data = 0;
            return false;
        }
    }
    return f.length >= 0;
}

bool Staging::pinFile(const File &f) {
    return _storage->pinFile(f);
}

bool Staging::unpinFile(const File &f) {
    return _storage->unpinFile(f);
}

bool Staging::isFilePinned(const File &f) {
    return _storage->isFilePinned(f);
}

void *Staging::cleanIdleFiles(void *arg) {
    Staging *self = (Staging*) arg;
    Config &config = Config::getInstance();

    int scanIntv = config.getProxyStagingAutoCleanScanIntv();

    time_t lastScan = time(NULL);

    while (self->_running && scanIntv > 0) {
        // scan at certain interval for files to repair
        if (lastScan == -1 || lastScan + scanIntv <= time(NULL)) {
            lastScan = time(NULL);
            DLOG(INFO) << "<STAGING> Start auto-clean at " << ctime (&lastScan);
            std::string acPolicy = config.getProxyStagingAutoCleanPolicy();
            int numDaysExpire = config.getProxyStagingAutoCleanNumDaysExpire();
            scanIntv = config.getProxyStagingAutoCleanScanIntv();
            if (acPolicy == "immediate") {
                self->_storage->cleanIdleFiles(0);
            } else if (acPolicy == "expiry") {
                self->_storage->cleanIdleFiles((time_t) numDaysExpire * SECONDS_PER_DAY);
            }
            lastScan = time(NULL);
            DLOG(INFO) << "<STAGING> Complete auto-clean at " << ctime(&lastScan);
        } else {
            time_t nextScan = lastScan + scanIntv;
            DLOG(INFO) << "<STAGING> Sleep until next cleanup scan at " << ctime(&nextScan);
            // sleep until the next scan interval
            sleep(nextScan - time(NULL));
        }
    }

    LOG(WARNING) << "<STAGING> Stop auto cleanup";
    return 0;
}

bool Staging::deleteFile(const File &f) {
    return _storage->deleteFile(f);
}

bool Staging::getFileInfo(FileInfo &info) {
    return _storage->getFileInfo(info);
}
