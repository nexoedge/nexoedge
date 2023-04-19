// SPDX-License-Identifier: Apache-2.0

#ifndef __REDIS_METASTORE_HH__
#define __REDIS_METASTORE_HH__

#include <mutex>
#include <string>

#include <hiredis/hiredis.h>
#include "metastore.hh"

#include <boost/uuid/uuid.hpp>

class RedisMetaStore : public MetaStore {
public:
    RedisMetaStore();
    ~RedisMetaStore();

    /**
     * See MetaStore::putMeta()
     **/
    bool putMeta(const File &f);

    /**
     * See MetaStore::getMeta()
     **/
    bool getMeta(File &f, int getBlocks = 3);

    /**
     * See MetaStore::deleteMeta()
     **/
    bool deleteMeta(File &f);

    /**
     * See MetaStore::renameMeta()
     **/
    bool renameMeta(File &sf, File &df);

    /**
     * See MetaStore::updateTimestamps()
     **/
    bool updateTimestamps(const File &f);

    /**
     * See MetaStore::updateChunks()
     **/
    int updateChunks(const File &f, int version);

    /**
     * See MetaStore::getFileName(boost::uuids::uuid, File)
     **/
    bool getFileName(boost::uuids::uuid fuuid, File &f);

    /**
     * See MetaStore::getFileList()
     **/
    unsigned int getFileList(FileInfo **list, unsigned char namespaceId = INVALID_NAMESPACE_ID, bool withSize = true, bool withTime = true, bool withVersions = false, std::string prefix = "");

    /**
     * See MetaStore::getFolderList()
     **/
    unsigned int getFolderList(std::vector<std::string> &list, unsigned char namespaceId = INVALID_NAMESPACE_ID, std::string prefix = "", bool skipSubfolders = true);

    /**
     * See MetaStore::getMaxNumKeysSupported()
     **/
    unsigned long int getMaxNumKeysSupported();

    /**
     * See MetaStore::getNumFiles()
     **/
    unsigned long int getNumFiles();

    /**
     * See MetaStore::getNumFilesToRepair()
     **/
    unsigned long int getNumFilesToRepair();

    /**
     * See MetaStore::getFilesToRepair()
     **/
    int getFilesToRepair(int numFiles, File files[]);

    /**
     * See MetaStore::markFileAsNeedsRepair()
     **/
    bool markFileAsNeedsRepair(const File &file);

    /**
     * See MetaStore::markFileAsRepaired()
     **/
    bool markFileAsRepaired(const File &file);

    /**
     * See MetaStore::markFileAsPendingWriteToCloud()
     **/
    bool markFileAsPendingWriteToCloud(const File &file);

    /**
     * See MetaStore::markFileAsWrittenToCloud()
     **/
    bool markFileAsWrittenToCloud(const File &file, bool removePending = false);

    /**
     * See MetaStore::getFilesPendingWriteToCloud()
     **/
    int getFilesPendingWriteToCloud(int numFiles, File files[]);

    /**
     * See MetaStore::updateFileStatus()
     **/
    bool updateFileStatus(const File &file);

    /**
     * See MetaStore::getNextFileForTaskCheck()
     **/
    bool getNextFileForTaskCheck(File &file);

    /**
     * See MetaStore::lockFile()
     **/
    bool lockFile(const File &file);

    /**
     * See MetaStore::unlockFile()
     **/
    bool unlockFile(const File &file);

    /**
     * See MetaStore::addChunkToJournal()
     **/
    bool addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite);

    /**
     * See MetaStore::updateChunkInJournal()
     **/
    bool updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId);

    /**
     * See MetaStore::getFileJournal()
     **/
    void getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records);

    /**
     * See MetaStore::getFilesWithJournal()
     **/
    int getFilesWithJounal(FileInfo **list);

    /**
     * See MetaStore::fileHasJournal()
     **/
    bool fileHasJournal(const File &file);

private:
    redisContext *_cxt;
    std::mutex _lock;
    
    std::string _taskScanIt;
    bool _endOfPendingWriteSet;


    int genFileKey(unsigned char namespaceId, const char *name, int nameLength, char key[]);
    int genVersionedFileKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[]);
    int genFileVersionListKey(unsigned char namespaceId, const char *name, int nameLength, char key[]);
    bool genFileUuidKey(unsigned char  namespaceId, boost::uuids::uuid uuid, char key[]);
    int genChunkKeyPrefix(int chunkId, char prefix[]);
    int genBlockKey(int blockId, char prefix[], bool unqiue);
    int genFileJournalKeyPrefix(char key[], unsigned char namespaceId = 0);
    int genFileJournalKey(unsigned char namespaceId, const char *name, int nameLength, int version, char key[]);
    const char *getBlockKeyPrefix(bool unique);
    bool getNameFromFileKey(const char *str, size_t len, char **name, int &nameLength, unsigned char &namespaceId, int *version = 0);
    bool markFileStatus(const File &file, const char *listName, bool set, const char *opName);
    bool markFileRepairStatus(const File &file, bool needsRepair);

    bool getFileName(char name[], File &f);
    bool isSystemKey(const char *key);
    bool isVersionedFileKey(const char *key);

    std::string getFilePrefix(const char name[], bool noEndingSlash = false);

    bool getLockOnFile(const File &file, bool lock);
    bool pinStagedFile(const File &file, bool pine);

    bool lockFile(const File &file, bool lock, const char *type, const char *name);
};

#endif // define __REDIS_METASTORE_HH__
