// SPDX-License-Identifier: Apache-2.0

#ifndef __PROXY_HH__
#define __PROXY_HH__

#include <atomic>
#include <string>
#include <map>
#include <vector>
#include <set>
#include <boost/uuid/uuid.hpp>
#include <boost/timer/timer.hpp>

#include <zmq.hpp>

#include "bg_chunk_handler.hh"
#include "chunk_manager.hh"
#include "coordinator.hh"
#include "stats_saver.hh"
#include "metastore/all.hh"
#include "staging/staging.hh"
#include "dedup/dedup.hh"


class Proxy {
public:
    Proxy();
    Proxy(ProxyCoordinator *coordinator, std::map<int, std::string> *map, BgChunkHandler::TaskQueue *queue = 0, DeduplicationModule *dedup = 0, bool enableAutoRepair = Config::getInstance().autoFileRecovery());
    virtual ~Proxy();


    /*******************/
    /* File Operations */
    /*******************/

    /**
     * Write the file to backend data store
     *
     * @param[in] f file to write, containing name, size, offset (0), length (same as size) and data
     *
     * @return whether the write is successful
     **/
    virtual bool writeFile(File &f);

    /**
     * Overwrite part of an existing file in the backend data store
     * @see getExpectedAppendSize()
     *
     * @param[in,out] f file to append, containing name, offset (needs to be a multiple of stripe size), length (needs to be a multiple of stripe size), and data; size is updated to the current file size upon successful overwrite 
     *
     * @return whether the overwrite is successful
     **/
    virtual bool overwriteFile(File &f);

    /**
     * Append to a existing file in the backend data store
     * @see getExpectedAppendSize()
     *
     * @param[in,out] f file to append, containing name, offset (needs to be a multiple of stripe size), length (needs to be a multiple of stripe size, or less only if this is the last append), and data; size is updated to the current file size upon successful append
     *
     * @return whether the append is successful
     **/
    virtual bool appendFile(File &f);

    /**
     * Read the file to backend data store
     *
     * @param[in] f file to read, containing the name, the size and data will be returned on successful read
     *
     * @return whether the read is successful
     **/
    virtual bool readFile(File &f, bool isPartial = false);

    /**
     * Read the file to backend data store
     *
     * @param[in]  fuuid  uuid of the file to read
     * @param[out] f      file to read, the name, size and data will be returned on successful read
     *
     * @return whether the read is successful
     **/
    virtual bool readFile(boost::uuids::uuid fuuid, File &f);

    /**
     * Read part of the file to backend data store
     *
     * @param[in] f file to read, containing the name, offset and length; data will be returned on successful read
     *
     * @return whether the read is successful
     **/
    virtual bool readPartialFile(File &f);

    /**
     * Delete the file in backend data store
     *
     * @param[in] f file to delete, containing the name
     *
     * @return whether the delete is successful
     **/
    virtual bool deleteFile(const File &f);

    /**
     * Delete the file to backend data store
     *
     * @param[in]  fuuid  uuid of the file to delete
     * @param[out] f      file to delete, the name, size and data will be returned on successful delete 
     *
     * @return whether the delete is successful
     **/
    virtual bool deleteFile(boost::uuids::uuid fuuid, File &f);

    /**
     * Rename the file in backend data store
     *
     * @param[in] sf file to rename, containing the name
     * @param[in] df renamed file, containing the name
     *
     * @return whether the renaming is successful
     **/
    virtual bool renameFile(File &sf, File &df);

    /**
     * Copy the file in backend data store
     *
     * @param[in] sf file to copy, containing the name
     * @param[in] df copied file, containing the name
     *
     * @return whether the copying is successful
     **/
    virtual bool copyFile(File &sf, File &df);

    /**
     * Copy the file in backend data store
     *
     * @param[in] fuuid  uuid of the file to copy
     * @param[in] sf     file to copy, containing the name
     * @param[in,out] df copied file, containing the name; with a new file id assigned upon copy success
     *
     * @return whether the copying is successful
     **/
    virtual bool copyFile(boost::uuids::uuid fuuid, File &sf, File &df);

    /**
     * Repair the files in the backend data store
     *
     * @param[in] f file to repair, containing the name
     * @param[in] isBg whether the repair is triggered by background thread
     * @return whether the redundancy of the file is restored
     **/
    virtual bool repairFile(const File &f, bool isBg = false);


    /****************************/
    /* File Metadata Operations */
    /****************************/

    /**
     * Get the file size of a file
     *
     * @param[in,out] f file to probe, containing the name
     * @param[in] copyMeta whether to copy file metadata into f
     * @return size of the file
     **/
    virtual unsigned long int getFileSize(File &f, bool copyMeta = false);

    /**
     * Get the expected size of data for append
     *
     * @param[in] f file to probe, containing the coding metadata
     *
     * @return the expected size of data for append
     **/
    virtual unsigned long int getExpectedAppendSize(const File &f);

    /**
     * Get the expected size of data for append
     *
     * @param[in] storageClass storage class of the file
     *
     * @return the expected size of data for append
     **/
    virtual unsigned long int getExpectedAppendSize(const std::string storageClass);

    /**
     * Get the expected size of data for read 
     *
     * @param[in] f file with name set
     *
     * @return the expected size of data for read
     **/
    virtual unsigned long int getExpectedReadSize(File &f);

    /**
     * Get the expected size of data for read 
     *
     * @param[in] fuuid  uuid of the file
     * @param[in] f file, name will be set if file exists
     *
     * @return the expected size of data for read
     **/
    virtual unsigned long int getExpectedReadSize(boost::uuids::uuid fuuid, File &f);

    /**
     * Get the list of files
     *
     * @param[out] list        pointer to the list of files
     * @param[in] withSize     whether to include file size
     * @param[in] namespaceId  namespace id of files to list
     * @param[in] prefix       prefix of files to list
     *
     * @return number of files in the list 
     **/
    virtual unsigned int getFileList(FileInfo **list, bool withSize = true, bool withVersions = false, unsigned char namespaceId = INVALID_NAMESPACE_ID, std::string prefix = "");

    /**
     * Get the list of folders
     *
     * @param[out] list        the list of folders
     * @param[in] namespaceId  namespace id of folders to list
     * @param[in] prefix       prefix of folders to list
     *
     * @return number of folders in the list 
     **/
    virtual unsigned int getFolderList(std::vector<std::string> &list, unsigned char namespaceId = INVALID_NAMESPACE_ID, std::string prefix = "");

    /**
     * Get the number of files stored and the maximum number of files allowed
     *
     * @param[out] count    number of files stored
     * @param[out] limit    maximum number of files allowed
     **/
    virtual void getFileCountAndLimit(unsigned long int &count, unsigned long int &limit);

    /**
     * Get the number of files stored and the maximum number of files allowed
     *
     * @param[out] count    number of files stored
     * @param[out] repair   number of files pending for repair
     * @return whether the number of files pending for repair is valid
     **/
    virtual bool getNumFilesToRepair(unsigned long int &count, unsigned long int &repair);


    /****************************/
    /* System Status Operations */
    /****************************/

    /**
     * Get Agent status
     *
     * @param[out] info pointer to the structure containing the status of Agents
     *
     * @return number of Agent status
     **/
    virtual int getAgentStatus(ProxyCoordinator::AgentInfo **info);

    /**
     * Get Proxy status
     *
     * @param[out] info pointer to the structure containing the status of Proxy
     *
     * @return always true
     **/
    virtual bool getProxyStatus(SysInfo &info);

    /**
     * Get the storage usage and capacity (usable, but not raw)
     *
     * @param[out] usage    used capacity
     * @param[out] capacity total cacpacity
     **/
    virtual void getStorageUsage(unsigned long int &usage, unsigned long int &capacity);

    /**
     * Get progress of background tasks
     *
     * @param[out] task     name of tasks
     * @param[out] progress progress of tasks
     *
     * @return number of background tasks
     **/
    virtual int getBackgroundTaskProgress(std::string *&task, int *&progress);

protected:
    /************************/
    /* [Internal] Data Type */
    /************************/

    // deduplication
    class StripeLocation {
    public:

        StripeLocation();
        StripeLocation(const std::string name, const unsigned long int offset);
        ~StripeLocation() {};

        void reset();

        std::string _objectName;
        unsigned long int _offset;

        bool operator< (const StripeLocation &rhs) const {
            if (_objectName < rhs._objectName) { // smaller object name
                return true;
            } else if (_objectName == rhs._objectName) { // same object (name)
                if (_offset < rhs._offset) { // smaller offset
                    return true;
                }
                return false;
            }
            return false;
        }
    };

    /*************************************/
    /* [Internal] File Operation Helpers */
    /*************************************/

    bool prepareWrite(File &f, File &wf, int *&spareContainers, int &numSelected, bool needsFindSpareContainers = true);

    /**
     * Write file stripes
     * @param[in] f                      current base file struct for write/overwrite/append
     * @param[in,out] wf                 file containing stripes to write
     * @param[in] spareContainers        id of containers which are spared/selected for write
     * @param[in] numSelected            number of containers in spareContainers
     *
     * @return whether the stripes in wf are written sucessfully
     **/
    bool writeFileStripes(File &f, File &wf, int spareContainers[], int numSelected );

    bool copyFileStripeMeta(File &dst, File &src, int stripeId, const char *op);
    void unsetCopyFileStripeMeta(File &copy);

    /**
     * Modify file via overwrite / append
     *
     * @param[in] f          file to modify
     * @param[in] isAppend   whether the modification is an append
     *
     * @return whether the modification is successful
     **/
    bool modifyFile(File &f, bool isAppend);

    virtual unsigned long int getExpectedAppendSize(int codingScheme, int n, int k, int maxChunkSize);

    // file locking
    bool lockFile(const File &f);
    bool unlockFile(const File &f);
    bool lockFileAndGetMeta(File &f, const char *op);

    // staging
    bool pinStagedFile(const File &f);
    bool unpinStagedFile(const File &f);
    static void *stagingBGWrite(void *param);
    virtual bool bgwriteFileToCloud(File &f);
    static void *stagingBGCacheReads(void *param);
    
    // dedup
    bool dedupStripe(File &swf, std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> > &uniqueFps, std::map<BlockLocation::InObjectLocation, Fingerprint> &duplicateFps, std::string &commitId);
    bool sortStripesAndBlocks(
            const unsigned char namespaceId,
            const char *name,
            const std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> >::iterator uniqueStartFp,
            const std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> >::iterator uniqueEndFp,
            const std::map<BlockLocation::InObjectLocation, Fingerprint>::iterator duplicateStartFp,
            const std::map<BlockLocation::InObjectLocation, Fingerprint>::iterator duplicateEndFp,
            std::map<StripeLocation, std::vector<std::pair<int, BlockLocation::InObjectLocation> > > *externalBlockLocs,
            std::map<unsigned long int, BlockLocation::InObjectLocation> *internalBlockLocs,
            std::map<StripeLocation, std::set<int> > &externalStripes,
            std::map<std::string, File*> &externalFiles,
            std::vector<Fingerprint> &duplicateBlockFps,
            int dataStripeSize = -1
    );


    /********************************/
    /* [Internal] System Operations */
    /********************************/

    // system status
    void updateAgentStatus();

    // repair
    static void *backgroundRepair(void *arg);
    bool needsRepair(File &f, bool updateStatusFirst);
    /**
     * Check and perform batched chunk checksum scan
     *
     * @param[in] list       full list of files 
     * @param[in] numFiles   number of files in the file list
     * @param[in] curIdx     current file scan index
     * @param[in,out] numChunksInBatch    number of chunks in the current batch (info. across consecutive calls)
     * @param[in,out] batchStartIdx       starting file index of the batch (info. across consecutive calls)
     *
     * @return whether the check or scan encountered an error
     **/
    bool batchedChunkScan(const FileInfo *list, const int numFiles, const int curIdx, int &numChunksInBatch, int &batchStartIdx);

    /**
     * Check for corrupted but not failed chunks
     *
     * @param[in] chunksCorrupted         list of corrupted chunks
     * @param[in] numChunks               number of chunks
     * @param[in,out] chunkIndicator      indicators which indicate failed chunks (as false) on input, and indicate both failed and corrupted chunks (as false) on output
     *
     * @return the number of corrupted but not failed chunks
     **/
    int checkCorruptedChunks(bool *chunksCorrupted, int numChunks, bool *chunkIndicator);

    // background tasks
    static void *backgroundTaskCheck(void *arg);
    static void *journalCheck(void *arg);
    bool needsCheckBgChunkTasks(File &f);

    // statistics collection
    std::map<std::string, double> genStatsMap(const boost::timer::cpu_times &dataT, const boost::timer::cpu_times &metaT, const unsigned long int &dataSize) const;


    /**********************/
    /* [Internal] modules */
    /**********************/

    // chunk manager
    ChunkManager *_chunkManager;                                  /**< chunk manager (foreground ops) */
    ChunkManager *_repairChunkManager;                            /**< chunk manager (repair) */
    ChunkManager *_tcChunkManager;                                /**< chunk manager (task checking) */

    // I/O
    ProxyIO *_io;                                                 /**< chunk io module (foreground IOs) */
    ProxyIO *_repairio;                                           /**< chunk io module (repair IOs) */
    ProxyIO *_bgio;                                               /**< chunk io module (background IOs) */
    ProxyIO *_tcio;                                               /**< chunk io module (task checking IOs) */
    BgChunkHandler *_bgChunkHandler;                              /**< background chunk handler */

    // dedup
    DeduplicationModule *_dedup;                                  /**< Deduplication Module */

    // metadata
    MetaStore *_metastore;                                        /**< metadata store */

    // coordinator
    std::map<int, std::string> *_containerToAgentMap;             /**< map of containers [container id]->agent socket*/
    ProxyCoordinator *_coordinator;                               /**< coordinator */

    // statistics
    StatsSaver _statsSaver;                                       /**< statistics saving modulde */

    // background threads
    pthread_t _ct;                                                /**< thread for coordinator */
    pthread_t _rt;                                                /**< thread for (auto) background repair */
    pthread_t _tct;                                               /**< thread for background task checking */
    pthread_t _irct;                                              /**< thread for incomplete request checking */

    // system status
    bool _running;                                                /**< status of the Proxy */
    bool _releaseCoordinator;                                     /**< whether to release coordinator */
    bool _releaseDedupModule;                                     /**< whether to release deduplication module */
    std::atomic<int> _ongoingRepairCnt;                           /**< number of on-going repair task */

    // staging
    bool _stagingEnabled;                                         /**< staging enabled */
    Staging *_staging;                                            /**< staging module */
    pthread_t _stagingBGWriteWorker;                              /**< staging background write to cloud worker */
    pthread_t _stagingBGCacheReadWorker;                          /**< staging background stage read cache worker */
    pthread_cond_t _stagingBgWritePending;                        /**< staging background write pending condition*/
    pthread_mutex_t _stagingBgWritePendingLock;                   /**< staging background write pending lock */
    RingBuffer<File> *_stagingPendingReadCache;                   /**< staging background read cache buffer */
};

#endif // define __PROXY_HH__
