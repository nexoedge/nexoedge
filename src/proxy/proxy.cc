// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h>
#include <time.h>

#include <glog/logging.h>

#include "proxy.hh"
#include "dedup/impl/dedup_all.hh"
#include "../common/config.hh"
#include "../common/define.hh"
#include "../common/util.hh"
#include "../common/coding/coding.hh"
#include "../common/checksum_calculator.hh"

#define BG_WRITE_TO_CLOUD_TAG "<BG WRITE TO CLOUD> "

Proxy::Proxy() : Proxy(0, 0) {
}

Proxy::Proxy(ProxyCoordinator *coordinator, std::map<int, std::string> *map, BgChunkHandler::TaskQueue *queue, DeduplicationModule *dedup, bool enableAutoRepair) {
    // config
    Config &config = Config::getInstance();
    
    // coordinator
    if (coordinator == 0 || map == 0) {
        _containerToAgentMap = new std::map<int, std::string>();
        _coordinator = new ProxyCoordinator(_containerToAgentMap);
        pthread_create(&_ct, NULL, ProxyCoordinator::run, _coordinator);
        _releaseCoordinator = true;
    } else {
        _containerToAgentMap = map;
        _coordinator = coordinator;
        _releaseCoordinator = false;
    }

    // chunk io
    _io =  new ProxyIO(_containerToAgentMap);
    _repairio =  new ProxyIO(_containerToAgentMap);
    _tcio =  new ProxyIO(_containerToAgentMap);
    _bgio =  new ProxyIO(_containerToAgentMap);

    // deduplication
    _releaseDedupModule = true;
    if (dedup) {
        _dedup = dedup;
        _releaseDedupModule = false;
    } else {
      LOG(INFO) << "No dedup mod provided";
      _dedup = new DedupNone();
    }

    // metadata store
    switch (config.getProxyMetaStoreType()) {
        case MetaStoreType::REDIS:
            _metastore = new RedisMetaStore();
            break;
        default:
            _metastore = new RedisMetaStore();
            break;
    }

    // set as running
    _running = true;

    // background chunk handler
    _bgChunkHandler = new BgChunkHandler(_bgio, _metastore, &_running, queue);
    _chunkManager = new ChunkManager(_containerToAgentMap, _io, _bgChunkHandler, _metastore);
    _repairChunkManager = new ChunkManager(_containerToAgentMap, _repairio, _bgChunkHandler, _metastore);
    _tcChunkManager = new ChunkManager(_containerToAgentMap, _tcio, _bgChunkHandler);

    // auto file recovery
    _ongoingRepairCnt = 0;
    if (enableAutoRepair)
        pthread_create(&_rt, NULL, Proxy::backgroundRepair, this);

    if (config.ackRedundancyInBackground())
        pthread_create(&_tct, NULL, Proxy::backgroundTaskCheck, this);

    // incomplete request check
    pthread_create(&_irct, NULL, Proxy::journalCheck, this);

    /* staging init */
    _staging = 0;
    _stagingEnabled = config.proxyStagingEnabled();
    if (_stagingEnabled) {
        pthread_mutex_init(&_stagingBgWritePendingLock, NULL);
        pthread_cond_init(&_stagingBgWritePending, NULL);

        // Proxy Staging Integration
         _staging = new Staging();

        // Staging BGTask Param
        pthread_create(&_stagingBGWriteWorker, 0, stagingBGWrite, (void *) this);

        // staging data read in background 
        //pthread_create(&_stagingBGCacheReadWorker, 0, stagingBGCacheReads, (void*) this);
        //_stagingPendingReadCache = new RingBuffer<File>(config.getReadCacheBufferSize(), /* block on empty */ true, 1, /* block on full */ false);
    }
}

Proxy::~Proxy() {
    _running = false;

    LOG(WARNING) << "Terminating Proxy ...";

    // release chunk manager and chunk-related handler
    delete _chunkManager;
    if (Config::getInstance().autoFileRecovery())
        pthread_join(_rt, NULL);
    if (Config::getInstance().ackRedundancyInBackground())
        pthread_join(_tct, NULL);
    pthread_join(_irct, NULL);
    delete _repairChunkManager;
    delete _tcChunkManager;
    delete _bgChunkHandler;
    delete _repairio;
    delete _tcio;
    delete _bgio;
    delete _io;
    if (_releaseDedupModule) {
        delete _dedup;
    }
    // staging
    if (_stagingEnabled) {
        pthread_cond_signal(&_stagingBgWritePending);
        pthread_join(_stagingBGWriteWorker, 0);
        delete _staging;
    }
    // release coordinator
    if (_releaseCoordinator) {
        pthread_join(_ct, NULL);
        delete _coordinator;
        delete _containerToAgentMap;
    }
    // release metadata store
    delete _metastore;
}

void Proxy::updateAgentStatus() {
    _coordinator->updateAgentStatus();
}

int Proxy::getAgentStatus(ProxyCoordinator::AgentInfo **info) {
    return _coordinator->getAgentStatus(info);
}

bool Proxy::getProxyStatus(SysInfo &info) {
    return _coordinator->getProxyStatus(info);
}

void Proxy::getStorageUsage(unsigned long int &usage, unsigned long int &capacity) {
    _coordinator->getStorageUsage(usage, capacity);
}

int Proxy::getBackgroundTaskProgress(std::string *&task, int *&progress) {
    return _bgChunkHandler->getTaskProgress(task, progress);
}

void* Proxy::backgroundRepair(void *arg) {
    Proxy *self = (Proxy *) arg;

    int pollIntv = Config::getInstance().getFileRecoverInterval();
    int fileScanIntv = Config::getInstance().getFileScanInterval();
    int chunkScanIntv = Config::getInstance().getChunkScanInterval();
    int batchSize = Config::getInstance().getFileRecoverBatchSize();

    int k = Config::getInstance().getK();

    time_t lastPoll = time(NULL);
    time_t lastFileScan = time(NULL);
    time_t lastChunkScan = time(NULL);

    while(self->_running && (pollIntv > 0 || fileScanIntv > 0 || chunkScanIntv > 0)) {
        time_t curTime = time(NULL);
        // scan at certain interval for files to repair
        if (
            (fileScanIntv > 0 && lastFileScan + fileScanIntv <= curTime) ||
            (chunkScanIntv > 0 && lastChunkScan + chunkScanIntv <= curTime)
        ) {
            DLOG(INFO) << "Start scanning at " << time(NULL);
            // scan all file names for repair
            FileInfo *list = 0;
            int numFiles = self->getFileList(&list, /* withSize */ true, /* withTime */ true, /* withVersions */ true);
            int batchStartIdx = 0, numChunksInBatch = 0;
            File file;

#define checkFile(__FL__) do { \
    DLOG(INFO) << "Check file " << file.name << " version " << file.version << " for missing chunk at " << time(NULL); \
    if ( \
        fileScanIntv > 0 && \
        lastFileScan + fileScanIntv <= curTime && \
        self->needsRepair(__FL__, /* updateStatusFirst */ i == 0) \
    ) { \
        self->_metastore->markFileAsNeedsRepair(__FL__); \
        DLOG(INFO) << "Add file " << __FL__.name << " of version " << __FL__.version << " for missing chunk at " << time(NULL); \
    } \
    /* scan for corrupted chunks */ \
    if (chunkScanIntv > 0 && lastChunkScan + chunkScanIntv <= curTime) { \
        self->batchedChunkScan(list, numFiles, i, numChunksInBatch, batchStartIdx); \
    } \
} while (0) \

            for (int i = 0; i < numFiles; i++) {
                file.name = list[i].name;
                file.nameLength = list[i].nameLength;
                file.namespaceId = list[i].namespaceId;
                file.version = list[i].version;
                checkFile(file);
                int numVersions = list[i].numVersions;
                for (int vi = 0; vi < numVersions; vi++) {
                    file.version = list[i].versions[vi].version;
                    checkFile(file);
                }
                file.name = 0;
            }

#undef checkFile

            DLOG(INFO) << "Complete scanning at " << time(NULL);
            // update time of last scan
            if (lastFileScan + fileScanIntv <= time(NULL))
                lastFileScan = time(NULL);
            if (lastChunkScan + chunkScanIntv <= time(NULL))
                lastChunkScan = time(NULL);
            delete [] list;
        }

        // poll at certain interval for files to repair
        if ((lastPoll == -1 || lastPoll + pollIntv <= time(NULL))) {
            if (self->_coordinator->getNumAliveContainers(/* skipfull */ true) >= k) {
                DLOG(INFO) << "Start repair at " << time(NULL);
                // get and move a file name to lock and repair
                int numToRepair = 0;
                do {
                    File files[batchSize];
                    numToRepair = self->_metastore->getFilesToRepair(batchSize, files);
                    self->_ongoingRepairCnt += numToRepair;
                    // repair file
                    for (int i = 0; i < numToRepair; i++) {
                        // if is locked for repair and repair suceed, remove from under repair list; otherwise, put it back to the list for pending repair (retry)
                        if (self->repairFile(files[i], /* isBg */ true)) {
                            DLOG(INFO) << "Repair file " << files[i].name << " at " << time(NULL);
                        } else {
                            //self->_metastore->markFileAsNeedsRepair(files[i]);
                            self->_ongoingRepairCnt -= numToRepair;
                            numToRepair = 0;
                            break;
                        }
                    }
                    self->_ongoingRepairCnt -= numToRepair;
                } while (numToRepair > 0);
                DLOG(INFO) << "End repair at " << time(NULL);
            }
            // update time of last poll
            lastPoll = time(NULL);
        }

        // choose to sleep the least amount of time before next scan or repair
#define updateTimeToSleep(__LAST_ACT_TIME__, __ACT_INTV__) do { \
    time_t timeToNextAction = __ACT_INTV__ - (time(NULL) - __LAST_ACT_TIME__); \
    if (timeToNextAction > 0 || (timeToNextAction == 0 && __ACT_INTV__ > 0)) { \
        sleepTime = std::min(timeToNextAction, sleepTime); \
    } else { \
        sleepTime = __ACT_INTV__ > 0? std::min(sleepTime, (time_t) __ACT_INTV__) : sleepTime; \
    } \
} while (0)
        time_t sleepTime = HOUR_IN_SECONDS * 24;
        updateTimeToSleep(lastFileScan, fileScanIntv);
        updateTimeToSleep(lastChunkScan, chunkScanIntv);
        updateTimeToSleep(lastPoll, pollIntv);

#undef updateTimeToSleep

        // sleep until next interval for scan or repair
        sleep(sleepTime);
    }

    LOG(WARNING) << "Stop repairing in backgroud";
    return 0;
}

bool Proxy::needsRepair(File &f, bool updateStatusFirst) {
    File rf;

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    if (rf.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file metadata for check repair operaiton";
        return false;
    }

    rf.copyVersionControlInfo(f);

    // get file metadata, no need to read the blocks information
    if (_metastore->getMeta(rf, /* get blocks */ false) == false) {
        LOG(WARNING) << "Failed to find file metadata for file " << f.name;
        return false;
    }

    bool chunkIndices[rf.numChunks];
    // recover if there are chunk failures, and the file has not been modified since last repair check
    return _coordinator->checkContainerLiveness(rf.containerIds, rf.numChunks, chunkIndices, updateStatusFirst, /* checkAllFailures */ false) > 0
            && rf.mtime + Config::getInstance().getFileRecoverInterval() < time(NULL);
}

bool Proxy::batchedChunkScan(const FileInfo *list, const int numFiles, const int curIdx, int &numChunksInBatch, int &batchStartIdx) {
    // report error if list is not provided, curIdx is beyond the list, or batchStartIdx is beyond the list
    if (list == NULL || curIdx >= numFiles || batchStartIdx >= numFiles)
        return false;

    Config &config = Config::getInstance();
    int batchSizeLimit = config.getChunkScanBatchSize();

    // count chunks in current version
    numChunksInBatch += list[curIdx].numChunks;
    // also count chunks from previous versions
    int numVersions = list[curIdx].numVersions;
    for (int vi = 0; vi < numVersions; vi++) {
        numChunksInBatch += list[curIdx].versions[vi].numChunks;
    }
    //DLOG(INFO) << "File list num files = " << numFiles << " curidx = " << curIdx << " numChunksBatched = " << numChunksInBatch << " batchStart = " << batchStartIdx << " batchSizeLimit = " << batchSizeLimit;

    // issue batched chunk checking request if we reach the end of list or the numbers of batched chunks reaches the limit
    if (curIdx + 1 == numFiles || numChunksInBatch >= batchSizeLimit) {
        DLOG(INFO) << "File list num files = " << numFiles << " curidx = " << curIdx << " numChunksBatched = " << numChunksInBatch << " batchStart = " << batchStartIdx << " batchSizeLimit = " << batchSizeLimit;

        File vf[MAX_NUM_CONTAINERS]; // virtual files for chunks associated to each container
        std::map<int, int> containerId2vfMap; // mapping from container id to vf index
        std::map<std::pair<boost::uuids::uuid, int>, std::pair<File*, bool> > fileMap;
        std::set<std::pair<boost::uuids::uuid, int> > filesModified;

        int numChunksProcessed[MAX_NUM_CONTAINERS]; // counter for number of chunks processed by container (chunk-level/container-level sampling)
        int samplingPolicy = config.getChunkScanSamplingPolicy(); 
        double samplingRate = config.getChunkScanSamplingRate();
        int numSampled = 0; // number of chunks/files sampled (chunk-level/file-level sampling)

        // batch chunks from files into virtual files according to their container ids
        for (int fidx = batchStartIdx; fidx <= curIdx; fidx++) {
            // skip files that are recently modified
            //DLOG(INFO) << "File = " << fidx << " mtime = " << list[fidx].mtime << " now = " << time(NULL) << " interval = " << config.getFileRecoverInterval();
            if (list[fidx].mtime + config.getFileRecoverInterval() > time(NULL))
                continue;

            // determine whether to scan this file for file-level sampling
            if (samplingPolicy == ChunkScanSamplingPolicy::FILE_LEVEL) {
                if (Util::includeSample(curIdx - batchStartIdx + 1, samplingRate)) { // include this in the samples
                    numSampled++;
                } else { // skip this
                    DLOG(INFO) << "Sampling: skip 1 file " << list[fidx].name << ", cur sampling rate = " << numSampled << "/" << curIdx - batchStartIdx + 1 << " vs " << samplingRate;
                    continue;
                }
            }
            
# define checkChunks(__SKIP_LOCK__) do { \
            /* lock and get metadata of all files in the batch */ \
            if ( (__SKIP_LOCK__ && _metastore->getMeta(*f)) || (!__SKIP_LOCK__ && lockFileAndGetMeta(*f, "chunk scan")) ) { \
                /* save file metadata */ \
                fileMap.insert(std::make_pair(std::make_pair(f->uuid, f->version), std::make_pair(f, !__SKIP_LOCK__))); \
                /* gather the chunks into a virtual file */ \
                for (int cidx = 0; cidx < f->numChunks; cidx++) { \
                    /* update the number of chunks processed for chunk-level sampling */ \
                    numChunksProcessed[0]++; \
                    /* determine whether to scan this chunk for chunk-level sampling */ \
                    if (samplingPolicy == ChunkScanSamplingPolicy::CHUNK_LEVEL) { \
                        if (Util::includeSample(numChunksInBatch, samplingRate)) { \
                            numSampled++; \
                        } else { /* skip chunk (among all) */ \
                            DLOG(INFO) << "Sampling: skip 1 chunk in file " << f->name << ", cur sampling rate = " << numSampled << "/" << numChunksInBatch << " vs " << samplingRate; \
                            continue; \
                        } \
                    } \
                    /* determine whether to scan this chunk for stripe-level sampling */ \
                    if (samplingPolicy == ChunkScanSamplingPolicy::STRIPE_LEVEL) { \
                        int chunkInStripeIdx = cidx % f->codingMeta.n; \
                        /* reset the number of sampled chunks when starting a new stripe */ \
                        if (chunkInStripeIdx == 0) \
                            numSampled = 0; \
                        if (Util::includeSample(f->codingMeta.n, samplingRate)) { \
                            numSampled++; \
                        } else { /* skip chunk (in stripe) */ \
                            DLOG(INFO) << "Sampling: skip 1 chunk in stripe " << cidx / f->codingMeta.n << " of file " << f->name << ", cur sampling rate = " << numSampled << "/" << f->codingMeta.n << " vs " << samplingRate; \
                            continue; \
                        } \
                    } \
                    int containerId = f->containerIds[cidx]; \
                    /* get or new the virtual file for this container id */ \
                    std::map<int, int>::iterator it; \
                    std::tie(it, std::ignore) = containerId2vfMap.insert(std::make_pair(containerId, containerId2vfMap.size())); \
                    if (it == containerId2vfMap.end()) { \
                        LOG(WARNING) << "Failed to process check for chunk " << cidx << " of file " << f->name; \
                        continue; \
                    } \
                    File &cf = vf[it->second]; \
                    /* determine whether to scan this chunk for container-level sampling */ \
                    if (samplingPolicy == ChunkScanSamplingPolicy::CONTAINER_LEVEL) { \
                        numChunksProcessed[it->second]++; \
                        /* assume the chunks are evenly distributed to all existing containers */ \
                        /* TODO remove this assumption (num. of containers can be larger than n) */ \
                        if (!Util::includeSample(numChunksInBatch / _coordinator->getNumAliveContainers(), samplingRate)) { \
                            DLOG(INFO) << "Sampling: skip 1 chunk in container " << containerId << ", cur sampling rate = " << cf.numChunks << "/" << numChunksInBatch / _coordinator->getNumAliveContainers() << " vs " << samplingRate; \
                            continue; \
                        } \
                    } \
                    /* init the chunk list and container id on first use */ \
                    if (cf.numChunks == 0) { \
                        try { \
                            cf.chunks = new Chunk[numChunksInBatch]; \
                            cf.containerIds = new int[1]; \
                        } catch (std::bad_alloc &e) { \
                            LOG(ERROR) << "Failed to allocate memory for " << numChunksInBatch << " chunks in batch for checking"; \
                            f = 0; \
                            /*return false;*/ \
                            break; \
                        } \
                        cf.containerIds[0] = containerId; \
                    } \
                    if (cf.numChunks < numChunksInBatch) { \
                        /* push a record to the virtual file */ \
                        cf.chunks[cf.numChunks++] = f->chunks[cidx]; \
                    } else { \
                        LOG(WARNING) << "Skipping chunk " << cidx << " of file " << f->name << " as the chunk list of container " << it->second << " is full (" << cf.numChunks << "/" << numChunksInBatch << ")"; \
                    } \
                } \
                f = 0; \
            } \
} while (0)

            File *f = new File();
            f->setName(list[fidx].name, list[fidx].nameLength);
            f->namespaceId = list[fidx].namespaceId;
            f->version = list[fidx].version;

            // check the current version
            checkChunks(false);

            // also examine the versions
            int numVersions = list[fidx].numVersions;
            for (int vi = 0; vi < numVersions; vi++) {
                f = new File();
                f->setName(list[fidx].name, list[fidx].nameLength);
                f->namespaceId = list[fidx].namespaceId;
                f->version = list[fidx].versions[vi].version;
                DLOG(INFO) << "Check chunks of file " << f->name << " version " << f->version;
                checkChunks(true);
            }

#undef checkChunks
        }

        // issue batched chunk request (of virtual files) and update metadata of files to repair
        for (auto it : containerId2vfMap) {
            // skip checking if container already fails
            bool containerStatus = false;
            int containerId = it.first;
            if (_coordinator->checkContainerLiveness(&containerId, 1, &containerStatus) > 0) {
                DLOG(INFO) << "Skip offline container " << containerId;
                continue;
            }

            File &cf = vf[it.second];
            bool chunkIndicators[cf.numChunks];
            int numFailedChunks = _chunkManager->verifyFileChecksums(cf, chunkIndicators);
            // update (local) file metadata for failed chunks
            for (int cidx = 0; numFailedChunks > 0 && cidx < cf.numChunks; cidx++) {
                // skip if chunk is good
                if (!chunkIndicators[cidx])
                    continue;
                // find the chunk's file metadata and mark the chunk as missing
                try {
                    std::pair<boost::uuids::uuid, int> key = std::make_pair(cf.chunks[cidx].fuuid, cf.chunks[cidx].fileVersion);
                    File *f = fileMap.at(key).first;
                    int chunkId = cf.chunks[cidx].getChunkId();
                    //f.containerIds[chunkId] = INVALID_CONTAINER_ID;
                    f->chunksCorrupted[chunkId] = true;
                    // indicate the file metadata is modified
                    filesModified.insert(key);
                    DLOG(INFO) << "Chunk corruption detected, file " << f->name << " chunk " << chunkId;
                } catch (std::exception &e) {
                    continue;
                }
            }
        }

        // update modified file metadata in metastore and mark them as pending for repair
        for (auto idx : filesModified) {
            File *f = fileMap.at(idx).first;
            _metastore->putMeta(*f);
            _metastore->markFileAsNeedsRepair(*f);
            DLOG(INFO) << "Add file " << f->name << " for repairing corrupted chunks at " << time(NULL);
        }

        // release file lock
        for (auto &it : fileMap) {
            if (it.second.second)
                unlockFile(*(it.second.first));
            delete it.second.first;
            it.second.first = 0;
        }

        batchStartIdx = curIdx + 1;
        numChunksInBatch = 0;
    }
    return false;
}

int Proxy::checkCorruptedChunks(bool *chunksCorrupted, int numChunks, bool *chunkIndicator) {
    int numCorruptedChunks = 0;
    for (int i = 0; i < numChunks; i++) {
        // chunk already failed, not marking as corrupted
        if (!chunkIndicator[i])
            continue;
        // mark corrupted chunks
        if (chunksCorrupted[i]) {
            chunkIndicator[i] = false;
            numCorruptedChunks++;
        }
    }
    return numCorruptedChunks;
}

void* Proxy::backgroundTaskCheck(void *arg) {
    Proxy *self = (Proxy *) arg;

    int taskCheckIntv = Config::getInstance().getBgTaskCheckInterval();
    
    time_t lastCheckTime = time(NULL);

    FileInfo lastCheckedFile;
    std::string lastCheckedFileName;

    while (self->_running) {
        File file;
        // sleep-wait until next interval
        sleep(std::max((long int) 0, lastCheckTime + taskCheckIntv - time(NULL)));
        // print the background task progress
        std::string *task = 0;
        int *progress = 0;
        int numTask = self->_bgChunkHandler->getTaskProgress(task, progress);
        if (numTask > 0)
            LOG(INFO) << "------- Background Task Progress -------";
        for (int i = 0; i < numTask; i++)
            LOG(INFO) << " " << std::left << std::setfill(' ') << std::setw(30) << task[i] << ": " << progress[i] << "%";
        if (numTask > 0)
            LOG(INFO) << "----------------------------------------";
        delete[] task;
        delete[] progress;
        // find next file to check
        while (self->_metastore->getNextFileForTaskCheck(file)) {
            lastCheckTime = time(NULL);
            // if there is task pending or is an empty file, do not initiate a check
            if (self->_bgChunkHandler->taskExistsForFile(file)) {
                break;
            }

            // lock file for checking
            if (self->lockFile(file) == false) {
                LOG(ERROR) << "Failed to lock file " << file.name << " for checking";
                break;
            }

            // lock file for checking chunk existence and update any failed chunk locations if no task in queue
            if (self->_metastore->getMeta(file) && file.numStripes > 0) {

                int numChunksPerStripe = file.numChunks / file.numStripes;
                bool chunkIndicator[numChunksPerStripe];
                for (int i = 0; i < file.numStripes; i++) {
                    File scf;
                    if (self->copyFileStripeMeta(scf, file, i, "check") == false) {
                        self->unlockFile(file);
                        continue;
                    }
                    // check for chunk failure in stripe
                    int numFailed = self->_tcChunkManager->checkFile(scf, chunkIndicator);
                    DLOG(INFO) << "Number of failed chunks = " << numFailed;
                    // skip if none of the chunks failed
                    if (numFailed > 0) {
                        // mark the failed chunks
                        for (int j = 0; j < numChunksPerStripe; j++) {
                            // skip if not failed
                            if (chunkIndicator[j])
                                continue;
                            scf.containerIds[j] = -1;
                        }
                        int ret = self->_metastore->updateChunks(scf, file.version);
                        // update metadata in metastore
                        if (ret != 0) {
                            LOG(ERROR) << "Failed to mark chunks that needs to be repaired for file " << scf.name << " in stripe " << i << " error = " << ret;
                            // TODO ask the user to download the file and re-upload it
                        }
                    }
                    // clean up (avoid double free)
                    scf.chunks = 0;
                    scf.containerIds = 0;
                    scf.codingMeta.codingState = 0;
                }
                self->_metastore->unlockFile(file);
            }

            // reset the number of pending task after checking
            file.status = FileStatus::ALL_BG_TASKS_COMPLETED;
            self->_metastore->updateFileStatus(file);
        }
        // update last task check time
        lastCheckTime = time(NULL);
    }

    LOG(WARNING) << "Stop background task checking";

    return 0;
}

void* Proxy::journalCheck(void *arg) {
    Proxy *self = (Proxy *) arg;

    int reqCheckIntv = Config::getInstance().getJournalCheckInterval();
    
    time_t lastCheckTime = time(NULL);

    FileInfo *fileList = nullptr;
    int numFiles = 0;

    while (self->_running && reqCheckIntv > 0) {
        // sleep-wait until next interval
        sleep(std::max((long int) 0, lastCheckTime + reqCheckIntv - time(NULL)));

        // get all files with journal
        numFiles = self->_metastore->getFilesWithJounal(&fileList);
        for (int fidx = 0; fidx < numFiles; fidx++) {
            const FileInfo &file = fileList[fidx];

            // obtain file lock and metadata before checking
            File fileMeta, reqFileMeta;
            fileMeta.copyName(file);
            reqFileMeta.copyName(file);
            if (!self->lockFile(fileMeta)) {
                LOG(WARNING) << "Failed to lock file " << file.name << " for checking incomplete chunk requests.";
                continue;
            }
            self->_metastore->getMeta(fileMeta);

            // allocate one chunk record for chunk checking / operation redo
            reqFileMeta.initChunksAndContainerIds(1);
            bool chunkIndicators[1] = { true };

            // get and process the list of journaled chunk requests of the file
            std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> records;
            self->_metastore->getFileJournal(file, records);

            int numRecords = records.size();
            for (int ridx = 0; ridx < numRecords; ridx++) {
                Chunk &chunk = std::get<0>(records[ridx]);
                chunk.fileVersion = file.version;
                const int chunkId = chunk.getChunkId();
                const int &containerId = std::get<1>(records[ridx]);
                const bool isWrite = std::get<2>(records[ridx]);
                const bool isPre = std::get<3>(records[ridx]);
                bool online = false;

                // skip on-going operations (this should not happen as on-going operation should have locked the file?)
                if (isPre) continue;

                // skip requests on offline containers
                if (self->_coordinator->checkContainerLiveness(&containerId, 1, &online, /* updateStatusFirst */ false) > 0) continue;

                // copy the chunk metadata to prepare for a request
                reqFileMeta.genUUID();
                reqFileMeta.chunks[0].copyMeta(chunk);
                reqFileMeta.chunks[0].setId(reqFileMeta.namespaceId, reqFileMeta.uuid, chunkId);
                reqFileMeta.containerIds[0] = containerId;
                reqFileMeta.copyStoragePolicy(fileMeta);

                bool removeJournal = false;
                if (isWrite) {
                    // check if the chunk record in the file metadata is same as the journaled one
                    // if true, simply treat the request as committed and remove it from the journal
                    removeJournal = fileMeta.numChunks > chunkId && chunk.matchMeta(fileMeta.chunks[chunkId]);
                    // otherwise, check chunk existance and integrity
                    if (!removeJournal) {
                        removeJournal = self->_tcChunkManager->verifyFileChecksums(reqFileMeta, chunkIndicators) == 0;
                        if (removeJournal) {
                            DLOG(INFO) << "Update the valid chunk " << chunkId << " of file " << fileMeta.name << " of versoin " << fileMeta.version << " in container " << containerId << " in the file metadata.";
                            // if the chunk is valid, commit the chunk by updating the file metadata
                            fileMeta.chunks[chunkId].copyMeta(chunk);
                            fileMeta.containerIds[chunkId] = containerId;
                            self->_metastore->putMeta(fileMeta);
                        } else {
                            // otherwise (if invavlid), delete the chunk
                            LOG(INFO) << "Going to delete the invalid chunk " << reqFileMeta.chunks[0].getChunkName() << " of file " << reqFileMeta.name << " of versoin " << reqFileMeta.version << " in container " << reqFileMeta.containerIds[0] << ".";
                            chunkIndicators[0] = true;
                            removeJournal = self->_tcChunkManager->deleteFile(reqFileMeta, chunkIndicators);
                        }
                    }
                } else {
                    // re-issue the deletion request and mark the journal as to-remove if succeeded
                    LOG(INFO) << "Going to delete the chunk " << chunkId << " of file " << fileMeta.name << " of versoin " << fileMeta.version << " in container " << containerId << " again.";
                    bool chunkExistsAndMatches = fileMeta.numChunks > chunkId && fileMeta.containerIds[chunkId] == containerId && fileMeta.chunks[chunkId].matchMeta(chunk);
                    if (chunkExistsAndMatches) {
                        removeJournal = true;
                    } else {
                        chunkIndicators[0] = true;
                        removeJournal = self->_tcChunkManager->deleteFile(reqFileMeta, chunkIndicators);
                    }
                }

                // remove the journal record once verified the chunk integrity or successfully removed the invalid chunk
                if (removeJournal) {
                    if (!self->_metastore->updateChunkInJournal(fileMeta, chunk, isWrite, /* deleteRecord */ removeJournal, containerId)) {
                        LOG(WARNING) << "Failed to remove the chunk journal of file " << file.name << " chunk " << chunkId << " in namespace " << file.namespaceId << " version " << file.version << ".";
                    }
                }
            }

            // release file lock after checking
            self->unlockFile(fileMeta);
        }

        // free the file list resources after use
        delete [] fileList;
        fileList = nullptr;
        numFiles = 0;

        // update the request checking time
        lastCheckTime = time(NULL);
    }

    return NULL;
}

void *Proxy::stagingBGWrite(void *param) {
    Proxy *self = (Proxy*) param;
    Config &config = Config::getInstance();
    std::string bgwritePolicy = config.getProxyStagingBackgroundWritePolicy();
    int scanIntv = config.getProxyStagingBackgroundWriteScanInterval();

    time_t lastScan = time(NULL);

    while (self->_running && scanIntv > 0) {

        if (bgwritePolicy == "none") {
            // set default scan interval to 1 hr for no background write
            scanIntv = 60 * 60;
        } else if (bgwritePolicy == "scheduled") {
            // get time now 
            time_t now = time(NULL);
            struct tm *nowTM = std::localtime(&now);

            // get background write deadline today
            std::string bgwriteTimestamp = config.getProxyStagingBackgroundWriteTimestamp();
            struct tm bgwriteTimestampTM = *nowTM;
            bgwriteTimestampTM.tm_sec = 0;
            strptime(bgwriteTimestamp.c_str(), "%H:%M", &bgwriteTimestampTM);

            // set scan interval to the time difference from now to next deadline
            time_t deadlineToday = mktime(&bgwriteTimestampTM);
            scanIntv = deadlineToday + ((deadlineToday <= time(NULL))? 24 * 60 * 60 : 2) - lastScan;
        } else if (bgwritePolicy == "idle") {
            // if the system is busy, wait for 60 seconds.
            scanIntv = 10;
        }

        // timed wait for 'start background' signal
        if (!(lastScan == -1 || lastScan + scanIntv <= time(NULL))) {
            pthread_mutex_lock(&self->_stagingBgWritePendingLock);
            time_t nextScanSec = lastScan + scanIntv;
            struct timespec nextScan = {nextScanSec, 0};
            DLOG(INFO) 
                    << BG_WRITE_TO_CLOUD_TAG
                    << "Sleep until" 
                    << (bgwritePolicy == "immediate"? " next foreground write or" : "")
                    << " next background write scan at " << ctime(&nextScanSec)
            ;
            pthread_cond_timedwait(&self->_stagingBgWritePending, &self->_stagingBgWritePendingLock, &nextScan);
            pthread_mutex_unlock(&self->_stagingBgWritePendingLock);

            if (!self->_running)
                return 0;
        }

        // update time of last scan
        lastScan = time(NULL);

        if (bgwritePolicy == "idle") {
            // Current implementation assumes the system is idle when cpu utilization is below 50% 
            SysInfo info;
            self->_coordinator->getProxyStatus(info);
            
            double avgCPUUsage = 0;
            for (unsigned int i = 0; i < (unsigned int) info.cpu.num; i++)
                avgCPUUsage += info.cpu.usage[i];

            // skip background write if the system is busy
            //if (!(avgCPUUsage / info.cpu.num < 50) && (info.mem.free * 1.0 / info.mem.total < 0.5)) {
            if (avgCPUUsage / info.cpu.num >= 50)
                continue;
        } else if (bgwritePolicy == "none") {
            continue;
        }

        // pop files pending for backgroud write
        while(self->_running) {
            File wf[1];
            int numFiles = self->_metastore->getFilesPendingWriteToCloud(1, wf);

            if (numFiles <= 0) {
                DLOG(INFO) << BG_WRITE_TO_CLOUD_TAG << "No Pending files to write";
                break;
            }

            if (self->bgwriteFileToCloud(*wf)) {
                LOG(INFO) << BG_WRITE_TO_CLOUD_TAG << "Background write task added, file: " << wf->name;
            } else {
                LOG(ERROR) << BG_WRITE_TO_CLOUD_TAG << "Failed to add background write task, file: " << wf->name;
            }

            // TODO rate limiting (avoid overload)
        }

        // update time of last scan (at the end of scan, to avoid scan interval < background write time)
        lastScan = time(NULL);
    }

    return NULL;
}

bool Proxy::bgwriteFileToCloud(File &f) {
    DLOG(INFO) << "Now writes file " << f.name << " back to cloud";

    time_t start = time(NULL);

    boost::timer::cpu_timer mytimer;
    
    File rf, wf;
    rf.copyNameAndSize(f);
    wf.copyNameAndSize(rf);

    // Get the file timestamps from staging
    FileInfo rfinfo;
    rf.copyNameToInfo(rfinfo);
    
    bool staged = _staging->getFileInfo(rfinfo);

    // very recently modified, wait for a while
    int scanIntv = Config::getInstance().getProxyStagingBackgroundWriteScanInterval();
    if (!staged || rfinfo.mtime + scanIntv * 2 > time(NULL)) {
        _metastore->markFileAsPendingWriteToCloud(f);
        LOG(WARNING) << BG_WRITE_TO_CLOUD_TAG << "Skip " << (staged? "recently modified" : "non-existing") << " file " << rf.name;
        return false;
    }

    // get file metadata
    if (lockFileAndGetMeta(rf, "background write") == false) {
        _metastore->markFileAsPendingWriteToCloud(f);
        LOG(ERROR) << BG_WRITE_TO_CLOUD_TAG << "Failed to get metadata of file " << rf.name;
        return false;
    }

    unsigned long int fileSize = rf.staged.size;
    CodingMeta &codingMeta = rf.staged.codingMeta;
    rf.size = rf.staged.size;
    rf.codingMeta.copyMeta(rf.staged.codingMeta);
    rf.storageClass = rf.staged.storageClass;

    // get stripe size
    unsigned long stripeSize = _chunkManager->getMaxDataSizePerStripe(codingMeta.coding, codingMeta.n, codingMeta.k, codingMeta.maxChunkSize);
    if (stripeSize == INVALID_FILE_OFFSET) {
        _metastore->markFileAsPendingWriteToCloud(f);
        unlockFile(f);
        LOG(ERROR) << BG_WRITE_TO_CLOUD_TAG << "Failed to get stripe size for " << codingMeta.print();
        return false;
    }

    int numStripes = (fileSize / stripeSize) + (fileSize % stripeSize == 0? 0 : 1);
    
    // find the containers
    int *spareContainers = 0;
    int numSelected = 0; // no need to find selected containers
    if (prepareWrite(rf, wf, spareContainers, numSelected, /* needsFindSpareContainers */ false) == false) {
        _metastore->markFileAsPendingWriteToCloud(f);
        LOG(ERROR) << BG_WRITE_TO_CLOUD_TAG << "Failed to prepare for background write for file " << rf.name;
        unlockFile(f);
        return false;
    }

    // mark the range to read
    wf.offset = 0;
    wf.size = rf.staged.size;
    wf.length = wf.size;
    wf.numStripes = numStripes;

    mytimer.start();

    // TODO process stripe-by-stripe

    if (!_staging->readFile(wf)) {
        _metastore->markFileAsPendingWriteToCloud(f);
        unlockFile(f);
        LOG(ERROR) << "Failed to read file " << f.name << " from staging for write back to cloud";
        return false;
    }
    
    boost::timer::cpu_times duration = mytimer.elapsed();
    LOG(INFO) << "Read file " << f.name << " from staging, (data) speed = " << (wf.length * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << wf.size * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";

    wf.version = rf.size == INVALID_FILE_LENGTH? 0 : rf.version + 1;

    // mark the range and coding for write
    rf.offset = 0;
    rf.length = rf.size;

    // write data back in stripes
    if (!writeFileStripes(rf, wf, spareContainers, numSelected)) {
        _metastore->markFileAsPendingWriteToCloud(f);
        unlockFile(f);
        LOG(ERROR) << "Failed to write file " << f.name << " back to cloud";
        return false;
    }
    
    duration = mytimer.elapsed();
    LOG_IF(INFO, duration.wall > 0) << "Write back file " << f.name << " to cloud, (data) speed = " << (wf.length * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << wf.size * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";

    // TODO process stripes in parallel

    mytimer.start();

    // update metadata
    if (_metastore->putMeta(wf) == false) {
        _metastore->markFileAsPendingWriteToCloud(f);
        unlockFile(f);
        LOG(ERROR) << "Failed to update file metadata of file " << f.name << " during write back";
        return false;
    }

    LOG(INFO) << "Write back file " << f.name << ", (meta) duration = " << (mytimer.elapsed().wall * 1.0 / 1e6) << " millseconds)";
    LOG(INFO) << "Write back file " << f.name << ", completes";

    _metastore->markFileAsWrittenToCloud(f);
    unpinStagedFile(f);
    unlockFile(f);

    // record the operation
    const std::map<std::string, double> stats = genStatsMap(duration, mytimer.elapsed(), wf.size);
    _statsSaver.saveStatsRecord(stats, "write staged file", std::string(wf.name, wf.nameLength), start, time(NULL));
    return true;
}

std::map<std::string, double> Proxy::genStatsMap(const boost::timer::cpu_times &dataT, const boost::timer::cpu_times &metaT, const unsigned long int &dataSize) const {
    std::map<std::string, double> stats;

    stats["meta (ms)"] = metaT.wall * 1.0 / 1e6;
    stats["data (s)"] = dataT.wall * 1.0 / 1e9;
    stats["data (MB/s)"] = (dataSize * 1.0 / (1 << 20)) / (dataT.wall * 1.0 / 1e9);
    stats["fileSize"] = (dataSize * 1.0 / (1 << 20));

    return stats;
}


Proxy::StripeLocation::StripeLocation() {
    reset();
}

Proxy::StripeLocation::StripeLocation(const std::string name, const unsigned long int offset) {
    _objectName = name;
    _offset = offset;
}

void Proxy::StripeLocation::reset() {
    _objectName = "";
    _offset = INVALID_FILE_OFFSET;
}
