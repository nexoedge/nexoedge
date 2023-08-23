// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h> // malloc(), remalloc()

#include <map>

#include <glog/logging.h>
#include <boost/timer/timer.hpp>

#include "chunk_manager.hh"
#include "../common/coding/all.hh"
#include "../common/coding/coding_generator.hh"

#include "../common/benchmark/benchmark.hh"


ChunkManager::ChunkManager(std::map<int, std::string> *containerToAgentMap, ProxyIO *io, BgChunkHandler *handler, MetaStore *metastore) {
    Config &config = Config::getInstance();
    
    // initialize storage classes
    std::set<std::string> classes = config.getStorageClasses();
    std::set<std::string>::iterator it = classes.begin(), ed = classes.end();
    Coding *code = 0;
    for ( ; it != ed; it++ ) {
        CodingOptions options;
        options.setN(config.getN(*it));
        options.setK(config.getK(*it));
        int f = config.getF(*it);
        int maxChunkSize = config.getMaxChunkSize(*it);
        int coding = config.getCodingScheme(*it);
        try {
            code = CodingGenerator::genCoding(coding, options);
        } catch (std::invalid_argument &e) {
            LOG(FATAL) << "Cannot init storage class " << *it;
            exit(1);
        }
        _codings.insert(std::make_pair(genCodingInstanceKey(coding, options.getN(), options.getK()), code));
        _storageClasses.insert(std::make_pair(*it, new StorageClass(*it, f, maxChunkSize, coding, code)));
        DLOG(INFO) << "Init storage class [" << *it << "] with options " << options.str();
    }
    code = 0;

    // initialize sub-modules
    _io = io;
    _eventCount = 0;
    _containerToAgentMap = containerToAgentMap;
    _bgChunkHandler = handler;
    _metastore = metastore;
}

ChunkManager::~ChunkManager() {
    LOG(WARNING) << "Terminating Chunk Manager ...";
    // free storage classes and coding instances
    for (auto it = _storageClasses.begin(); it != _storageClasses.end(); it++) {
        delete it->second;
        it->second = 0;
    }
    std::lock_guard<std::mutex> lkg (_codingsLock);
    for (auto it = _codings.begin(); it != _codings.end(); it++) {
        delete it->second;
        it->second = 0;
    }
    LOG(WARNING) << "Terminated Chunk Manager";
}

bool ChunkManager::writeFileStripe(File &file, int spareContainers[], int numSpare, bool alignDataBuf, bool isOverwrite, bool withEncode) {

    // benchmark
    BMWrite *bmWrite = dynamic_cast<BMWrite *>(Benchmark::getInstance().at(file.reqId));
    BMWriteStripe *bmStripe = NULL;
    bool benchmark = bmWrite && bmWrite->isStripeOn();
    if (benchmark) {
        bmStripe = &(bmWrite->at(file.stripeId));
    }

    // get coding instance
    Coding *coding = getCodingInstance(file.storageClass);
    if (coding == NULL) {
        return false;
    }

    bool storeCodeChunksOnly = coding->storeCodeChunksOnly();
    int numDataChunks = coding->getNumDataChunks();
    int numCodeChunks = coding->getNumCodeChunks();
    int numChunksPerNode = coding->getNumChunksPerNode();

    // perform encoding before write if necessary
    unsigned char *codebuf = 0; 
    if (withEncode) {
        // allocate code chunks buffers
        int chunkSize = coding->getChunkSize(file.length);
        codebuf = (unsigned char *) malloc (numCodeChunks * (chunkSize));
        if (codebuf == NULL) {
            LOG(ERROR) << "Failed to allocate buffer for code chunks of size " << ((unsigned long int) chunkSize) * numCodeChunks;
            return false;
        }
        boost::timer::cpu_timer mytimer;
        // encode
        if (!encodeFile(file, spareContainers, numSpare, alignDataBuf, codebuf)) {
            LOG(ERROR) << "<WRITE> Error encoding file";
            return false;
        }
        if (file.reqId == -1) {
            boost::timer::cpu_times duration = mytimer.elapsed();
            LOG_IF(INFO, duration.wall > 0) << "Write file " << file.name << ", finish encoding speed = " << (file.length * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
                    << "(" << file.length * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
        }
    }
    
    // TAGPT (start): prepare chunks
    if (benchmark) { 
        bmStripe->prepareChunks.markStart();
    }

    // distribute the chunks (evenly)
    bool bgack = Config::getInstance().ackRedundancyInBackground();
    bool bgwrite = Config::getInstance().writeRedundancyInBackground();
    int numReqs = ((storeCodeChunksOnly? 0 : numDataChunks) + numCodeChunks) / numChunksPerNode;
    int numFgReqs = bgack? numDataChunks / numChunksPerNode : numReqs;
    int numBgReqs = numSpare / numChunksPerNode - numFgReqs;

    pthread_t *wt = 0;
    ProxyIO::RequestMeta *meta = 0;
    ChunkEvent *events = 0;
    try {
        wt = new pthread_t[numReqs];
        meta = new ProxyIO::RequestMeta[numReqs];
        events = new ChunkEvent[numReqs * 2];
    } catch (std::bad_alloc &e) {
        delete [] wt;
        delete [] meta;
        delete [] events;
        LOG(ERROR) << "Failed to allocate memoryy for events metadata and threads";
        return false;
    }

    DLOG(INFO) << "Write file " << file.name << ", issue " << numReqs 
        << " requests for block " << file.blockId << " stripe " << file.stripeId;

    // TAGPT (end): prepare chunks
    if (benchmark) {
        bmStripe->prepareChunks.markEnd();
        bmStripe->networkRT.markStart();
    }

    boost::timer::cpu_timer mytimer;

    // send chunk requests in a node-based manner
    for (int i = 0; i < numReqs; i++) {        
        events[i].id = _eventCount.fetch_add(1);
        events[i].opcode = Opcode::PUT_CHUNK_REQ;
        events[i].numChunks = numChunksPerNode;
        try {
            events[i].chunks = new Chunk[numChunksPerNode];
        } catch (std::bad_alloc &e) {
            delete [] wt;
            delete [] meta;
            delete [] events;
            LOG(ERROR) << "Failed to allocate memory for event chunks";
            return false;
        }
        try {
            events[i].containerIds = new int[numChunksPerNode];
        } catch (std::bad_alloc &e) {
            delete [] wt;
            delete [] meta;
            delete [] events;
            delete [] events[i].chunks;
            LOG(ERROR) << "Failed to allocate memory for event container ids";
            return false;
        }
        for (int j = 0; j < numChunksPerNode; j++) {
            int chunkIdx = i * numChunksPerNode + j;
            // compute checksum (and send to agent for verification)
            file.chunks[chunkIdx].computeMD5();
            events[i].chunks[j] = file.chunks[chunkIdx];
            // never free data reference copied from (and is held by) others
            events[i].chunks[j].freeData = false;
            events[i].containerIds[j] = i < numSpare? spareContainers[i] : INVALID_CONTAINER_ID;

            // journal the upcoming write change
            //if (events[i].containerIds[j] != INVALID_CONTAINER_ID && _metastore && !_metastore->addChunkToJournal(file, events[i].chunks[j], events[i].containerIds[j], /* isWrite */ true)) {
            //    LOG(ERROR) << "Failed to journal the chunk change of file " << file.name << " chunk " << events[i].chunks[j].getChunkId() << " in container " << events[i].containerIds[j];
            //    return false;
            //}
        }

        if (i >= numSpare)
            continue;

        meta[i].containerId = spareContainers[i];
        meta[i].io = _io;
        meta[i].request = &events[i];
        meta[i].reply = &events[i + numReqs];

        // TAGPT: benchmark network time
        if (benchmark && bmStripe->network && bmStripe->network->size() > (size_t) i) {
            meta[i].network = &(bmStripe->network->at(i));
        }

        // send the requests in separate threads
        if (!bgwrite || i < numFgReqs)
            pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);
    }

    DLOG(INFO) << "Write file " << file.name << ", finish issuing chunk requests for block " << file.blockId << ", stripe " << file.stripeId;
    
    // check replies and gather the container id to file
    // TODO handle partial success, e.g., remove chunk already set?
    bool allsuccess = true;
    int numSuccess = 0;
    bool chunkIndicator[numDataChunks + numCodeChunks] = { false };
    try {
        file.containerIds = new int[numDataChunks + numCodeChunks];
    } catch (std::bad_alloc &e) {
        delete [] wt;
        delete [] meta;
        delete [] events;
        LOG(ERROR) << "Failed to allocate memory for container Ids";
        return false;
    }
    for (int i = 0; i < numDataChunks + numCodeChunks; i++)
        file.containerIds[i] = INVALID_CONTAINER_ID;

    // TAGPT (start): benchmark reply check time
    if (benchmark) {
        bmStripe->replyCheck.markStart();
    }

    // check the replies
    for (int i = 0; i < numReqs; i++) {
        void *ptr = 0;
        if (i < numSpare) {
            // check until the number of sent requests reaches the required minimum and the number of foreground requests
            if (numSuccess < numDataChunks || i < numFgReqs) {
                // some foreground request failed, and need to move some background one to foreground
                if (i > numDataChunks)
                    numBgReqs--;
                // issue the request if it was designated to background
                if (bgwrite && i > numFgReqs)
                    pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);
                pthread_join(wt[i], &ptr);
                // proxy internal error
                if (ptr != 0) {
                    long errNum = static_cast<long>(reinterpret_cast<unsigned long>(ptr));
                    LOG(ERROR) << "Failed to store chunk of file " << file.name << " due to internal failure (error = " << errNum << "), container id = " << meta[i].containerId;
                    if (errNum == -2) { // failed to receive a reply
                    }
                    allsuccess = allsuccess && meta[i].containerId == INVALID_CONTAINER_ID;
                    for (int j = 0; j < numChunksPerNode; j++) {
                        if (meta[i].containerId == INVALID_CONTAINER_ID) { continue; }
                        int chunkIdx = i * numChunksPerNode + j;
                        // journal the write failures
                        //if (_metastore && !_metastore->updateChunkInJournal(file, events[i].chunks[j], /* isWrite */ true, /* deleteRecord */ false, meta[i].containerId)) {
                        //    LOG(ERROR) << "Failed to mark chunk write failure in the journal of file " << file.name << " chunk " << events[i].chunks[j].getChunkId() << ".";
                        //}
                        // TODO not necessary(?)
                        file.containerIds[chunkIdx] = INVALID_CONTAINER_ID;
                    }
                    continue;
                }
            }
            
            // mark the location of written chunks
            for (int j = 0; j < numChunksPerNode; j++) {
                int chunkIdx = i * numChunksPerNode + j;
                // verify chunk checksum if needed
                bool checksumPassed = 
                        !Config::getInstance().verifyChunkChecksum() || 
                        (
                            meta[i].reply->opcode == Opcode::PUT_CHUNK_REP_SUCCESS && 
                            memcmp(file.chunks[chunkIdx].md5, events[i + numReqs].chunks[j].md5, MD5_DIGEST_LENGTH) == 0
                        );
                // mark the container id when either 
                // (1) it is going to complete in the background
                // (2) it has completed successfully in the foreground
                if (i >= numSpare / numChunksPerNode - numBgReqs) { // background request
                    file.containerIds[chunkIdx] = spareContainers[i];
                } else if (meta[i].reply->opcode == Opcode::PUT_CHUNK_REP_SUCCESS && checksumPassed) { // foreground successful request
                    file.containerIds[chunkIdx] = events[i + numReqs].containerIds[j];
                    memcpy(file.chunks[chunkIdx].chunkVersion, events[i + numReqs].chunks[j].chunkVersion, CHUNK_VERSION_MAX_LEN);
                    numSuccess++;

                    // TAGPT: benchmark agent process time
                    if (benchmark && bmStripe->agentProcess && bmStripe->agentProcess->size() > (size_t) i) {
                        bmStripe->agentProcess->at(i) = meta[i].reply->agentProcess;
                    }

                    LOG(INFO) << "Write file " << file.name 
                        << ", finish " << numSuccess * 100.0 / (numFgReqs * numChunksPerNode) << "\% of " 
                        << (bgwrite || bgack? "foreground " : "") << "requests"
                        << " for stripe " << file.stripeId 
                        << " (chunk " << i << ", container id = " << file.containerIds[chunkIdx] << ")";
                
                } else { // foreground failed request
                    file.containerIds[chunkIdx] = INVALID_CONTAINER_ID;
                    LOG(ERROR) << "Failed to put chunk id = " << i << " due to failure at agent for container id = " << spareContainers[i] << ", rep = " << (int) meta[i].reply->opcode;
                    allsuccess = allsuccess && meta[i].containerId == INVALID_CONTAINER_ID;
                }
                // treat the both two types of chunks above as alive
                // note that we still mark chunks with mismatched checksum as okay here, so they are delete/revert upon failure
                chunkIndicator[chunkIdx] = i >= numSpare - numBgReqs || meta[i].reply->opcode == Opcode::PUT_CHUNK_REP_SUCCESS;

                // journal the write completion
                //if (_metastore && !_metastore->updateChunkInJournal(file, events[i].chunks[j], /* isWrite */ true, /* deleteRecord */ false, meta[i].containerId)) {
                //    LOG(ERROR) << "Failed to journal the chunk change of file " << file.name << " chunk " << events[i].chunks[j].getChunkId() << ".";
                //}
            }
        } else {
            // mark the unwritten chunks as invalid for degraded writes
            for (int j = 0; j < numChunksPerNode; j++) {
                chunkIndicator[i * numChunksPerNode + j] = false;
                file.containerIds[i * numChunksPerNode + j] = INVALID_CONTAINER_ID;
            }
        }
    }

    if (!benchmark) {
        int numChunksPerContainer = getNumChunksPerContainer(file.storageClass);
        boost::timer::cpu_times duration = mytimer.elapsed();
        LOG_IF(INFO, duration.wall) << "Write file " << file.name << ", finish writing chunks (foreground) speed = " 
                << (file.chunks[0].size * numChunksPerContainer * numSuccess * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
                << "(" << file.chunks[0].size * numChunksPerContainer * numSuccess * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
    }

    if (numBgReqs > 0) {
        // check the reply in the background
        try {
            File *bgfile = new File();
            bgfile->status = FileStatus::BG_TASK_PENDING;
            bgfile->copyAllMeta(file);
            BgChunkHandler::ChunkTask task(PUT_CHUNK_REQ, bgfile, numSpare / numChunksPerNode, numBgReqs, wt, meta, events, codebuf);
            LOG(INFO) << "Put task with " << numBgReqs << " requests into background";
            _bgChunkHandler->addChunkTask(task);
        } catch (std::bad_alloc &e) {
            delete [] wt;
            delete [] meta;
            delete [] events;
            LOG(ERROR) << "Failed to allocate memory for background task with " << numBgReqs << " requests";
        }
    } else {
        // if all requests are done in foreground, clean up now
        free(codebuf);
        delete [] wt;
        delete [] meta;
        delete [] events;
    }

    // report fail if the amount of data stored is less than file size (without any redundancy)
    if (isOverwrite && !allsuccess) {
        LOG(WARNING) << "Failed to overwrite file " << file.name << ", going to revert partial uploaded data now.";
        revertFile(file, chunkIndicator);
        return false;
    } else if (!allsuccess && numSuccess < numDataChunks) {
        LOG(WARNING) << "Failed to append file " << file.name << ", going to remove partial uploaded data now.";
        deleteFile(file, chunkIndicator);
        return false;
    }

    // TAGPT (end): benchmark reply check time
    if (benchmark) {
        bmStripe->replyCheck.markEnd();
        bmStripe->networkRT.markEnd();
    }

    return true;
}

bool ChunkManager::encodeFile(File &file, int spareContainers[], int numSpare, bool alignDataBuf, unsigned char *codebuf) {
    CodingMeta &codingMeta = file.codingMeta;
    int fcoding = codingMeta.coding;

    // get coding instance
    Coding *coding = getCodingInstance(fcoding, codingMeta.n, codingMeta.k);
    if (coding == NULL) {
        return false;
    }

    int chunkSize = coding->getChunkSize(file.length);
    int numDataChunks = coding->getNumDataChunks();
    int numCodeChunks = coding->getNumCodeChunks();
    int numChunksPerNode = coding->getNumChunksPerNode();

    // TODO support uneven (or arbitrary) distribution of chunks
    if ((numDataChunks + numCodeChunks) % numChunksPerNode != 0) {
        LOG(ERROR) << "Failed to evenly distribute chunks\n";
        return false;
    }

    // data chunks, reallocate to an aligned size if needed
    unsigned char *databuf = file.data;
    if (alignDataBuf && (unsigned int) chunkSize * numDataChunks > file.length) {
        databuf = (unsigned char *) realloc (file.data, chunkSize * numDataChunks);
        if (databuf == NULL) {
            LOG(ERROR) << "Failed to allocate buffer for data chunks of size " << ((unsigned long int) chunkSize) * numDataChunks;
            return false;
        }
        memset(databuf + file.length, 0, chunkSize * numDataChunks - file.length);
        file.data = databuf;
    }

    // mark whether the codebuf is managed externally by the caller
    bool isCodeBufLocal = codebuf == NULL;
    // coded chunks, allocate (and free at the end) if caller does not provide any external buffer
    if (codebuf == NULL)
        codebuf = (unsigned char *) malloc (numCodeChunks * (chunkSize));
    if (codebuf == NULL) {
        LOG(ERROR) << "Failed to allocate buffer for code chunks of size " << ((unsigned long int) chunkSize) * numCodeChunks;
        return false;
    }

    unsigned long int encodingSize = file.length;

    // coding metadata
    file.codingMeta.codingStateSize = coding->getCodingStateSize();
    if (file.codingMeta.codingStateSize > 0) {
        file.codingMeta.codingState = new unsigned char [file.codingMeta.codingStateSize];
        if (file.codingMeta.codingState == 0) {
            LOG(ERROR) << "Failed to allocate buffer for coding state of size " << file.codingMeta.codingStateSize;
            if (isCodeBufLocal) free(codebuf);
            return false;
        }
    }
    
    // encode
    std::vector<Chunk> stripe;
    if (coding->encode(file.data, encodingSize, stripe, &file.codingMeta.codingState) == false) {
        LOG(ERROR) << "Failed to encode data of size " << file.length << " of " << file.size;
        if (isCodeBufLocal) free(codebuf);
        return false;
    }

    // prepare chunks in file by referencing them to buffer
    file.numChunks = stripe.size();
    try {
        file.chunks = new Chunk[file.numChunks];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate buffer for chunks of size " << sizeof(Chunk) * file.numChunks;
        if (isCodeBufLocal) free(codebuf);
        return false;
    }
    int chunkIdOffset = file.offset / getMaxDataSizePerStripe(fcoding, coding->getN(), coding->getK(), codingMeta.maxChunkSize) * file.numChunks;
    for (int i = 0; i < file.numChunks; i++) {
        file.chunks[i].move(stripe.at(i));
        file.chunks[i].setId(file.namespaceId, file.uuid, i + chunkIdOffset);
        file.chunks[i].fileVersion = file.version;
    }

    if (isCodeBufLocal) free(codebuf);

    return true;
}

bool ChunkManager::copyFile(File &srcFile, File &dstFile, int *start, int *end) {
    return fullFileModify(srcFile, dstFile, /* isCopy */ true, start, end);
}

bool ChunkManager::moveFile(File &srcFile, File &dstFile) {
    return fullFileModify(srcFile, dstFile, /* isCopy */ false);
}

bool ChunkManager::fullFileModify(File &srcFile, File &dstFile, bool isCopy, int *start, int *end) {
    // TODO check coding scheme if destination file can use a different scheme from the source file

    // skip data copying for empty file
    if (srcFile.numChunks <= 0 || srcFile.numStripes <= 0) {
        dstFile.size = srcFile.size;
        dstFile.numChunks = srcFile.numChunks;
        if (!dstFile.codingMeta.copyMeta(srcFile.codingMeta)) {
            LOG(ERROR) << "Failed to copy coding metadata, borrow from source file";
            dstFile.codingMeta = srcFile.codingMeta;
            srcFile.codingMeta.reset();
        }
        dstFile.numStripes = srcFile.numStripes;
        return true;
    }

    // clean the dst file 
    delete [] dstFile.chunks;
    delete [] dstFile.containerIds;

    dstFile.storageClass = srcFile.storageClass;

    try {
        dstFile.chunks = new Chunk[srcFile.numChunks];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate chunks for write";
        return false;
    }

    try {
        dstFile.containerIds = new int[srcFile.numChunks];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate memory for containerIds";
        delete [] dstFile.chunks;
        dstFile.chunks = 0;
        return false;
    }

    // get coding instance using coding scheme and options of source file
    Coding *coding = getCodingInstance(srcFile.codingMeta.coding, srcFile.codingMeta.n, srcFile.codingMeta.k);
    if (coding == NULL) {
        return false;
    }

    int numChunksPerNode = coding->getNumChunksPerNode();
    unsigned long int stripeSize = getMaxDataSizePerStripe(srcFile.codingMeta.coding, srcFile.codingMeta.n, srcFile.codingMeta.k, srcFile.codingMeta.maxChunkSize);
    int startIdx = srcFile.offset / stripeSize;
    int endIdx = (srcFile.offset + srcFile.length + stripeSize - 1) / stripeSize; // exclusive
    int numChunksPerStripe = srcFile.numChunks / srcFile.numStripes;

    // copy the chunks
    int numReqs = (endIdx - startIdx) * numChunksPerStripe / numChunksPerNode;
    int numReqsPerStripe = numChunksPerStripe / numChunksPerNode;
    pthread_t wt[numReqs];
    ProxyIO::RequestMeta meta[numReqs];
    ChunkEvent events[numReqs * 2];

    int numSuccess = 0, numTotalSuccess = 0;
    bool okay = true;
    bool chunkIndicator[srcFile.numChunks];

    for (int i = 0; i < numReqs; i++) {
        // prepare the copy chunk requests
        events[i].id = _eventCount.fetch_add(1);
        events[i].opcode = isCopy? Opcode::CPY_CHUNK_REQ : Opcode::MOV_CHUNK_REQ;
        events[i].numChunks = numChunksPerNode;
        try {
            events[i].chunks = new Chunk[numChunksPerNode * 2];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event chunks";
            free(dstFile.chunks);
            free(dstFile.containerIds);
            dstFile.chunks = 0;
            dstFile.containerIds = 0;
            return false;
        }
        try {
            events[i].containerIds = new int[numChunksPerNode];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event container ids";
            free(dstFile.chunks);
            free(dstFile.containerIds);
            dstFile.chunks = 0;
            dstFile.containerIds = 0;
            return false;
        }
        for (int j = 0; j < numChunksPerNode; j++) {
            // source
            events[i].chunks[j] = srcFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe];
            // destination
            dstFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe].setId(
                dstFile.namespaceId,
                dstFile.uuid,
                srcFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe].getChunkId()
            );
            dstFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe].size = 
                srcFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe].size;
            dstFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe].fileVersion = dstFile.version;
            events[i].chunks[j + numChunksPerNode] = dstFile.chunks[i * numChunksPerNode + j + startIdx * numChunksPerStripe];
            // container id
            events[i].containerIds[j] = srcFile.containerIds[i * numChunksPerNode + j + startIdx * numChunksPerStripe];
        }
        meta[i].containerId = srcFile.containerIds[i * numChunksPerNode + startIdx * numChunksPerStripe];
        meta[i].io = _io;
        meta[i].request = &events[i];
        meta[i].reply = &events[i + numReqs];

        // send the requests via threads
        pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);

        // continue issuing requests until the end of a stripe
        if ((i + 1) % numReqsPerStripe != 0) 
            continue;

        // check if we reach the end of one stripe
        for (int j = 0; j < numReqsPerStripe; j++) {
            void *ptr;
            int reqIdx = i - (numReqsPerStripe - 1) + j;
            pthread_join(wt[reqIdx], &ptr);
            if (ptr != 0) {
                LOG(ERROR) << "Failed to store chunk due to internal failure, container id = " << meta[reqIdx].containerId;
            }
            for (int k = 0; k < numChunksPerNode; k++) {
                int expectedOpRep = isCopy? Opcode::CPY_CHUNK_REP_SUCCESS : Opcode::MOV_CHUNK_REP_SUCCESS;
                int failedOp = isCopy? Opcode::CPY_CHUNK_REP_FAIL : Opcode::MOV_CHUNK_REP_FAIL;
                dstFile.containerIds[k + reqIdx * numChunksPerNode + startIdx * numChunksPerStripe] = meta[reqIdx].reply->opcode == expectedOpRep && ptr == 0? events[reqIdx + numReqs].containerIds[k] : INVALID_CONTAINER_ID;
                chunkIndicator[k + reqIdx * numChunksPerNode + startIdx * numChunksPerStripe] = meta[reqIdx].reply->opcode == expectedOpRep && ptr == 0;
                if (meta[reqIdx].reply->opcode == failedOp || ptr != 0) {
                    LOG(ERROR) << "Failed to " << (isCopy? "copy" : "move") << " chunk id = " << i << " due to failure at agent for container id = " << srcFile.containerIds[k + reqIdx * numChunksPerNode + startIdx * numChunksPerNode];
                    continue;
                }
                // verify the checksum if needed
                if (
                        Config::getInstance().verifyChunkChecksum() && 
                        memcmp(meta[reqIdx].reply->chunks[k].md5, srcFile.chunks[k + reqIdx * numChunksPerNode + startIdx * numChunksPerStripe].md5, MD5_DIGEST_LENGTH) != 0
                ) {
                    LOG(ERROR) << "Failed to " << (isCopy? "copy" : "move") << " chunk id = " << i << " due to failure at agent for container id = " << srcFile.containerIds[k + reqIdx * numChunksPerNode + startIdx * numChunksPerNode] << " chunk checksum mismatched";
                    continue;
                }
                dstFile.chunks[k + reqIdx * numChunksPerNode + startIdx * numChunksPerStripe].copyMD5(meta[reqIdx].reply->chunks[k]);
                numSuccess++;
                numTotalSuccess++;
            }
            LOG(INFO) << (isCopy ? "Copy" : "Move") << " chunk of size " << dstFile.chunks[reqIdx * numChunksPerNode + startIdx * numChunksPerStripe].size * numChunksPerNode;
            LOG(INFO) << (isCopy ? "Copy" : "Move") << " file " << dstFile.name << ", finish " << numTotalSuccess * 100.0 / numReqs << "% requests";
        }
        okay &= numSuccess >= srcFile.codingMeta.k;
        numSuccess = 0;
    }

    // remove chunk if any part of the request fails
    if (!okay) {
        LOG(WARNING) << "Failed to " << (isCopy? "Copy" : "Move") << " file " << dstFile.name << ", going to remove partial uploaded data now.";
        deleteFile(dstFile, chunkIndicator);
        delete [] dstFile.chunks;
        delete [] dstFile.containerIds;
        dstFile.chunks = 0;
        dstFile.containerIds = 0;
    } else {
        // copy the metadata
        dstFile.numChunks = endIdx == srcFile.numStripes? srcFile.numChunks : endIdx * srcFile.numChunks / srcFile.numStripes;
        dstFile.size = endIdx == srcFile.numStripes? srcFile.size : (srcFile.offset + srcFile.length + stripeSize - 1) / stripeSize * stripeSize;
        if (!dstFile.codingMeta.copyMeta(srcFile.codingMeta)) {
            LOG(ERROR) << "Failed to copy coding metadata, borrow from source file";
            dstFile.codingMeta = srcFile.codingMeta;
            srcFile.codingMeta.reset();
        }
        dstFile.numStripes = endIdx;
    }

    // pass the index to caller to copy data if needed
    if (start) *start = startIdx;
    if (end) *end = endIdx;

    return okay;
}

bool ChunkManager::readFile(File &file, bool chunkIndicator[], int **nodeIndicesOut, ChunkEvent **eventsOut, bool withDecode, DecodingPlan &plan) {
    // get coding instance
    Coding *coding = getCodingInstance(file.codingMeta.coding, file.codingMeta.n, file.codingMeta.k);
    if (coding == NULL) {
        return false;
    }

    // read the min number of chunks read, i.e., the number of data chunks
    int numChunks = coding->getNumDataChunks(), selected = 0;
    int numChunksPerNode = coding->getNumChunksPerNode();
    ChunkEvent *events = 0;
    try {
        events = new ChunkEvent[numChunks * 2];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate memory for " << numChunks * 2 << " chunk events";
        return false;
    }

    // figure out the failed chunks by going through the chunk liveness indicators
    std::vector<chunk_id_t> failedChunkIds;
    for (int i = 0; i < file.numChunks; i++) {
        if (chunkIndicator[i] == false) failedChunkIds.push_back(i);
    }

    // obtain the decoding plan from coding scheme
    bool decodable = coding->preDecode(failedChunkIds, plan, file.codingMeta.codingState);
    // set the number of selected chunk as that in output plan
    selected = plan.getNumInputChunks();

    // figure out the chunks available for decode, pack their ids into a list
    int *nodeIndices = 0;
    try {
        nodeIndices = new int[file.numChunks / numChunksPerNode];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate memory for " << file.numChunks / numChunksPerNode << " indices";
        delete [] events;
        events = 0;
        return false;
    }
    // construct the node indices from the selected chunk ids
    int chunkIndices[selected];
    std::vector<chunk_id_t> inputChunkIds = plan.getInputChunkIds();
    for (int i = 0; i < selected; i++) {
        chunkIndices[i] = inputChunkIds.at(i);
        if (i % numChunksPerNode == 0)
          nodeIndices[i / numChunksPerNode] = chunkIndices[i] / numChunksPerNode;
    }

    if (selected < numChunks || !decodable) {
        LOG(ERROR) << "Failed to find enough chunks (only " << selected << " alive, and need " << numChunks << ") for read";
        delete [] events;
        delete [] nodeIndices;
        events = 0;
        nodeIndices = 0;
        return false;
    }
    DLOG(INFO) << "Find enough chunks (" << selected << " alive out of " << numChunks << ") for read";

    boost::timer::cpu_timer mytimer;
    bool benchmark = file.reqId != -1;
    if (!accessChunks(events, file, plan.getMinNumInputChunks(), Opcode::GET_CHUNK_REQ, Opcode::GET_CHUNK_REP_SUCCESS, numChunksPerNode, chunkIndices, selected)) {
        LOG(ERROR) << "Failed to get some of the required chunks, need to handle degraded read or repair first";
        delete [] events;
        delete [] nodeIndices;
        events = 0;
        nodeIndices = 0;
        return false;
    }

    // update node indcies according to the chunk obtained
    for (int i = 0; i < numChunks / numChunksPerNode; i++)
        nodeIndices[i] = chunkIndices[i * numChunksPerNode] / numChunksPerNode;

    if (!benchmark) {
        // asusme all chunks are of same size, one chunk per request
        int chunkSize = events[numChunks].chunks[0].size;
        unsigned long int inputSize = chunkSize * numChunks;
        boost::timer::cpu_times duration = mytimer.elapsed();
        LOG_IF(INFO, duration.wall > 0) << "Read file " << file.name << ", finish retreiving chunks speed = " << (inputSize * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << inputSize * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
    }

    if (!withDecode) {  // early return if decode is not required
        *nodeIndicesOut = nodeIndices;
        *eventsOut = events;
        return true;
    }

    bool decodeSuccess = this->decodeFile(file, nodeIndices, events, plan);

    // clean up
    delete [] events;
    delete [] nodeIndices;
    events = 0;
    nodeIndices = 0;

    return decodeSuccess;
}

bool ChunkManager::decodeFile(File &file, int *nodeIndices, ChunkEvent *events, DecodingPlan &plan) {
    // get coding instance
    Coding *coding = getCodingInstance(file.codingMeta.coding, file.codingMeta.n, file.codingMeta.k);
    if (coding == NULL) {
        return false;
    }

    // assume all chunks are of same size, one chunk per request
    int chunkSize = events[0].chunks[0].size;
    int numChunks = coding->getNumDataChunks();
    unsigned long int inputSize = chunkSize * numChunks;

    // decode
    if (file.data == 0) {
        unsigned long int dataSize = coding->getChunkSize(file.size) * coding->getNumDataChunks();
        file.data = (unsigned char *) malloc (dataSize);
        if (file.data == NULL) {
            LOG(ERROR) << "Failed to allocate memory for file data";
            return false;
        }
    }

    boost::timer::cpu_timer mytimer;
    boost::timer::cpu_times duration;
    bool benchmark = file.reqId != -1;

    // pack the input chunks from events to an array
    std::vector<Chunk> inputChunks;
    inputChunks.resize(numChunks);
    for (int i = 0; i < numChunks; i++) {
        if (chunkSize != events[numChunks + i].chunks[0].size) {
            LOG(ERROR) << "Failed to gather input, chunk size mismatched ([" << i
                       << "] = " << events[numChunks + i].chunks[0].size 
                       << " vs [0] = " << chunkSize;
            return false;
        }
        inputChunks.at(i).move(events[numChunks + i].chunks[0]);
        inputChunks.at(i).setChunkId(inputChunks.at(i).chunkId % coding->getNumChunks());
    }

    if (!benchmark) {
        duration = mytimer.elapsed();
        LOG_IF(INFO, duration.wall) << "Read file " << file.name << ", finish rearranging chunks speed = " << (inputSize * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
                << "(" << inputSize * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";

        mytimer.start();
    }

    unsigned int decodedSize = 0;
    bool decodeSuccess = coding->decode(inputChunks, &file.data, decodedSize, plan, file.codingMeta.codingState);
    if (!decodeSuccess)
        LOG(ERROR) << "Failed to decode";

    file.length = decodedSize;

    if (!benchmark) {
        duration = mytimer.elapsed();
        LOG_IF(INFO, duration.wall) << "Read file " << file.name << ", finish decoding speed = " << (inputSize * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
                << "(" << inputSize * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
    }

    return decodeSuccess;
}

bool ChunkManager::readFileStripe(File &file, bool chunkIndicator[]) {
    DecodingPlan plan;
    return this->readFile(file, chunkIndicator, NULL, NULL, true, plan);
}

bool ChunkManager::deleteFile(const File &file, bool chunkIndicator[]) {
    return operateOnAliveChunks(file, chunkIndicator, Opcode::DEL_CHUNK_REQ, Opcode::DEL_CHUNK_REP_SUCCESS);
}

bool ChunkManager::operateOnAliveChunks(const File &file, bool chunkIndicator[], Opcode reqOp, Opcode expectedOpRep) {
    if (file.numChunks < 0) {
        return false;
    }

    // no chunks to remove
    if (file.numChunks == 0) {
        return true;
    }

    ChunkEvent events[file.numChunks * 2];
    int selected = 0;
    Coding *coding = getCodingInstance(file.codingMeta.coding, file.codingMeta.n, file.codingMeta.k);
    int numChunksPerNode = coding? coding->getNumChunksPerNode() : 1;

    // only operate on alive chunks
    int chunkIndices[file.numChunks];
    for (int i = 0; i < file.numChunks / numChunksPerNode; i++) {

        // check and skip non-alive chunks
        if (chunkIndicator != NULL && chunkIndicator[i * numChunksPerNode] == false) {
            continue;
        }
        for (int j = 0; j < numChunksPerNode; j++, selected++) {
            // unselect this chunk if any sub-chunk failed
            if (chunkIndicator != NULL && chunkIndicator[i * numChunksPerNode + j] == false) { 
                selected -= j;
                break;
            }
            chunkIndices[selected] = i * numChunksPerNode + j;
        }
    }

    // operate on chunks in batches to avoid opening too many connections at the same time for files with lots of chunks
    bool okay = true;
    for (int i = 0, inc = 0; i < selected; i+= inc) {
        inc = std::min(selected - i, Config::getInstance().getN());
        okay &= accessChunks(events + i * 2, file, inc, reqOp, expectedOpRep, numChunksPerNode, chunkIndices + i);
    }

    return okay;
}

bool ChunkManager::repairFile(File &file, bool *chunkIndicator, int *spareContainers, int *chunkGroups, int numChunkGroups) {

    // benchmark
    BMRepair *bmRepair = dynamic_cast<BMRepair*>(Benchmark::getInstance().at(file.reqId));
    BMRepairStripe *bmStripe = NULL;
    bool benchmark = bmRepair && bmRepair->isStripeOn();
    if (benchmark) 
        bmStripe = &(bmRepair->at(file.stripeId));

    // get coding instance
    Coding *coding = getCodingInstance(file.codingMeta.coding, file.codingMeta.n, file.codingMeta.k);
    if (coding == NULL) {
        LOG(INFO) << "Failed to find the coding instance for " << file.codingMeta.print();
        return false;
    }

    int numChunksPerNode = coding->getNumChunksPerNode();
    int numFailedNodes = 0, numInputChunks = 0;
    int numDataChunks = coding->getNumDataChunks();
    int numCodeChunks = coding->getNumCodeChunks();

    std::vector<chunk_id_t> failedChunkIds;
    int failedNodes[coding->getN()] = { 0 };
    int inputChunkIndices[numDataChunks + numCodeChunks] = { false };

    // check node liveness
    for (int i = 0; i < file.numChunks; i++) {
        // alive chunk 
        if (chunkIndicator[i] == true)
            continue;
        // failed node
        if (i % numChunksPerNode == 0) {
          failedNodes[numFailedNodes++] = i / numChunksPerNode;
        }
        // failed chunk
        failedChunkIds.push_back(i);
        DLOG(INFO) << "Failed chunk " << i << " detected";
    }
    DLOG(INFO) << "Num. of failed nodes " << numFailedNodes << " detected";

    // no node failure, no need to repair file
    if (failedChunkIds.empty()) return true;

    DecodingPlan plan;
    std::string extraInfo;

    // get the information on inputs for repair
    if (coding->preDecode(failedChunkIds, plan, file.codingMeta.codingState, /* is repair */ true) == false) {
        LOG(ERROR) << "Failed to figure out a repair plan";
        return false;
    }

    // set information according to decoding plan
    unsigned char *repairMatrix = plan.getRepairMatrix();
    std::vector<chunk_id_t> inputChunkIds = plan.getInputChunkIds();
    numInputChunks = plan.getMinNumInputChunks();
    for (int i = 0; i < numInputChunks; i++) {
        inputChunkIndices[i] = inputChunkIds.at(i);
    }

    bool isRepairAtProxy = Config::getInstance().isRepairAtProxy() || numFailedNodes > 1;
    bool isRepairUsingCAR = Config::getInstance().isRepairUsingCAR() && numFailedNodes == 1;
    int numFailedChunks = numFailedNodes * numChunksPerNode;
    // number of failed chunks can be greater than input, e.g., replication
    int maxNumChunkReqs = std::max(numInputChunks, numFailedChunks);
    ChunkEvent events[maxNumChunkReqs * 3];
    // prepare the (meta) information for repair
    std::string submatrix;
    int numSubChunkGroups = 0;
    int subChunkGroups[numInputChunks * (numInputChunks + 1)];
    int subContainerGroups[numInputChunks];
    switch (file.codingMeta.coding) {
        case CodingScheme::RS:
            if (isRepairUsingCAR) { // single failure, encode partial chunks for decode
                std::map<int, int> selectedChunks; // chunk id to index at inputChunkIndices
                // update the chunk group according to selected chunks
                for (int i = 0; i < numInputChunks; i++) {
                    selectedChunks.insert(std::pair<int, int>(inputChunkIds.at(i), i));
                }
                // scan each chunk group, add a chunk into the submap if it is selected as the input
                // break early if all input chunks are find in the chunk groups scanned so far
                for (int i = 0, pmatrixSize = 0; i < numChunkGroups && submatrix.size() < (size_t) numInputChunks; i++) {
                    pmatrixSize = submatrix.size();
                    if (isRepairAtProxy) {
                        subChunkGroups[numSubChunkGroups * (numInputChunks + 1)] = 0;
                    } else {
                        subChunkGroups[pmatrixSize + numSubChunkGroups] = 0;
                    }
                    for (int j = 0; j < chunkGroups[i * (file.numChunks + 1)]; j++) {
                        int cid = chunkGroups[i * (file.numChunks + 1) + j + 1];
                        if (selectedChunks.count(cid) <= 0)
                            continue; 
                        // one more chunk is in
                        if (isRepairAtProxy) {
                            int &gcidx = subChunkGroups[numSubChunkGroups * (numInputChunks + 1)];
                            subChunkGroups[numSubChunkGroups * (numInputChunks + 1) + gcidx + 1] = cid;
                            gcidx++;
                            //DLOG(INFO) << "Selected chunk id = " << cid << " in group id = " << numSubChunkGroups << " at index " << gcidx;
                        } else {
                            subChunkGroups[numSubChunkGroups + submatrix.size() + 1] = file.chunks[cid].getChunkId();
                            subContainerGroups[submatrix.size()] = file.containerIds[cid];
                            subChunkGroups[pmatrixSize + numSubChunkGroups]++;
                            /*
                            DLOG(INFO) << "Selected chunk id = " << cid << " in group id = " << numSubChunkGroups
                                << " subChunkGroup [" << numSubChunkGroups + submatrix.size() + 1 << "] = " << file.chunks[cid].getChunkId()
                                << " subContainerGroup [" << submatrix.size() << "] = " << file.containerIds[cid]
                                << " Num chunks in group [" << pmatrixSize + numSubChunkGroups << "] = " <<  subChunkGroups[pmatrixSize + numSubChunkGroups];
                            */
                        }
                        // update the submatrix
                        submatrix.append(1, repairMatrix[selectedChunks.at(cid)]);
                    }
                    // open a new group for next chunk group, this group has some selected chunk in the submap
                    if (subChunkGroups[(isRepairAtProxy? numSubChunkGroups * (numInputChunks + 1) : pmatrixSize + numSubChunkGroups)] > 0) {
                        numSubChunkGroups++;
                        if (!isRepairAtProxy) {
                            // indicate the agent address
                            try {
                                events[0].agents.append(_containerToAgentMap->at(subContainerGroups[pmatrixSize]));
                                events[0].agents.append(";");
                            } catch (std::exception &e) {
                                LOG(ERROR) << "Failed to find agent address for container id = " << subContainerGroups[pmatrixSize];
                                return false;
                            }
                        }
                    }
                }
                // mark single-node failure to indicate CAR should be used
                extraInfo.clear();
                extraInfo.append(1, failedNodes[0]);
                break;
            }
            // nothing special to do for repair at Proxy
            if (isRepairAtProxy) {
                break;
            }
            // for repairing at Agent,
            // copy the whole matrix
            submatrix = std::string((char*) repairMatrix, (size_t) plan.getRepairMatrixSize());
            // input chunk ids, container ids, agent address (input and replacement nodes, except where chunk repair takes place)
            // 1 subChunkGroup, with all input chunks
            numSubChunkGroups = 1;
            subChunkGroups[0] = numInputChunks;
            for (int i = 0; i < numInputChunks + numFailedNodes - 1; i++) {
                if (i < numInputChunks) {
                    // chunk id
                    subChunkGroups[i + 1] = file.chunks[inputChunkIndices[i]].getChunkId();
                    // container id
                    subContainerGroups[i] = file.containerIds[inputChunkIndices[i]]; 
                }
                // agent address
                try {
                    int cid = i < numInputChunks? subContainerGroups[i] : spareContainers[i - numInputChunks + 1];
                    events[0].agents.append(_containerToAgentMap->at(cid));
                    events[0].agents.append(";");
                } catch (std::exception &e) {
                    LOG(ERROR) << "Failed to find agent address for container id = " << subContainerGroups[i];
                    return false;
                }
            }
            break;

        default:
            LOG(ERROR) << "Failed to prepare metadata for unknown coding scheme " << (int) file.codingMeta.coding;
            return false;
    }

    // start of repairing
    if (isRepairAtProxy) { // repair at Proxy
        switch (file.codingMeta.coding) {
            case CodingScheme::RS:
                if (isRepairUsingCAR) {
                    // request encoded chunks from agents
                    if (!accessGroupedChunks(events, file.containerIds, numInputChunks, subChunkGroups, numSubChunkGroups, file.namespaceId, file.uuid, submatrix, file.chunks[0].getChunkId())) {
                        LOG(ERROR) << "Failed to read partial encoded chunks for repair";
                        return false;
                    }
                    // the partial encoded chunks are now input chunks for further decoding
                    numInputChunks = numSubChunkGroups;
                    break;
                }
                // collect alive chunks from agents
                if (!accessChunks(events, file, numInputChunks, Opcode::GET_CHUNK_REQ, Opcode::GET_CHUNK_REP_SUCCESS, numChunksPerNode, inputChunkIndices)) {
                    LOG(ERROR) << "Failed to read chunks for repair";
                    return false;
                }
                break;
            default:
                LOG(ERROR) << "Failed to access chunks for unknown coding scheme" << (int) file.codingMeta.coding;
                return false;
        }

        // benchmark: set repair size of this stripe
        if (benchmark) 
            bmStripe->setRepairSize(numFailedChunks * file.chunks[0].size);

    } else { // repair at Agent
        // issue a repair reuqest to the agent
        events[0].id = _eventCount.fetch_add(1);
        events[0].opcode = Opcode::RPR_CHUNK_REQ;
        events[0].numChunks = numFailedChunks;
        try {
            events[0].chunks = new Chunk[numFailedChunks];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for chunks in repair request";
            return false;
        }
        // repair target
        for (int i = 0; i < numFailedNodes; i++) {
            for (int j = 0; j < numChunksPerNode; j++) {
                events[0].chunks[i * numChunksPerNode + j].setId(file.namespaceId, file.uuid, file.chunks[0].getChunkId() + failedNodes[i] * numChunksPerNode + j);
                events[0].chunks[i * numChunksPerNode + j].size = 0;
                events[0].chunks[i * numChunksPerNode + j].data = 0;
                events[0].chunks[i * numChunksPerNode + j].fileVersion = file.version;
            }
        }
        events[0].containerIds = spareContainers;
        // way to repair
        events[0].codingMeta.coding = file.codingMeta.coding;
        events[0].codingMeta.codingStateSize = submatrix.size();
        events[0].codingMeta.codingState = (unsigned char *) submatrix.data();
        events[0].numChunkGroups = numSubChunkGroups;
        events[0].numInputChunks = numInputChunks;
        events[0].chunkGroupMap = subChunkGroups;
        events[0].containerGroupMap = subContainerGroups;
        events[0].repairUsingCAR = isRepairUsingCAR;
        // the request
        ProxyIO::RequestMeta meta;
        meta.containerId = spareContainers[0];
        meta.io = _io;
        meta.request = &events[0];
        meta.reply = &events[1];

        pthread_t rt;
        void *ptr;
        // use a thread to send the request, and check if the request succeeded
        pthread_create(&rt, NULL, ProxyIO::sendChunkRequestToAgent, &meta);
        pthread_join(rt, &ptr);

        //if (ProxyIO::sendChunkRequestToAgent(&meta) != NULL || meta.reply->opcode != RPR_CHUNK_REP_SUCCESS) {
        if (ptr != NULL || meta.reply->opcode != RPR_CHUNK_REP_SUCCESS) {
            LOG(ERROR) << "Failed to send repair chunk request to agent";
            events[0].containerIds = 0;
            events[0].codingMeta.codingState = 0;
            events[0].chunkGroupMap = 0;
            events[0].containerGroupMap = 0;
            return false;
        }

        // benchmark: set repair size of this stripe
        if (benchmark) 
            bmStripe->setRepairSize(numFailedChunks * events[1].chunks[0].size);

        // update file metadata, and return
        for (int i = 0; i < numFailedChunks; i++) {
            int nid = i / numChunksPerNode;
            int cid = failedNodes[nid] + i % numChunksPerNode;
            LOG(INFO) << "Container for chunk " << failedNodes[nid] << " from " << file.containerIds[cid] << " to " << meta.reply->containerIds[nid];
            file.containerIds[cid] = meta.reply->containerIds[nid];
        }

        // reset the chunk corruption indicators
        memset(file.chunksCorrupted, 0, file.numChunks * sizeof(bool));

        events[0].containerIds = 0;
        events[0].codingMeta.codingState = 0;
        events[0].chunkGroupMap = 0;
        events[0].containerGroupMap = 0;
        return true;
    }

    int chunkSize = events[numInputChunks].chunks[0].size;

    std::vector<Chunk> inputChunks;
    inputChunks.resize(numInputChunks);
    for (int i = 0; i < numInputChunks; i++) {
        inputChunks.at(i).move(events[numInputChunks + i].chunks[0]);
        inputChunks.at(i).setChunkId(inputChunks.at(i).getChunkId() % coding->getNumChunks());
    }
    unsigned long int dataSize = chunkSize * numFailedNodes * numChunksPerNode;
    length_t decodedSize = 0;
    unsigned char *repairedData = (unsigned char *) malloc (dataSize);

    // assemble the input chunks for repair
    int numRepairedChunks = failedChunkIds.size();
    if (coding->decode(inputChunks, &repairedData, decodedSize, plan, file.codingMeta.codingState, /* is repair */ true, failedChunkIds) == false) {
        LOG(ERROR) << "Failed to repair lost chunk";
        free(repairedData);
        return false;
    }

    pthread_t wt[numRepairedChunks];
    ProxyIO::RequestMeta meta[numRepairedChunks];
    //for (int i = 0; i < file.numChunks; i++) DLOG(INFO) << "Chunk " << i << " size = " << file.chunks[i].size;
    // redistribute the repaired chunks
    for (int i = 0; i < numRepairedChunks / numChunksPerNode; i++) {
        events[i].id = _eventCount.fetch_add(1);
        events[i].opcode = Opcode::PUT_CHUNK_REQ;
        events[i].numChunks = numChunksPerNode;
        delete [] events[i].chunks;
        try {
            events[i].chunks = new Chunk[numChunksPerNode];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event chunks";
            return false;
        }
        delete [] events[i].containerIds;
        try {
            events[i].containerIds = new int[numChunksPerNode];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event container ids";
            return false;
        }
        for (int j = 0; j < numChunksPerNode; j++) {
            events[i].chunks[j].copyMeta(file.chunks[failedNodes[i] * numChunksPerNode + j]);
            events[i].chunks[j].size = chunkSize;
            events[i].chunks[j].data = repairedData + (i * numChunksPerNode + j) * chunkSize;
            events[i].chunks[j].computeMD5();
            events[i].chunks[j].freeData = false;
            events[i].containerIds[j] = spareContainers[i];

            // journal the upcoming write change
            //if (events[i].containerIds[j] != INVALID_CONTAINER_ID && _metastore && !_metastore->addChunkToJournal(file, events[i].chunks[j], events[i].containerIds[j], /* isWrite */ true)) {
            //    LOG(ERROR) << "Failed to journal the chunk change of file " << file.name << " chunk " << events[i].chunks[j].getChunkId() << " in container " << events[i].containerIds[j];
            //    return false;
            //}
        }
        meta[i].containerId = spareContainers[i];
        LOG(INFO) << "Store repaired chunk " << file.chunks[failedNodes[i] * numChunksPerNode].getChunkId() << " to container " << spareContainers[i];
        meta[i].io = _io;
        meta[i].request = &events[i];
        meta[i].reply = &events[i + numInputChunks * 2];
        // send the requests via threads
        pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);
    }

    // benchmark: set repair size of this stripe
    if (benchmark) 
        bmStripe->setRepairSize(numFailedChunks * events[1].chunks->size);

    // check reply and gather the updated container id to file
    // TODO handle partial success, e.g., remove chunk already set?
    bool allsuccess = true;
    for (int i = 0; i < numRepairedChunks / numChunksPerNode; i++) {
        void *ptr;
        pthread_join(wt[i], &ptr);
        // journal the replied change
        //for (int j = 0; j < numChunksPerNode; j++) {
        //    int containerId = meta[i].containerId;
        //    if (_metastore && !_metastore->updateChunkInJournal(file, events[i].chunks[j], /* isWrite */ true, /* isDelete */ false, containerId)) {
        //        LOG(ERROR) << "Failed to journal the chunk change of file " << file.name << " chunk " << events[i].chunks[j].getChunkId() << " in container " << containerId;
        //    }
        //}
        if (ptr != 0) {
            LOG(ERROR) << "Failed to store chunk due to internal failure, container id = " << meta[i].containerId;
            allsuccess = false;
            continue;
        }
        bool success = meta[i].reply->opcode == PUT_CHUNK_REP_SUCCESS;
        if (meta[i].reply->opcode != PUT_CHUNK_REP_SUCCESS) {
            allsuccess = false;
            LOG(ERROR) << "Failed to store chunks (" << i * numChunksPerNode << "," << (i + 1) * numChunksPerNode << ")";
            continue;
        }
        for (int j = 0; j < numChunksPerNode; j++) {
            int cidx = failedNodes[i] * numChunksPerNode + j;
            if (success) {
                file.chunks[cidx].copyMeta(events[i].chunks[j]);
            }
            LOG(INFO) << "Container for chunk " << file.chunks[cidx].getChunkId() << " from " << file.containerIds[cidx] << " to " << (success? events[i + numInputChunks * 2].containerIds[j] : -1);
            file.containerIds[cidx] = success? events[i + numInputChunks * 2].containerIds[j] : -1;
        }
    }

    // reset the chunk corruption indicators
    if (allsuccess) {
        memset(file.chunksCorrupted, 0, file.numChunks * sizeof(bool));
    }

    free(repairedData);

    return allsuccess;
}

int ChunkManager::checkFile(File &file, bool chunkIndicator[]) {
    Coding *coding = getCodingInstance(file.codingMeta.coding, file.codingMeta.n, file.codingMeta.k);
    if (coding == NULL) {
        return false;
    }
    int numChunksPerNode = coding->getNumChunksPerNode();

    int numFailedChunks = 0;
    ChunkEvent events[file.numChunks * 2];
    if (!accessChunks(events, file, file.numChunks, CHK_CHUNK_REQ, CHK_CHUNK_REP_SUCCESS, numChunksPerNode, 0, -1, chunkIndicator)) {
        for (int i = 0; i < file.numChunks; i++)
            numFailedChunks += chunkIndicator[i] == true;
    }
    return numFailedChunks;
}

int ChunkManager::verifyFileChecksums(File &file, bool chunkIndicator[]) {
    ChunkEvent events[2];

    pthread_t ct;
    ProxyIO::RequestMeta meta;

    // construct the request event
    events[0].id = _eventCount.fetch_add(1);
    events[0].opcode = Opcode::VRF_CHUNK_REQ;
    // chunk information
    events[0].numChunks = file.numChunks;
    try {
        events[0].chunks = new Chunk[file.numChunks];
        events[0].containerIds = new int[file.numChunks];
    } catch (std::bad_alloc &e) {
        LOG(ERROR) << "Failed to allocate memory for event metadata";
        return -1;
    }
    for (int i = 0; i < file.numChunks; i++) {
        events[0].chunks[i] = file.chunks[i];
        events[0].chunks[i].freeData = false;
        events[0].containerIds[i] = file.containerIds[0];
    }
    // request metadata
    meta.containerId = file.containerIds[0];
    meta.io = _io;
    meta.request = &events[0];
    meta.reply = &events[1];

    void *ptr = 0;

    // send the request
    pthread_create(&ct, NULL, ProxyIO::sendChunkRequestToAgent, &meta);
    pthread_join(ct, &ptr);

    // check if verification request fails over the network / at agent
    if (ptr != 0 || events[1].opcode != Opcode::VRF_CHUNK_REP_SUCCESS) {
        LOG(ERROR) << "Failed to verify " << file.numChunks << " checksums for container " << file.containerIds[0] << ", " << (ptr == 0? "failed at Agent" : "network error");
        // assume all chunks are not corrupted (although they might be missing if the container is unavailable)
        for (int cidx = 0; cidx < file.numChunks; cidx++)
            chunkIndicator[cidx] = false;
        return -1;
    }
    
    // process the response event
    // we assume the ordering of chunks in both response and request are the same
    for (int cidx = 0, replyChunkIdx = 0; cidx < file.numChunks; cidx++) {
        if (
                events[1].numChunks == 0 || // no failed chunks
                replyChunkIdx >= events[1].numChunks || // all failed chunks have been checked
                file.chunks[cidx].getChunkName() != events[1].chunks[replyChunkIdx].getChunkName() // current chunk is not the next failed chunk to process
        ) {
            chunkIndicator[cidx] = false;
        } else { // mark failed chunk
            chunkIndicator[cidx] = true;
            replyChunkIdx++;
        }
    }

    return events[1].numChunks;
}

int ChunkManager::getNumRequiredContainers(std::string storageClass) {
    Coding *coding = getCodingInstance(storageClass);
    if (coding == NULL) {
        return -1;
    }
    return getNumRequiredContainers(coding);
}

int ChunkManager::getNumRequiredContainers(int codingScheme, int n, int k) {
    Coding *coding = getCodingInstance(codingScheme, n, k);
    if (coding == NULL) {
        return -1;
    }
    return getNumRequiredContainers(coding);
}

int ChunkManager::getNumRequiredContainers(Coding *coding) {
    if (coding == NULL) {
        return -1;
    }
    return coding->storeCodeChunksOnly()? 
            coding->getNumCodeChunks() / coding->getNumChunksPerNode() :
            ( coding->getNumDataChunks() + coding->getNumCodeChunks() ) / coding->getNumChunksPerNode();
}

int ChunkManager::getMinNumRequiredContainers(std::string storageClass) {
    Coding *coding = getCodingInstance(storageClass);
    if (coding == NULL) {
        return -1;
    }
    
    // TODO this only considers codes that are MDS in which parity and data have the same repair capability
    return coding->getNumDataChunks() / coding->getNumChunksPerNode();
}

int ChunkManager::getNumChunksPerContainer(std::string storageClass) {
    Coding *coding = getCodingInstance(storageClass);
    if (coding == NULL) {
        return -1;
    }
    return getNumChunksPerContainer(coding);
}

int ChunkManager::getNumChunksPerContainer(int codingScheme, int n, int k) {
    Coding *coding = getCodingInstance(codingScheme, n, k);
    if (coding == NULL) {
        return -1;
    }
    return getNumChunksPerContainer(coding);
}

int ChunkManager::getNumChunksPerContainer(Coding *coding) {
    if (coding == 0) {
        return -1;
    }
    return coding->getNumChunksPerNode();
}

unsigned long int ChunkManager::getMaxDataSizePerStripe(std::string storageClass) {
    CodingMeta codingMeta;
    Coding *coding = getCodingInstance(storageClass);
    setCodingMeta(storageClass, codingMeta);
    if (coding == NULL) {
        return INVALID_FILE_OFFSET;
    }
    return getMaxDataSizePerStripe(coding, codingMeta.maxChunkSize);
}

unsigned long int ChunkManager::getMaxDataSizePerStripe(int codingScheme, int n, int k, int chunkSize, bool isFullChunkSize) {
    Coding *coding = getCodingInstance(codingScheme, n, k);
    if (coding == NULL) {
        return INVALID_FILE_OFFSET;
    }
    // for read, the input chunk size is that of a sub-chunk instead of a full chunk
    return getMaxDataSizePerStripe(coding, chunkSize * (isFullChunkSize? 1 : coding->getNumChunksPerNode()));
}

unsigned long int ChunkManager::getMaxDataSizePerStripe(Coding *coding, int chunkSize) {
    int numChunksPerNode = coding->getNumChunksPerNode();
    unsigned long int size = chunkSize / numChunksPerNode * coding->getNumDataChunks() - coding->getExtraDataSize();
    while (coding->getChunkSize(size) * numChunksPerNode > (length_t) chunkSize) size--;
    return size;
}

unsigned long int ChunkManager::getDataStripeSize(int codingScheme, int n, int k, unsigned long int size) {
    Coding *coding = getCodingInstance(codingScheme, n, k);
    if (coding == NULL) {
        return INVALID_FILE_OFFSET;
    }

    int chunkSize = coding->getChunkSize(size);
    int numDataChunks = coding->getNumDataChunks();

    return ((unsigned long int) chunkSize) * numDataChunks;
}

bool ChunkManager::setCodingMeta(std::string className, CodingMeta &codingMeta) {
    try {
        StorageClass* sc = _storageClasses.at(className);
        codingMeta.copyMeta(sc->getCodingMeta(), /* parameters only */ true);
        return true;
    } catch (std::out_of_range &e) {
        DLOG(INFO) << "Storage class [" << className << "] not found";
    }
    return false;
}

bool ChunkManager::willModifyDataBuffer(std::string storageClass) {
    Coding *coding = getCodingInstance(storageClass);
    if (coding != NULL) {
        return coding->modifyDataBuffer();
    }
    return false;
}

unsigned long int ChunkManager::getPerStripeExtraDataSize(std::string storageClass) {
    Coding *coding = getCodingInstance(storageClass);
    if (coding != NULL) {
        return coding->getExtraDataSize();
    }
    return 0;
}

bool ChunkManager::accessChunks(ChunkEvent events[], const File &f, int numChunks, Opcode reqOp, Opcode expectedOpRep, int numChunksPerNode, int *chunkIndices, int chunkIndicesSize, bool *chunkIndicator) {
    Benchmark &bm = Benchmark::getInstance();
    BMStripe *bmStripe = NULL;
    bool benchmark = false;
    
    // only do benchmark for read operation
    if (reqOp == Opcode::GET_CHUNK_REQ) {
        BMStripeFunc *func = 0;
        try {
            func = dynamic_cast<BMStripeFunc *>(bm.at(f.reqId));
        } catch (std::out_of_range &e) {
        }
        if (func && func->isStripeOn()) {
            try {
                bmStripe = &(func)->at(f.stripeId);
                benchmark = true;
            } catch (std::out_of_range &e) {
            }
        } 
    }

    bool useIdx = (chunkIndices != 0);

    // get container ids and chunk list
    int *containerIds = f.containerIds;
    Chunk *chunkList = f.chunks;

    // for checking if the event chunks are init for the first time
    bool init = false;

    // assume only the number of chunks to access is being indexed 
    if (chunkIndicesSize == -1)
        chunkIndicesSize = numChunks;

    int numSuccess = 0;
    bool allsuccess = false;

    pthread_t wt[numChunks];
    ProxyIO::RequestMeta meta[numChunks];

    // retry others if number of chunks get in last iteration is less than required, and there is more chunks to try
    while (numSuccess < numChunks && chunkIndicesSize >= numChunks) {
        allsuccess = true;

        // generate a read chunk event for each chunk
        for (int i = numSuccess; i < numChunks; i++) {
            events[i].id = _eventCount.fetch_add(1);
            events[i].opcode = reqOp;
            events[i].numChunks = 1;
            try {
                if (!init)
                    events[i].chunks = new Chunk[1];
            } catch (std::bad_alloc &e) {
                LOG(ERROR) << "Failed to allocate memory for event chunks";
                allsuccess = false;
                break;
            }
            events[i].chunks[0] = chunkList[(useIdx? chunkIndices[i] : i)];
            events[i].chunks[0].freeData = false;
            try {
                if (!init)
                    events[i].containerIds = new int[1];
            } catch (std::bad_alloc &e) {
                LOG(ERROR) << "Failed to allocate memory for event container ids";
                allsuccess = false;
                break;
            }
            events[i].containerIds[0] = containerIds[(useIdx? chunkIndices[i] : i)];

            // request metadata, set the container id (map to agent), io module, request and reply events
            // each request goes to one agent only
            meta[i].containerId = containerIds[(useIdx? chunkIndices[i] : i)];
            meta[i].io = _io;
            meta[i].request = &events[i];
            meta[i].reply = &events[i + numChunks];

            if (benchmark && bmStripe->network && bmStripe->network->size() > (size_t) i) {
                meta[i].network = &(bmStripe->network->at(i));
            }

            // send the requests via threads
            pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);
        }

        // the event chunks are init (no need to init upon retry)
        init = true;

        // wait for all replies
        // TODO check reply while waiting for others
        bool sentNoError[numChunks];
        for (int i = numSuccess; i < numChunks; i++) {
            void *ptr;
            pthread_join(wt[i], &ptr);
            sentNoError[i] = ptr == 0;
            
            if (benchmark && bmStripe->agentProcess && bmStripe->agentProcess->size() > (size_t) i) {
                bmStripe->agentProcess->at(i) = meta[i].reply->agentProcess;
            }
        }

        // TAGPT (start): checkReply
        if (benchmark) bmStripe->replyCheck.markStart();

        // check replies
        int chunkIndicatorIdx = 0;
        for (int i = numSuccess, numToCheck = numChunks; i < numToCheck; i++) {
            // verify checksum for chunk get, copy and move
            bool checksumPassed = !Config::getInstance().verifyChunkChecksum();
            bool chunkSizeMatches = false;
            if (sentNoError[i] && meta[i].reply->opcode == expectedOpRep) {
                switch (meta[i].request->opcode) {
                case Opcode::GET_CHUNK_REQ:
                    if (Config::getInstance().verifyChunkChecksum()) {
                        meta[i].reply->chunks[0].copyMD5(chunkList[(useIdx? chunkIndices[i] : i)]);
                        checksumPassed = meta[i].reply->chunks[0].verifyMD5();
                    }
                    chunkSizeMatches = chunkList[useIdx? chunkIndices[i] : i].size ==  meta[i].reply->chunks[0].size;
                    break;
                case Opcode::DEL_CHUNK_REQ:
                    // remove the deleted chunk from the file journal
                    //if (_metastore && !_metastore->updateChunkInJournal(f, chunkList[(useIdx? chunkIndices[i] : i)], /* isWrite */ false, /* isDelete */ true, meta[i].containerId)) {
                    //    LOG(ERROR) << "Failed to remove file " << f.name << " deletion journal record of chunk " << (useIdx? chunkIndices[i] : i) << ".";
                    //}
                    checksumPassed = true;
                    chunkSizeMatches = true;
                    break;
                default:
                    checksumPassed = true;
                    chunkSizeMatches = true;
                    break;
                }
            }
            if (!sentNoError[i] || meta[i].reply->opcode != expectedOpRep || !checksumPassed || !chunkSizeMatches) {
                LOG(ERROR) << "Failed to operate on chunk " << i << " (opcode  = " << reqOp << ") due to internal failure, container id = " << meta[i].containerId << ", return opcode =" << meta[i].reply->opcode << " instead of " << expectedOpRep << ",  send error = " << !sentNoError[i] << ", checksum okay = " << checksumPassed << ", chunk size okay = " << chunkSizeMatches; // << "(expected " << chunkList[useIdx? chunkIndices[i] : i].size << " but got " << meta[i].reply->chunks[0].size << ")";
                int start = i / numChunksPerNode * numChunksPerNode;
                // handle sucessfully obtained sub-chunks from the same node
                for (int j = start; j < start + numChunksPerNode; j++) {
                    // free and reset any successful reply
                    meta[j].reply->release();
                    meta[j].reply->reset();
                    // mark failed chunks
                    if (chunkIndicator && numChunks >= chunkIndicesSize && j >= i)
                        chunkIndicator[chunkIndicatorIdx++] = false; 
                    // update the chunk in the file journal
                    //if (
                    //    (meta[i].request->opcode == Opcode::DEL_CHUNK_REQ)
                    //    && (
                    //        _metastore
                    //        && !_metastore->updateChunkInJournal(f, chunkList[(useIdx? chunkIndices[j] : j)], /* isWrite */ false, /* isDelete */ false, meta[i].containerId)
                    //    )
                    //) {
                    //    LOG(ERROR) << "Failed to update file " << f.name << " deletion journal record of chunk " << (useIdx? chunkIndices[j] : j) << ".";
                    //}
                }
                if (start + numChunksPerNode < chunkIndicesSize && useIdx) {
                    // throw away the (ids of) chunks that fail
                    memmove(
                            &chunkIndices[start],
                            &chunkIndices[start + numChunksPerNode],
                            (chunkIndicesSize - (start + numChunksPerNode)) * sizeof(int)
                    );
                    for (int ci = 0; ci < chunkIndicesSize; ci++)
                        DLOG(INFO) << "CI [" << ci << "] = " << chunkIndices[ci];
                }
                if (start + numChunksPerNode < numChunks) {
                    // shift thread info
                    memmove(
                            &sentNoError[start],
                            &sentNoError[start + numChunksPerNode],
                            (numChunks - (start + numChunksPerNode)) * sizeof(bool)
                    );
                    // shift the chunk request event information without reallocating resource
                    ChunkEvent tmpEvent;
                    for (int idx = start; idx < numToCheck - numChunksPerNode; idx++) {
                        tmpEvent = events[idx];
                        events[idx] = events[idx + numChunksPerNode];
                        events[idx + numChunksPerNode] = tmpEvent;
                        // move container id (for message printing)
                        meta[idx].containerId = meta[idx + numChunksPerNode].containerId;
                    }
                    DLOG(INFO) << "Move events from " << start + numChunksPerNode << " to " << start;
                    // shift the chunk reply event information without reallocating resource
                    for (int idx = numChunks + start; idx < numChunks + numToCheck - numChunksPerNode; idx++) {
                        tmpEvent = events[idx];
                        events[idx] = events[idx + numChunksPerNode];
                        events[idx + numChunksPerNode] = tmpEvent;
                    }
                    DLOG(INFO) << "Move events from " << numChunks + start + numChunksPerNode << " to " << numChunks + start;
                    tmpEvent.reset();
                }
                // the list is cut by chunks on one node
                chunkIndicesSize -= numChunksPerNode;
                // decrement the number of successful obtained (and scaned) chunks so far
                numSuccess -= i - start;
                // skip all chunk requests to this node
                numToCheck -= numChunksPerNode;
                // restart request checking from the shifted position
                i = start - 1;
                DLOG(INFO) << "chunk indices num = " << chunkIndicesSize << " i = " << i << " numSuccess = " << numSuccess;
                // indicate retry is required (for read)
                allsuccess = false;
                continue;
            } else if (chunkIndicator && numChunks >= chunkIndicesSize) {
                // mark chunk as alive
                chunkIndicator[chunkIndicatorIdx++] = true;
            }
            if (meta[i].reply->numChunks > 0) {
                LOG(INFO)   << "Get reply for chunk "
                            << "("
                            << (unsigned int) meta[i].reply->chunks[0].getNamespaceId()
                            << ", "
                            << meta[i].reply->chunks[0].getFileUUID()
                            << ", "
                            << (int) meta[i].reply->chunks[0].getChunkId()
                            << ") of size "
                            << meta[i].reply->chunks[0].size
                ;
            }
            numSuccess++;
        }

        // TAGPT (end): checkReply
        if (benchmark) bmStripe->replyCheck.markEnd();

        // only retry for read
        if (reqOp != Opcode::GET_CHUNK_REQ)
            break;
    }

    if (!allsuccess) {
        LOG(ERROR) << "Failed to get some of the required chunks, need to handle degraded read or repair first";
    }

    return allsuccess;
}

bool ChunkManager::accessGroupedChunks(ChunkEvent events[], int containerIds[], int numChunks, int chunkGroups[], int numChunkGroups, unsigned char  namespaceId, boost::uuids::uuid fuuid, std::string matrix, int chunkIdOffset) {
    pthread_t wt[numChunkGroups];
    ProxyIO::RequestMeta meta[numChunkGroups];
    DLOG(INFO) << "Get grouped chunks from " << numChunkGroups << " groups of " << numChunks << " chunks";

    // generate a read chunk event for each chunk group
    for (int i = 0, midx = 0; i < numChunkGroups; i++) {
        events[i].id = _eventCount.fetch_add(1);
        events[i].opcode = Opcode::ENC_CHUNK_REQ;
        // chunks and containers
        events[i].numChunks = chunkGroups[i * (numChunks + 1)];
        try {
            events[i].chunks = new Chunk[events[i].numChunks];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event chunks";
            return false;
        }
        try {
            events[i].containerIds = new int[events[i].numChunks];
        } catch (std::bad_alloc &e) {
            LOG(ERROR) << "Failed to allocate memory for event container ids";
            return false;
        }
        // coding matrix
        events[i].codingMeta.codingStateSize = events[i].numChunks;
        events[i].codingMeta.codingState = new unsigned char [events[i].numChunks];
        if (events[i].codingMeta.codingState == NULL) {
            LOG(ERROR) << "Failed to allocate memory for event coding matrix";
            return false;
        }
        for (int j = 0; j < events[i].numChunks; j++) {
            events[i].chunks[j].setId(namespaceId, fuuid, chunkGroups[i * (numChunks + 1) + j + 1] + chunkIdOffset);
            events[i].chunks[j].freeData = false;
            events[i].containerIds[j] = containerIds[chunkGroups[i * (numChunks + 1) + j + 1]];
            events[i].codingMeta.codingState[j] = matrix[midx++];
            //DLOG(INFO) << "Chunk id = (" << events[i].chunks[j].getFileUUID() << "," << events[i].chunks[j].getChunkId() << ") in container " << containerIds[chunkGroups[i * (numChunks + 1) + j + 1]] << " and group " << i;
        }

        meta[i].containerId = containerIds[chunkGroups[i * (numChunks + 1) + 1]];
        meta[i].io = _io;
        meta[i].request = &events[i];
        meta[i].reply = &events[i + numChunkGroups];
        // send the requests via threads
        pthread_create(&wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &meta[i]);
    }

    // check the reply
    bool allsuccess = true;

    for (int i = 0; i < numChunkGroups; i++) {
        void *ptr;
        pthread_join(wt[i], &ptr);
        if (ptr != 0 || meta[i].reply->opcode != ENC_CHUNK_REP_SUCCESS) {
            LOG(ERROR) << "Failed to operate on chunk (" << ENC_CHUNK_REQ << ") due to internal failure, container id = " << meta[i].containerId << ", return opcode =" << meta[i].reply->opcode;
            allsuccess = false;
            continue;
        }
        if (meta[i].reply->numChunks > 0) {
            for (int j = 0; j < meta[i].reply->numChunks; j++)
                LOG(INFO)   << "Get reply for chunk ("
                            << (unsigned int) meta[i].reply->chunks[j].getNamespaceId()
                            << ", "
                            << meta[i].reply->chunks[j].getFileUUID()
                            << ", "
                            << (int) meta[i].reply->chunks[j].getChunkId()
                            << ") of size "
                            << meta[i].reply->chunks[j].size
                ;
        }
    }

    return allsuccess;
}

bool ChunkManager::isValidCoding(int coding) {
    return coding >= 0 && coding < CodingScheme::UNKNOWN_CODE;
}

bool ChunkManager::revertFile(const File &file, bool chunkIndicator[]) {
    return operateOnAliveChunks(file, chunkIndicator, Opcode::RVT_CHUNK_REQ, Opcode::RVT_CHUNK_REP_SUCCESS);
}

Coding* ChunkManager::getCodingInstance(std::string className) {
    try {
        StorageClass* sc = _storageClasses.at(className);
        return sc->getCodingInstance();
    } catch (std::out_of_range &e) {
        DLOG(INFO) << "Storage class [" << className << "] not found";
    }
    return NULL;
}

int ChunkManager::getCodingScheme(std::string className) {
    try {
        StorageClass* sc = _storageClasses.at(className);
        return sc->getCodingScheme();
    } catch (std::out_of_range &e) {
        DLOG(INFO) << "Storage class [" << className << "] not found";
    }
    return CodingScheme::UNKNOWN_CODE;
}

Coding* ChunkManager::getCodingInstance(int codingScheme, int n, int k) {
    Coding *code = NULL;
    if (!isValidCoding(codingScheme)) {
        return code;
    }
    std::lock_guard<std::mutex> lkg (_codingsLock);
    std::string key = genCodingInstanceKey(codingScheme, n, k);
    try {
        code = _codings.at(key);
    } catch (std::logic_error &e) {
        CodingOptions options;
        options.setN(n);
        options.setK(k);
        try {
            code = CodingGenerator::genCoding(codingScheme, options);
            _codings.insert(std::make_pair(key, code));
            DLOG(INFO) << "Coding instance for scheme = " << codingScheme << " n = " << n << " and k = " << k << " not found, but generated";
        } catch (std::invalid_argument &e2) {
            DLOG(INFO) << "Coding instance for scheme = " << codingScheme << " n = " << n << " and k = " << k << " not found, and failed to generate one" << e2.what();
        }
    }
    return code;
}

std::string ChunkManager::genCodingInstanceKey(int codingScheme, int n, int k) {
    std::string key;
    if (isValidCoding(codingScheme)) {
        key.append(CodingSchemeName[codingScheme])
            .append("_")
            .append(std::to_string(n))
            .append("_")
            .append(std::to_string(k))
        ;
    } else {
        throw std::invalid_argument("Invalid coding scheme");
    }
    return key;
}
