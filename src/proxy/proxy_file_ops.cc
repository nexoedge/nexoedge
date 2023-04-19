// SPDX-License-Identifier: Apache-2.0

#include "proxy.hh"

#include "../common/config.hh"
#include "../common/define.hh"

bool Proxy::writeFile(File &f) {

    boost::timer::cpu_timer all, getMeta, writeData, computeChecksum, removeOldData, commitfp, putMeta;
    TagPt overallT;
    overallT.markStart();
    
    // write file, old file
    File wf, of;

    // benchmark: mark block id
    wf.copyOperationBenchmarkInfo(f);
    of.copyOperationBenchmarkInfo(f);

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;
    if (f.storageClass.empty())
        f.storageClass = Config::getInstance().getDefaultStorageClass();

    int *spareContainers = 0;
    int numSelected = 0; // no need to find selected containers
    if (prepareWrite(f, wf, spareContainers, numSelected, /* needsFindSpareContainers */ false) == false) {
        delete [] spareContainers;
        return false;
    }
        
    // make a shadow copy of data for file processing
    wf.data = f.data;

    getMeta.start();
    // lock file for write
    if (lockFile(wf) == false) {
        LOG(ERROR) << "Failed to lock file " << wf.name << " before write";
        wf.data = 0;
        delete [] spareContainers;
        return false;
    }

    // remove old version of the file
    bool deleteOldFile = false;
    of.copyName(f, /* shadow */ true);
    wf.version = 0;

    // increment version number, and update timestamps
    time_t now = time(NULL);
    if (_metastore->getMeta(of)) {
        wf.setTimeStamps(of.ctime, now, now);
        // delete old file chunks only when (i) system is configured to overwrite file data, and (ii) there is no chunk reference, i.e., either deduplication is disabled or the old file has no unique chunk
        deleteOldFile = Config::getInstance().overwriteFiles();
        DLOG(INFO) << "Increment version of file " << f.name << " from " << of.version << " to " << wf.version;
    } else if (f.ctime == 0) {
        wf.setTimeStamps(now, now, now);
    }
    getMeta.stop();

    writeData.start();
    // write data
    bool writtenToBackend = false, writtenToStaging = false;
    if (wf.size != wf.length || wf.offset != 0) {
        LOG(ERROR) << "Partial file write (" << f.name << ") is not supported";
        unlockFile(wf);
        of.name = 0;
        wf.data = 0;
        delete [] spareContainers;
        return false;
    } else if (wf.size == 0) { // empty file
        wf.numStripes = 0;
        writtenToBackend = true;
        wf.version = of.version == -1? 0 : of.version + 1;
    } else {
        // try writing to staging first
        if (_stagingEnabled) {
            // open, write, close
            pinStagedFile(wf);
            _staging->openFileForWrite(wf);
            writtenToStaging = _staging->writeFile(wf);
            _staging->closeFileForWrite(wf);
            // if write to staging succeeded, mark as pending background write
            if (writtenToStaging) {
                of.setStagedInfo(wf.size, wf.codingMeta, wf.storageClass, wf.mtime);
                if (of.version == -1) {
                    of.version = 0;
                }
                _metastore->markFileAsPendingWriteToCloud(of);
            } else {
                unpinStagedFile(wf);
            }
        }
        // fall back if needed
        if (!writtenToStaging) {
            wf.version = of.version + 1;
            wf.storageClass = f.storageClass.empty()? Config::getInstance().getDefaultStorageClass() : f.storageClass;
            writtenToBackend = writeFileStripes(f, wf, spareContainers, numSelected);
        }
    }
    // report error if data is not written to both staging and backend
    if (!writtenToBackend && !writtenToStaging) {
        unlockFile(wf);
        wf.data = 0;
        of.name = 0;
        delete [] spareContainers;
        // abort all fingerprints
        size_t numCommits = wf.commitIds.size();
        for (size_t i = 0; i < numCommits; i++) {
            _dedup->abort(wf.commitIds.at(i));
        }
        return false;
    }
    writeData.stop();

    computeChecksum.start();
    // md5 checksum
    MD5Calculator md5;
    md5.appendData(f.data, f.length);
    unsigned int md5len = MD5_DIGEST_LENGTH;
    md5.finalize(wf.md5, md5len);
    memcpy(f.md5, wf.md5, MD5_DIGEST_LENGTH);
    computeChecksum.stop();

    putMeta.start();
    // unset data after encoding
    wf.data = 0;
    // update id and uuid
    f.uuid = wf.uuid;
    // update metadata
    if (_metastore->putMeta(writtenToStaging? of : wf) == false) {
        LOG(ERROR) << "Failed to update file metadata of file " << f.name;
        unlockFile(wf);
        of.name = 0;
        delete [] spareContainers;
        // abort all fingerprints
        size_t numCommits = wf.commitIds.size();
        for (size_t i = 0; i < numCommits; i++) {
            _dedup->abort(wf.commitIds.at(i));
        }
        return false;
    }
    putMeta.stop();

    commitfp.start();
    // commit all fingerprints
    size_t numCommits = wf.commitIds.size();
    for (size_t i = 0; i < numCommits; i++) {
        _dedup->commit(wf.commitIds.at(i));
    }
    commitfp.stop();

    boost::timer::cpu_times duration = writeData.elapsed();

    // update journal on the write success
    //for (int i = 0; i < wf.numChunks; i++) {
    //    int containerId = wf.containerIds[i];
    //    const Chunk &chunk = wf.chunks[i];
    //    if (containerId == INVALID_CONTAINER_ID) { continue; }
    //    if (!_metastore->updateChunkInJournal(wf, chunk, /* isWrite */ true, /* deleteRecord */ true, containerId)) {
    //        LOG(ERROR) << "Failed to journal the chunk change of file " << wf.name << " chunk " << chunk.getChunkId() << ".";
    //    }
    //}
    
    removeOldData.start();
    // if the new data is written to backend (not staging), one can safely remove the old data from backend
    if (deleteOldFile && !writtenToStaging) {
        bool chunkIndices[of.numChunks];
        _coordinator->checkContainerLiveness(of.containerIds, of.numChunks, chunkIndices);
        if (_chunkManager->deleteFile(of, chunkIndices) == false) {
            LOG(WARNING) << "Failed to delete file " << f.name << " from backend";
        }
    }
    removeOldData.stop();

    of.name = 0;
    delete [] spareContainers;

    overallT.markEnd();

    // record the operation
    const std::map<std::string, double> stats = genStatsMap(duration, putMeta.elapsed(), f.size);
    _statsSaver.saveStatsRecord(stats, writtenToStaging? "write (staging)" : "write (cloud)", std::string(wf.name, wf.nameLength), overallT.getStart().sec(), overallT.getEnd().sec());

    unlockFile(wf);

    LOG_IF(INFO, duration.wall > 0) << "Write file " << f.name << ", (data) speed = " << (f.size * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << f.size * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
    LOG(INFO) << "Write file " << f.name 
            << ", (get-meta) = " << getMeta.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (compute-checksum) = " << computeChecksum.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (put-meta) = " << putMeta.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (commit-fp) = " << commitfp.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (remove-old-chunks) = " << removeOldData.elapsed().wall * 1.0 / 1e6 << " ms";
    LOG(INFO) << "Write file " << f.name << ", completes in " << all.elapsed().wall * 1.0 / 1e9 << " s";

    return true;
}

bool Proxy::overwriteFile(File &f) {
    return modifyFile(f, /* isAppend */ false);
}

bool Proxy::appendFile(File &f) {
    return modifyFile(f, /* isAppend */ true);
}

bool Proxy::modifyFile(File &f, bool isAppend) {
    File wf, of, rf;

    boost::timer::cpu_timer all, getMeta, readOldData, writeData, processMeta, putMeta, commitfp;
    TagPt overallT;
    overallT.markStart();

    // benchmark: mark block id for wf and of
    wf.copyOperationBenchmarkInfo(f);
    of.copyOperationBenchmarkInfo(f);

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;
    of.copyName(f, /* shadow */ true);
    boost::uuids::uuid expectedUUID = of.uuid;

    unsigned long int ooffset = f.offset, olength = f.length;

    getMeta.start();
    // check if the file already exists
    if (lockFileAndGetMeta(of, isAppend? "append" : "overwrite") == false) {
        of.name = 0;
        return false;
    }
    getMeta.stop();

    // reuse previous storage class setting if not specified in the current request
    if (f.storageClass.empty())
        f.storageClass = _stagingEnabled? of.staged.storageClass : of.storageClass;

    // do not support change in storage class for all appends and overwrite when versioning is enabled
    bool isVersioned = !Config::getInstance().overwriteFiles();
    if (
        ((_stagingEnabled && of.staged.storageClass != f.storageClass)
        || (!_stagingEnabled && of.storageClass != f.storageClass))
        && (isAppend || (!isAppend && isVersioned))
    ) {
        LOG(ERROR) << "Do not support chnage in storage class";
        unlockFile(of);
        of.name = 0;
        return false;
    }

    writeData.start();
    // if the most recent data is in staging, write to staging instead
    if (_staging && of.staged.size > 0 && of.staged.mtime >= of.mtime) {
        wf.copyNameAndSize(f);
        wf.copyOperationDataRange(f);
        wf.size = isAppend? f.offset + f.length : (of.staged.mtime >= of.mtime? of.staged.size : of.size);
        wf.data = f.data;
        bool writtenToStaging = _staging->writeFile(wf, /* read from cloud */ false, /* truncate to zero */ false);
        // only report failure if staged file is more recent, but not visible to this proxy for modifications
        // if file on cloud is as recent as the staged one, allow modification to by-pass staging
        if (!writtenToStaging && of.staged.mtime > of.mtime) {
            unlockFile(of);
            LOG(ERROR) << "Failed to " << (isAppend? "append" : "overwrite") << " " << wf.name << " (" << wf.offset << "," << wf.length << ") in staging";
            wf.data = 0;
            of.name = 0;
            return false;
        }
        if (writtenToStaging) {
            of.staged.size = wf.size;
            if (!_metastore->putMeta(of)) {
                LOG(WARNING) << "Failed to update metadata of file for " << (isAppend? "append" : "overwrite") << " " << wf.name << " (" << wf.offset << "," << wf.length << ") in staging";
            }
            _metastore->markFileAsPendingWriteToCloud(of);
            unlockFile(of);

            f.size = f.offset + f.length;

            wf.data = 0;
            of.name = 0;

            return true;
        }
    }
    writeData.stop();

    readOldData.start();
    unsigned long int alignment = getExpectedAppendSize(of);
    // check against the restrictions
    bool okay = true;
    if (isAppend) {
        // only allow full stripe appends
        if (of.size % alignment != 0) {
            LOG(ERROR) << "Append to files of unaligned sizes (file size = " << of.size << " vs. append size = " <<  alignment << ") is not supported";
            isAppend = false;
        }
        // make sure append is at the end of file
        if (of.size != f.offset) {
            LOG(ERROR) << "Cannot append to file as the current file size = " << of.size << " but the append offset = " << f.offset;
        }
        if (of.size > f.offset) {
            isAppend = false;
        } else if (of.size < f.offset) {
            okay = false;
        }
    }
    if (!isAppend) {
        // overwrite should start from the beginning of a stripe in the file 
        if (of.size < f.offset) {
            // check if write is within the old file size
            LOG(ERROR) << "Invalid overwrite operation for file " << f.name << " (file size = " << of.size << " vs. overwrite position (" << f.offset << "," << f.length << ")";
            okay = false;
        } else if (of.size > 0 && (f.offset % alignment != 0 || f.length % alignment != 0)) {
            unsigned long int readAlignment = _chunkManager->getMaxDataSizePerStripe(of.codingMeta.coding, of.codingMeta.n, of.codingMeta.k, of.chunks[0].size, /* full chunk size */ false);
            rf.copyNameAndSize(of);
            rf.offset = f.offset / alignment * alignment;
            rf.length = (f.offset - rf.offset + f.length + readAlignment - 1) / readAlignment * readAlignment;
            rf.data = (unsigned char *) calloc (rf.length, 1);
            if (rf.data == 0) {
                LOG(ERROR) << "Invalid overwrite operation for file " << f.name << " (file size = " << of.size << ", offset = " << f.offset << ", length = " << f.length << ", read_buf size = " << rf.length << ")";
                rf.name = 0;
                okay = false;
            } else {
                if (!readFile(rf, /* is partial */ rf.offset != 0)) {
                    rf.name = 0;
                    okay = false;
                } else {
                    // overwrite the existing data
                    memcpy(rf.data + (f.offset - rf.offset), f.data, f.length);
                    // swap target file data buffer to the aligned and overwritten one 
                    std::swap(f.data, rf.data);
                    // adjust the offset and length to overwrite
                    f.offset = rf.offset;
                    f.length = ooffset + olength > rf.offset + rf.size? ooffset - f.offset + olength : rf.size;
                }
            }
        }
    }
    // TODO support append after rename (rename chunks during file rename)
    if (of.uuid != expectedUUID) {
        LOG(ERROR) << "Do not support append after rename\n";
        okay = false;
    }
    if (!okay) {
        unlockFile(of);
        of.name = 0;
        // swap the information back
        if (rf.data) {
            std::swap(f.data, rf.data);
            f.offset = ooffset;
            f.length = olength;
        }
        return false;
    }
    readOldData.stop();

    writeData.resume();
    // init for write
    int numContainers = _chunkManager->getNumRequiredContainers(of.codingMeta.coding, of.codingMeta.n, of.codingMeta.k);
    if (numContainers < 0) {
        unlockFile(of);
        of.name = 0;
        // swap the information back
        if (rf.data) {
            std::swap(f.data, rf.data);
            f.offset = ooffset;
            f.length = olength;
        }
        return false;
    }
    // update the new file size
    if (isAppend)
        of.size += f.length;
    else if (f.offset + f.length > of.size)
        of.size = f.offset + f.length;
    // update the ofset and length of data to write
    of.copyOperationDataRange(f);
    int *spareContainers = 0;
    int numSelected = 0;

    if (prepareWrite(of, wf, spareContainers, numSelected, /* needsFindSpareContainers */ false) == false) {
        unlockFile(of);
        of.name = 0;
        // swap the information back
        if (rf.data) {
            std::swap(f.data, rf.data);
            f.offset = ooffset;
            f.length = olength;
        }
        delete [] spareContainers;
        return false;
    }
    // offset the address calculation, since the data is assumed to be pointing at the start of file
    wf.data = f.data - f.offset;
    // use old version number
    wf.copyVersionControlInfo(of);

    // do append as if writing large files
    if (!writeFileStripes(of, wf, spareContainers, numSelected)) {
        unlockFile(of);
        of.name = 0;
        wf.data = 0;
        // swap the information back
        if (rf.data) {
            std::swap(f.data, rf.data);
            f.offset = ooffset;
            f.length = olength;
        }
        delete [] spareContainers;
        return false;
    }
    writeData.stop();

    // process metadata
    processMeta.start();
    CodingMeta &cmeta = wf.codingMeta;
    unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize);
    int startIdx = f.offset / maxDataStripeSize;
    int endIdx = (f.offset + f.length + maxDataStripeSize - 1) / maxDataStripeSize;
    // copy metadata of the previous / following stripes (container ids, chunk locations, coding metadata)
    int numChunksPerStripe = of.numStripes > 0? of.numChunks / of.numStripes : 0;
    int codingStateSize = of.numStripes > 0? of.codingMeta.codingStateSize / of.numStripes : 0;

    if (startIdx > 0) { // stripes before the first modified stripe
        memcpy(wf.containerIds, of.containerIds, sizeof(int) * numChunksPerStripe * startIdx);
        for (int i = 0; i < numChunksPerStripe * startIdx; i++) {
            wf.chunks[i].copyMeta(of.chunks[i]);
        }
        if (wf.codingMeta.codingStateSize > 0 && of.codingMeta.codingStateSize > 0)
            memcpy(wf.codingMeta.codingState, of.codingMeta.codingState, codingStateSize * startIdx);
    }
    if (endIdx < of.numStripes) { // (rear) stripes after the last modified stripe
        int numRearStripes = of.numStripes - endIdx;
        memcpy(
            wf.containerIds + numChunksPerStripe * endIdx
            , of.containerIds + numChunksPerStripe * endIdx 
            , sizeof(int) * numChunksPerStripe * numRearStripes
        );
        for (int i = numChunksPerStripe * endIdx; i < numChunksPerStripe * of.numStripes; i++) {
            wf.chunks[i].copyMeta(of.chunks[i]);
        }
        if (wf.codingMeta.codingStateSize > 0 && of.codingMeta.codingStateSize > 0)
            memcpy(wf.codingMeta.codingState + codingStateSize * endIdx, of.codingMeta.codingState + codingStateSize * endIdx, codingStateSize * numRearStripes);
    }
    // accumulated fingerprints
    if (wf.uniqueBlocks.size() < of.uniqueBlocks.size())
        std::swap(wf.uniqueBlocks, of.uniqueBlocks);
    if (wf.duplicateBlocks.size() < of.duplicateBlocks.size())
        std::swap(wf.duplicateBlocks, of.duplicateBlocks);
    wf.uniqueBlocks.insert(of.uniqueBlocks.begin(), of.uniqueBlocks.end());
    wf.duplicateBlocks.insert(of.duplicateBlocks.begin(), of.duplicateBlocks.end());
    // update last access time and last modified time
    time_t now = time(NULL);
    wf.setTimeStamps(wf.ctime, now, now);
    processMeta.stop();

    putMeta.start();
    // update metadata
    if (_metastore->putMeta(wf) == false) {
        LOG(ERROR) << "Failed to update file metadata of file " << f.name;
        unlockFile(of);
        of.name = 0;
        wf.data = 0;
        // swap the information back
        if (rf.data) {
            std::swap(f.data, rf.data);
            f.offset = ooffset;
            f.length = olength;
        }
        delete [] spareContainers;
        size_t numCommits = wf.commitIds.size();
        for (size_t i = 0; i < numCommits; i++) {
            _dedup->abort(wf.commitIds.at(i));
        }
        return false;
    }
    putMeta.stop();

    commitfp.start();
    size_t numCommits = wf.commitIds.size();
    for (size_t i = 0; i < numCommits; i++) {
        _dedup->commit(wf.commitIds.at(i));
    }
    commitfp.stop();
    
    unlockFile(of);

    // swap the information back
    if (rf.data) {
        std::swap(f.data, rf.data);
        f.offset = ooffset;
        f.length = olength;
    }

    f.size = isAppend? wf.size : f.offset + f.length;
    of.name = 0;
    wf.data = 0;

    overallT.markEnd();

    boost::timer::cpu_times duration = writeData.elapsed();
    const std::map<std::string, double> stats = genStatsMap(duration, putMeta.elapsed(), f.length);
    _statsSaver.saveStatsRecord(stats, isAppend? "append" : "overwrite", std::string(wf.name, wf.nameLength), overallT.getStart().sec(), overallT.getEnd().sec());

    delete [] spareContainers;

    const char *opType = isAppend? "Append" : "Overwrite";
    LOG_IF(INFO, duration.wall > 0) << opType << " file " << f.name << ", (data) speed = " << (f.length * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << f.size * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " s";
    LOG(INFO) << opType << " file " << f.name 
            << ", (get-meta) = " << (getMeta.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (read-old-data) = " << (readOldData.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (process-meta) = " << (processMeta.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (put-meta) = " << (putMeta.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (commit-fp) = " << (commitfp.elapsed().wall * 1.0 / 1e6) << " ms";
    LOG(INFO) << opType << " file " << f.name << ", completes in " << all.elapsed().wall * 1.0 / 1e9 << " s";
        
    return true;
}

bool Proxy::writeFileStripes(File &f, File &wf, int spareContainers[], int numSelected) {
    int numContainers = _chunkManager->getNumRequiredContainers(wf.codingMeta.coding, wf.codingMeta.n, wf.codingMeta.k);
    int numChunksPerContainer = _chunkManager->getNumChunksPerContainer(wf.codingMeta.coding, wf.codingMeta.n, wf.codingMeta.k);
    if (numContainers < 0 || numChunksPerContainer < 0) {
        return false;
    }

    unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(wf.codingMeta.coding, wf.codingMeta.n, wf.codingMeta.k, wf.codingMeta.maxChunkSize);
    if (maxDataStripeSize == INVALID_FILE_OFFSET) {
        return false;
    }

    unsigned char *stripebuf = 0;
    int numStripes = f.size / maxDataStripeSize;
    numStripes += (f.size % maxDataStripeSize == 0)? 0 : 1;
    int numChunksPerStripe = numContainers * numChunksPerContainer;
    wf.numChunks = numChunksPerStripe * numStripes;
    if (!wf.initChunksAndContainerIds()) {
        return false;   
    }

    boost::timer::cpu_timer dedupScanTime, dedupPostProcessTime, prepareWriteTime, dataWriteTime, postWriteProcessTime;
    dedupScanTime.stop();
    dedupPostProcessTime.stop();
    prepareWriteTime.stop();
    dataWriteTime.stop();
    postWriteProcessTime.stop();

    // write the data stripe-by-stripe
    int startIdx = f.offset / maxDataStripeSize;
    int endIdx = (f.offset + f.length + maxDataStripeSize - 1) / maxDataStripeSize;

    DLOG(INFO) << "Write stripe " << startIdx << " to " << endIdx << " of file " << wf.name;

    std::string filename = std::string(wf.name, wf.nameLength);

    for (int i = startIdx; i < endIdx; i++) {
        bool isAppend = i >= f.numStripes;

        prepareWriteTime.resume();

        File swf; // stripe to write
        swf.copyVersionControlInfo(wf);

        wf.offset = i * maxDataStripeSize;
        wf.length = (f.size - i * maxDataStripeSize > maxDataStripeSize)? maxDataStripeSize : f.size - i * maxDataStripeSize;

        // copy request information for benchmark
        swf.reqId = wf.reqId;
        swf.blockId = wf.blockId;
        swf.stripeId = i;
        
        // reuse container ids for overwrite
        if (!isAppend) {
            numSelected = numChunksPerStripe;
            for (int cidx = 0; cidx < numChunksPerStripe; cidx++) {
                spareContainers[cidx] = f.containerIds[i * numChunksPerStripe + cidx];
            }
        }

#define CLEAN_UP_PREVIOUS_STRIPES(__END_IDX__) do { \
    if (i > startIdx) { \
        memmove(wf.containerIds, wf.containerIds + startIdx * numChunksPerStripe, (__END_IDX__ - startIdx) * numChunksPerStripe); \
        for (int j = startIdx * numChunksPerStripe; j < __END_IDX__ * numChunksPerStripe; j++) { \
            wf.chunks[j - startIdx * numChunksPerStripe] = wf.chunks[j]; \
            wf.chunks[j].freeData = false; \
        } \
        wf.numChunks = numChunksPerStripe * (__END_IDX__ - startIdx); \
        bool chunkIndicator[wf.numChunks] = { true }; \
        if (isAppend) { \
            _chunkManager->deleteFile(wf, chunkIndicator); \
        } else { \
            _chunkManager->revertFile(wf, chunkIndicator); \
        } \
    } \
} while (0)

        if (prepareWrite(wf, swf, spareContainers, numSelected, isAppend) == false) {
            swf.data = 0;
            // clean up previous data
            if (i > startIdx) CLEAN_UP_PREVIOUS_STRIPES((i-1));
            return false;
        }

        // make a shadow copy of data for file processing
        swf.data = wf.data;

        // benchmark
        BMWrite *bmWrite = dynamic_cast<BMWrite *>(Benchmark::getInstance().at(swf.reqId));
        BMWriteStripe *bmStripe = NULL;
        bool benchmark = bmWrite && bmWrite->isStripeOn();
        if (benchmark) {
            bmStripe = &(bmWrite->at(swf.stripeId));
            bmStripe->setMeta(swf.stripeId, swf.length, bmWrite);
            // TAGPT (start): process stripe
            bmStripe->overallTime.markStart();
            bmStripe->preparation.markStart();
        }

        // use buffer if the data buffer will be modified (e.g., appending coding specific info), or the stripe needs padding
        bool useBuffer = _chunkManager->willModifyDataBuffer(f.storageClass) || swf.length != maxDataStripeSize;
        if (useBuffer) {
            // adjust the buffer size for last stripe with unaligned size
            if (stripebuf == 0)
                stripebuf = (unsigned char *) calloc (_chunkManager->getDataStripeSize(wf.codingMeta.coding, wf.codingMeta.n, wf.codingMeta.k, maxDataStripeSize), 1);
            // copy data to temp buffer
            memcpy(stripebuf, swf.data + swf.offset, swf.length);
            // point to the temp buffer instead of shadowing the original data buffer
            swf.data = stripebuf;
        } else {
            // directly advance to the start of the current data stripe
            swf.data += swf.offset;
        }

        prepareWriteTime.stop();

        dedupScanTime.resume();
        // scan for duplicate blocks
        std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> > stripeFps;
        std::string commitId;
        if (!dedupStripe(swf, wf.uniqueBlocks, wf.duplicateBlocks, commitId)) {
            return false;
        }
        dedupScanTime.stop();

        bool emptyStripe = swf.length == 0;

        dedupPostProcessTime.resume();
        // save the fingerprints to file

        // add commit id to file (do it here instead of after chunk write, so if returned on error, the current commit id can also be aborted)
        wf.commitIds.push_back(commitId);
        dedupPostProcessTime.stop();

        dataWriteTime.resume();
        // TODO journaling / copy-on-write for overwrite to avoid file corruption due to unexpected termination
        // write the stripe (as part of the file)
        if (!emptyStripe && _chunkManager->writeFileStripe(swf, spareContainers, numSelected, /* alignDataBuf */ false, /* isOverwrite */ !isAppend) == false) {
            LOG(ERROR) << "Failed to write file " << f.name << " to backend";
            swf.data = 0;
            // clean up previous data
            CLEAN_UP_PREVIOUS_STRIPES(i);
            free(stripebuf);
            return false;
        }
        dataWriteTime.stop();

        postWriteProcessTime.resume();
        // process metadata
        if (!emptyStripe && swf.numChunks != numChunksPerStripe) {
            LOG(WARNING) << "Expected num of chunks in stripe: " << numChunksPerStripe << ", but actually got " << swf.numChunks;
        }

        // copy container ids and chunk information from stripe (holder) to file
        if (!emptyStripe) {
            memcpy(wf.containerIds + i * numChunksPerStripe, swf.containerIds, numChunksPerStripe * sizeof(int));
        } else {
            for (int cidx = 0; cidx < numChunksPerStripe; cidx++) {
                wf.containerIds[i * numChunksPerStripe + cidx] = UNUSED_CONTAINER_ID;
            }
        }
        for (int nc = 0; nc < numChunksPerStripe; nc++) {
            if (!emptyStripe) {
                wf.chunks[i * numChunksPerStripe + nc].copyMeta(swf.chunks[nc]);
            } else {
                wf.chunks[i * numChunksPerStripe + nc].size = 0;
                wf.chunks[i * numChunksPerStripe + nc].resetMD5();
            }
            wf.chunks[i * numChunksPerStripe + nc].setChunkId(i * numChunksPerStripe + nc);
        }

        // copy coding meta from stripe (holder) to file
        if (i == startIdx) {
            wf.codingMeta.n = swf.codingMeta.n;
            wf.codingMeta.k = swf.codingMeta.k;
            wf.codingMeta.codingStateSize = swf.codingMeta.codingStateSize * numStripes;
            if (wf.codingMeta.codingStateSize > 0)
                wf.codingMeta.codingState = new unsigned char [wf.codingMeta.codingStateSize];
        }
        if (wf.codingMeta.codingStateSize > 0) {
            memcpy(wf.codingMeta.codingState + i * swf.codingMeta.codingStateSize, swf.codingMeta.codingState, swf.codingMeta.codingStateSize);
        }
        postWriteProcessTime.stop();

        // clean up
        swf.data = 0;

        // TAGPT (end): process stripe
        if (benchmark) {
            bmStripe->overallTime.markEnd();
        }
    }

    LOG(INFO) << " Write file " << f.name 
            << ", (dedup-scan) = " << (dedupScanTime.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (dedup-post-process) = " << (dedupPostProcessTime.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (prepare-write) = " << (prepareWriteTime.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (data-write) = " << (dataWriteTime.elapsed().wall * 1.0 / 1e6) << " ms"
            << ", (post-write-process) = " << (postWriteProcessTime.elapsed().wall * 1.0 / 1e6) << " ms"
    ;

    wf.numStripes = numStripes;

    free(stripebuf);

#undef CLEAN_UP_PREVIOUS_STRIPES
    return true;
}

bool Proxy::dedupStripe(File &swf, std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> > &uniqueFps, std::map<BlockLocation::InObjectLocation, Fingerprint> &duplicateFps, std::string &commitId) {
    boost::timer::cpu_timer copyTime, buildListTime, scanTime;
    copyTime.stop();
    buildListTime.stop();

    std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, bool> > logicalBlocks;
    BlockLocation location (swf.namespaceId, std::string(swf.name, swf.nameLength), swf.version, swf.offset, swf.length);
    scanTime.start();
    commitId = _dedup->scan(swf.data, location, logicalBlocks);
    scanTime.stop();

    if (logicalBlocks.empty()) {
        LOG(ERROR) << "Failed to write file stripe, deduplication results is empty!";
        return false;
    }

    unsigned int physicalLength = 0;

    // trim duplicate blocks, and construct the set of address mapping
    for (auto it = logicalBlocks.begin(); it != logicalBlocks.end(); it++) {
        unsigned int inStripeOffset = it->first._offset - swf.offset;
        unsigned int blockLength = it->first._length;

        // duplicate blocks
        if (it->second.second) {
            buildListTime.resume();
            // create a logical-to-physical address mapping, along with fingerprint and block length
            duplicateFps.insert(std::make_pair(it->first, it->second.first));
            buildListTime.stop();
            continue;
        }

        copyTime.resume();
        // unique blocks
        // move the data from original logical offset to new physical offset
        memmove(swf.data + physicalLength, swf.data + inStripeOffset, blockLength);
        copyTime.stop();

        buildListTime.resume();
        // create a logical-to-physical address mapping, along with fingerprint and block length
        uniqueFps.insert(std::make_pair(it->first, std::make_pair(it->second.first, physicalLength)));
        buildListTime.stop();

        // update physical stripe length
        physicalLength += blockLength;
    }

    // update physical stripe size to encode
    swf.length = physicalLength;
    LOG(INFO) << "Write file " << swf.name << " deduplicated stripe of size " << physicalLength << " bytes"
            << ", (scan-for-unique) = " << scanTime.elapsed().wall * 1.0 / 1e6  << " ms"
            << ", (move-data) = " << copyTime.elapsed().wall * 1.0 / 1e6  << " ms"
            << ", (build-fp-list) = " << buildListTime.elapsed().wall * 1.0 / 1e6  << " ms"
    ;

    return true;
}

bool Proxy::prepareWrite(File &f, File &wf, int *&spareContainers, int &numSelected, bool needsFindSpareContainers) {
    // copy name, size, time
    if (wf.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file name and size for write operaiton";
        return false;
    }

    // coding
    if (_chunkManager->setCodingMeta(f.storageClass, f.codingMeta) == false) {
        LOG(ERROR) << "Failed to find the coding metadata of class " << f.storageClass;
        return false;
    }
    wf.copyStoragePolicy(f);

    // offset and length
    wf.copyOperationDataRange(f);

    CodingMeta &cmeta = wf.codingMeta;
    // find available containers for write
    int numContainers = wf.size > 0? _chunkManager->getNumRequiredContainers(cmeta.coding, cmeta.n, cmeta.k) : 0;
    if (numContainers == -1) {
        LOG(ERROR) << "Insufficient number of containers for " << wf.codingMeta.print();
        wf.data = 0;
        return false;
    }

    bool newSpareContainersLocally = false;
    if (spareContainers == NULL) {
        spareContainers = new int[numContainers];
        newSpareContainersLocally = true;
    }

    if (!needsFindSpareContainers)
        return true;

    unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize);
    if (maxDataStripeSize == 0) {
        LOG(ERROR) << "Failed to get max data stripe size for config " << cmeta.print();
        if (newSpareContainersLocally) {
            delete [] spareContainers;
            spareContainers = 0;
        }
        return false;
    }
    bool isSmallFile = wf.size < maxDataStripeSize;
    unsigned long int extraDataSize = _chunkManager->getPerStripeExtraDataSize(wf.storageClass);
    int alignedStart = f.offset / maxDataStripeSize;
    int alignedEnd = (f.offset + f.length + maxDataStripeSize - 1) / maxDataStripeSize;
    int numStripes = isSmallFile? 1 : alignedEnd - alignedStart; 

    numSelected = _coordinator->findSpareContainers(
        /* existing containers */ NULL,
        /* num existing containers */ 0,
        /* container status */ NULL,
        spareContainers,
        numContainers,
        (isSmallFile? wf.size : maxDataStripeSize * numStripes) + extraDataSize * numStripes,
        cmeta
    );

    int minNumContainers = _chunkManager->getMinNumRequiredContainers(wf.storageClass);
    if (numSelected < minNumContainers || minNumContainers == -1) {
        LOG(ERROR) << "Failed to write file " << f.name << ", only " << numSelected << " of " << numContainers << " coantiners available, needs at least " << minNumContainers;
        wf.data = 0;
        if (newSpareContainersLocally) {
            delete [] spareContainers;
            spareContainers = 0;
        }
        return false;
    }

    DLOG(INFO) << "Found " << numSelected << " containers for file " << f.name;

    return true;
}

bool Proxy::readFile(boost::uuids::uuid fuuid, File &f) {
    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    // get the file name using fuuid
    if (!_metastore->getFileName(fuuid, f))
        return false;
    // read using filename
    return readFile(f);
}

bool Proxy::readFile(File &f, bool isPartial) {
    File rf;
    boost::timer::cpu_timer all, getMeta, readData, processfp, updateMeta, memoryCopy, dataBufferAlloc, cleanup;
    memoryCopy.stop();

    TagPt overallT;
    overallT.markStart();

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    if (rf.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file metadata for read operaiton";
        return false;
    }
    rf.copyVersionControlInfo(f);

    getMeta.start();
    // get file metadata
    if (_metastore->getMeta(rf) == false) {
        LOG(WARNING) << "Failed to find file metadata for file " << f.name;
        return false;
    }
    LOG(INFO) << "Read file " << f.name << ", metadata found ";
    getMeta.stop();

    // record the last access time
    rf.atime = time(NULL);

    readData.start();
    // handle file with staged copy, read from the staged copy if it exists and is most recent
    FileInfo rinfo;
    f.copyNameToInfo(rinfo);
    if (_stagingEnabled && _staging->getFileInfo(rinfo) && rf.staged.mtime >= rf.mtime && rinfo.mtime >= rf.staged.mtime) {
        if (f.length == INVALID_FILE_LENGTH) {
            f.length = rf.staged.size;
        }
        if (_staging->readFile(f)) {
            f.size = f.length;
            return true;
        }
    }
    readData.stop();

    // handle empty file
    if (rf.size == 0 || rf.numStripes == 0) {
        f.data = (unsigned char *) malloc (1);
        f.data[0] = 0;
        return true;
    }

    bool preallocated = f.data != 0;
    // full file read (not expected from large files but will try)
    if (f.length == INVALID_FILE_LENGTH) {
        f.length = rf.size;
    }
    // assume reading from the beginning of a file if offset is not provided
    if (f.offset == INVALID_FILE_OFFSET) {
        f.offset = 0;
    }

    int numChunksPerStripe = rf.numChunks / rf.numStripes;
    CodingMeta &cmeta = rf.codingMeta;
    unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize, /* full chunk size */ true);
    if (isPartial && (f.offset % maxDataStripeSize != 0 || f.length % maxDataStripeSize != 0)) {
        LOG(ERROR) << "Unaligned partial read at offset " << f.offset << " and size " << f.length << " not supported (alignment is " << maxDataStripeSize << ")";
        return false;
    }

    unsigned long int bytesRead = 0;

    processfp.start();
    /** 
     * Sort the fingerprints (i.e., the blocks) in the current read range as 
     * internal (data is physically inside the object) and otherwise external
     *
     * For internal data, simply construct a mapping of '' <local offset> -> <physical offset, length> ''
     *
     * For external data, query the logical locations of fingerprints,  '' <fp> -> <external object name, external logical offset, external physical in-stripe offset>  ''
     * then merge with existing mapping of  '' <local offset, legnth> -> <fp> ''
     * to form a mapping  '' <local offset, length> -> <external physical in-strip offset, external object name, external logical offset> ''
     *
     * After that, read all the duplicate blocks from the external objects as
     * stripes and copy duplicated data back, followed the internal object 
     * stripes.
     **/
    std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> >::iterator uniqueStartFp = rf.uniqueBlocks.lower_bound(BlockLocation::InObjectLocation(f.offset, 0));
    std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, int> >::iterator uniqueEndFp = rf.uniqueBlocks.upper_bound(BlockLocation::InObjectLocation(f.offset + f.length - 1, 0));
    std::map<BlockLocation::InObjectLocation, Fingerprint>::iterator duplicateStartFp = rf.duplicateBlocks.lower_bound(BlockLocation::InObjectLocation(f.offset, 0));
    std::map<BlockLocation::InObjectLocation, Fingerprint>::iterator duplicateEndFp = rf.duplicateBlocks.upper_bound(BlockLocation::InObjectLocation(f.offset + f.length - 1, 0));

    std::map<StripeLocation, std::vector<std::pair<int, BlockLocation::InObjectLocation> > > externalBlockLocs; // <ext object, ext logical offset> -> <ext physical offset, [<internal logical offset, length>]
    std::map<unsigned long int, BlockLocation::InObjectLocation> internalBlockLocs; // logical offset -> <physical offset, length>
    std::map<StripeLocation, std::set<int> > externalStripes; // <object, logical offset> -> [stripe ids]
    std::map<std::string, File*> externalFiles; // <file name, file metadata (pointer)>

    std::vector<Fingerprint> duplicateBlockFps;

    // abort if no fingerprint is available
    if ((uniqueStartFp == rf.uniqueBlocks.end() && duplicateStartFp == rf.duplicateBlocks.end()) || (uniqueStartFp == uniqueEndFp && duplicateStartFp == duplicateEndFp)) {
        LOG(ERROR) << "Failed to find any fingerprints (i.e., blocks) mapping of file " << rf.name << " for range (" << f.offset << ", " << f.length << ") among " << rf.uniqueBlocks.size() << " + " << rf.duplicateBlocks.size() << " fingerprints";
        return false;
    }

#define clean_external_filemeta() do { \
    for (auto fit = externalFiles.begin(); fit != externalFiles.end(); fit++) { \
        delete fit->second; \
        fit->second =  0; \
    } \
} while (0) \

    if (!sortStripesAndBlocks(f.namespaceId, rf.name, uniqueStartFp, uniqueEndFp, duplicateStartFp, duplicateEndFp, &externalBlockLocs, &internalBlockLocs, externalStripes, externalFiles, duplicateBlockFps)) {
        clean_external_filemeta();
        return false;
    }
    processfp.stop();

    dataBufferAlloc.start();
    // use preallocated memory if any, or allocate a read buffer here
    if (preallocated) {
        rf.data = f.data;
    } else {
        rf.data = (unsigned char *) malloc (f.length);
        if (rf.data == 0) {
            LOG(ERROR) << "Failed to allocate memory (size = " << rf.length << ") for read";
            clean_external_filemeta();
            return false;
        }
    }
    dataBufferAlloc.stop();

    // adjust such that rf.data always points to the (virtual) start of file
    rf.data -= f.offset;

    readData.resume();
    // read the duplicate data in other objects
    for (auto it = externalStripes.begin(); it != externalStripes.end(); it++) {
        // obtain the saved object metadata
        File *ef = 0;
        try {
            ef = externalFiles.at(it->first._objectName);
        } catch (std::out_of_range &e) {
            LOG(ERROR) << "Cannot find any saved external file metadata of referenced file " << it->first._objectName << ", abort reading duplicate blocks for file " << f.name;
            rf.data += f.offset;
            if (preallocated) { rf.data = 0; }
            clean_external_filemeta();
            return false;
        }

        // set the stripe offset
        ef->offset = it->first._offset;

        // figure out the stripe length
        CodingMeta &cmeta = ef->codingMeta;
        unsigned long int maxDataSizePerStripe = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize, /* is full chunk */ true);
        ef->length = std::min(maxDataSizePerStripe, ef->size - ef->offset);
        DLOG(INFO) << "Read stripe from external object " << ef->name << " in range (" << ef->offset << ", " << ef->length << ")";

        // figure out the number of chunks and the container liveness
        int numRequiredContainers = _chunkManager->getNumRequiredContainers(cmeta.coding, cmeta.n, cmeta.k);
        int numChunksPerContainer = _chunkManager->getNumChunksPerContainer(cmeta.coding, cmeta.n, cmeta.k);
        int numChunksPerStripe = numRequiredContainers * numChunksPerContainer;
        int stripeId = ef->offset / maxDataSizePerStripe;
        bool chunkIndices[numChunksPerStripe];
        _coordinator->checkContainerLiveness(ef->containerIds + stripeId * numChunksPerStripe, numChunksPerStripe, chunkIndices);

        File erf;
        if (copyFileStripeMeta(erf, *ef, stripeId, "read") == false) {
            rf.data += f.offset;
            if (preallocated) { rf.data = 0; }
            clean_external_filemeta();
            return false;
        }

        // read the stripe
        if (!_chunkManager->readFileStripe(erf, chunkIndices)) {
            // TODO clean up
            LOG(ERROR) << "Failed to read file " << f.name << " from backend";
            rf.data += f.offset;
            if (preallocated) { rf.data = 0; }
            clean_external_filemeta();
            unsetCopyFileStripeMeta(erf);
            return false;
        }

        // find the logical address range of duplicate blocks referenced in this external stripe
        auto startIt = externalBlockLocs.lower_bound(it->first);
        if (startIt == externalBlockLocs.end()) {
            LOG(WARNING) << "Read stripe at " << ef->offset << " from referenced file " << ef->name << " but no physical blocks are copied.";
            continue;
        }
        StripeLocation endLoc = it->first;
        endLoc._offset += ef->length - 1;
        auto endIt = externalBlockLocs.upper_bound(endLoc);

        // copy the data from this external stripe back to the original data buffer
        for (auto vit = startIt; vit != endIt; vit++) { // block location vector
            for (auto bit = vit->second.begin(); bit != vit->second.end(); bit++) { // block location
                unsigned long int objOffset = bit->second._offset;
                unsigned int length = std::min(f.offset + f.length - objOffset, static_cast<unsigned long int>(bit->second._length));
                int stripeOffset = bit->first;

                memoryCopy.resume();
                //DLOG(INFO) << "Copy external block at (" << stripeOffset << ") to (" << objOffset << ", " << length << ")";
                memcpy(rf.data + objOffset, erf.data + stripeOffset, length);
                memoryCopy.stop();
                bytesRead += length;
            }
        }

        unsetCopyFileStripeMeta(erf);
    }

    // make it back to the actual data buffer starting address
    rf.data += f.offset;

    // read the unique data in the range
    bool chunkIndices[numChunksPerStripe];
    // adjust such that rf.data always points to the (virtual) start of file
    rf.data -= f.offset;
    // decode stripe by stripe
    bool okay = true;
    int startStripe = isPartial? f.offset / maxDataStripeSize : 0;
    int endStripe = isPartial && f.offset + f.length <= rf.size? (f.offset + f.length) / maxDataStripeSize : rf.numStripes;
    int currStripeId = 0;
    unsigned char *tmpBuffer = 0;
    unsigned long int bufferSize = 0;

    for (int i = startStripe; i < endStripe; i++, currStripeId++) {
        File srf;

        // copy the stripe metadata
        if (copyFileStripeMeta(srf, rf, i, "read") == false) {
            if (preallocated) { rf.data = 0; }
            clean_external_filemeta();
            return false;
        }
        srf.blockId = f.blockId;
        srf.stripeId = currStripeId;

        // mark the offset and length
        srf.offset = 0;
        srf.length = srf.size;
        // skip empty (i.e., fully deduplicated) stripes
        if (srf.chunks[0].size == 0) {
            if (preallocated) { rf.data = 0; }
            unsetCopyFileStripeMeta(srf);
            continue;
        }
        // check for alive containers
        _coordinator->checkContainerLiveness(srf.containerIds, srf.numChunks, chunkIndices);
        // read the data from stripe
        unsigned long int actualDataStripeSize = _chunkManager->getDataStripeSize(cmeta.coding, cmeta.n, cmeta.k, srf.size);
        bool unalignedStripe = i + 1 == rf.numStripes && (rf.size % maxDataStripeSize != 0); // last stripe may be unaligned
        bool useTempBuffer = unalignedStripe || actualDataStripeSize > maxDataStripeSize;
        if (useTempBuffer) {
            // allocate buffer on first use, or when the size is not sufficiently large
            if (tmpBuffer == 0 || bufferSize < actualDataStripeSize || bufferSize < maxDataStripeSize) {
                free(tmpBuffer);
                bufferSize = std::max(actualDataStripeSize, maxDataStripeSize);
                DLOG(INFO) << bufferSize;
                tmpBuffer = static_cast<unsigned char *>(calloc (bufferSize, 1));
                if (tmpBuffer == 0) {
                    LOG(ERROR) << "Out of memory for reading stripes for file " << f.name;
                    unlockFile(rf);
                    clean_external_filemeta();
                    unsetCopyFileStripeMeta(srf);
                    return false;
                }
            }
            // zero out the zone to use
            memset(tmpBuffer, 0, actualDataStripeSize);
            // assigned it to the stripe
            srf.data = tmpBuffer;
        } else { // aligned stripes
            srf.data = rf.data + i * maxDataStripeSize;
        }
        if (_chunkManager->readFileStripe(srf, chunkIndices) == false) {
            LOG(ERROR) << "Failed to read file " << f.name << " from backend (stripe " << i << ")";
            okay = false;
        }
        if (useTempBuffer) { // copy data back to the original file data buffer
            // directly copy all data read
            memcpy(rf.data + i * maxDataStripeSize, srf.data, srf.size);
            bytesRead += srf.size;
            // if buffer is replaced by lower level functions, free the new buffer and reset to tmp buffer pointer to avoid double free
            if (srf.data != tmpBuffer) {
                tmpBuffer = 0;
                bufferSize = 0;
                free(srf.data);
            }
        } else {
            bytesRead += srf.size;
	}
        // unset the data reference to the original file data buffer or the temp buffer
        srf.data = 0;
        // clean up (avoid double free)
        unsetCopyFileStripeMeta(srf);
        // skip once read failed
        if (!okay) {
            if (preallocated) {
                rf.data = 0;
            } else {
                rf.data += f.offset;
            }
            clean_external_filemeta();
            return false;
        }
    }
    // make it back to the actual data buffer starting address
    rf.data += f.offset;
    readData.stop();

    // pass the number of bytes decoded to caller
    if (!isPartial)
        f.size = rf.size;
    else
        f.size = bytesRead;
    // pass the decoded data to caller
    f.data = rf.data;
    rf.data = 0;
    // pass timestamps
    f.setTimeStamps(rf.ctime, rf.mtime, rf.atime);
    // report data read speed

    updateMeta.start();
    if (_metastore->updateTimestamps(rf) == false) {
        LOG(WARNING) << "Failed to update timestamp of file " << f.name << " after read";
    }
    updateMeta.stop();

    overallT.markEnd();
    
    boost::timer::cpu_times duration = readData.elapsed(), metaDuration = getMeta.elapsed();
    const std::map<std::string, double> stats = genStatsMap(duration, metaDuration, f.size);
    _statsSaver.saveStatsRecord(stats, "read", std::string(f.name, f.nameLength), overallT.getStart().sec(), overallT.getEnd().sec());

    // TODO write back to staging in background
    if (_stagingEnabled && !isPartial) {
    }

    cleanup.start();
    free(tmpBuffer);

    clean_external_filemeta();
    cleanup.stop();

    LOG_IF(INFO, duration.wall > 0) << "Read file " << f.name << ", (data) speed = " << (f.size * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << f.size * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " s)";
    LOG(INFO) << "Read file " << f.name 
            << ", (data-buf-alloc) = " << dataBufferAlloc.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (get-meta) = " << metaDuration.wall * 1.0 / 1e6 << " ms"
            << ", (process-fp) = " << processfp.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (update-meta) = " << updateMeta.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (clean-up) = " << cleanup.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (memcpy-in-read-data) = " << memoryCopy.elapsed().wall * 1.0 / 1e6 << " ms"
    ;
    LOG(INFO) << "Num. of external files/stripes referenced = " << externalFiles.size() << "/" << externalStripes.size(); 
    LOG(INFO) << "Read file " << f.name << ", completes in " << all.elapsed().wall * 1.0 / 1e9 << " s";

    return true;

#undef clean_external_filemeta
}

bool Proxy::sortStripesAndBlocks(
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
        int dataStripeSize
) {

    // report error on missing output place holder
    if (externalBlockLocs == 0 || internalBlockLocs == 0) {
        return false;
    }

    bool stripeSizeProvided = dataStripeSize != -1; 
    boost::timer::cpu_timer fpScan, queryLoc, getExtFileMeta, accuExtStripe;
    getExtFileMeta.stop();
    accuExtStripe.stop();

    // worst-case complexity: 2 * num blocks
    int stripeIdx = 0, prevStripeIdx = -1, startingStripeIdx = stripeSizeProvided? std::min(uniqueStartFp->first._offset, duplicateStartFp->first._offset) / dataStripeSize : 0;
    std::map<unsigned long int, BlockLocation::InObjectLocation>::iterator inBlocksIt;
    
    fpScan.start();
    // complexity: scan the list once O(num blocks)
    // accumulate (1) fingerprints to query and (2) unqiue blocks
    for (auto fpIt = duplicateStartFp; fpIt != duplicateEndFp; fpIt++) {
        duplicateBlockFps.push_back(fpIt->second);
    }
    for (auto fpIt = uniqueStartFp; fpIt != uniqueEndFp; fpIt++) {
        int physicalOffset = fpIt->second.second;
        stripeIdx = stripeSizeProvided? fpIt->first._offset / dataStripeSize - startingStripeIdx : 0;
        if (prevStripeIdx != stripeIdx) {
            inBlocksIt = internalBlockLocs[stripeIdx].empty()? internalBlockLocs[stripeIdx].begin() : std::prev(internalBlockLocs[stripeIdx].end());
        } else if (inBlocksIt->first + inBlocksIt->second._length == fpIt->first._offset && inBlocksIt->second._offset + inBlocksIt->second._length == (size_t) physicalOffset) { // coalesce with previous block within the same stripe for copying
            inBlocksIt->second._length += fpIt->first._length;
            continue;
        }
        inBlocksIt = internalBlockLocs[stripeIdx].emplace_hint(inBlocksIt, std::make_pair(fpIt->first._offset, BlockLocation::InObjectLocation(physicalOffset, fpIt->first._length)));
        prevStripeIdx = stripeIdx;
    }
    fpScan.stop();

    queryLoc.start();
    // query block locations
    std::vector<BlockLocation> duplicateBlockLoc = _dedup->query(namespaceId, duplicateBlockFps);

    if (duplicateBlockLoc.size() != duplicateBlockFps.size()) {
        LOG(ERROR) << "Failed to find sufficient physical locations of the duplicated blocks of file " << name << " (expect " << duplicateBlockFps.size() << ", but got " << duplicateBlockLoc.size() << ")";
        return false;
    }
    queryLoc.stop();

    // complexity: scan all duplicate block once O(num duplicate blocks)
    // process the block locations
    size_t numDuplicateBlocks = duplicateBlockFps.size();
    auto dit = duplicateStartFp;
    for (size_t i = 0; i < numDuplicateBlocks; i++, dit++) {

        BlockLocation &blockLoc = duplicateBlockLoc.at(i);

        stripeIdx = stripeSizeProvided? dit->first._offset / dataStripeSize - startingStripeIdx : 0;

        getExtFileMeta.resume();
        // find the file metadata for the duplicate block
        File *ef = 0;
        const std::string &extFilename = blockLoc.getObjectID();
        auto fit = externalFiles.find(extFilename);
        if (fit == externalFiles.end()) {
            // query the file metadata from metastore
            ef = new File();
            ef->setVersion(blockLoc.getObjectVersion());
            std::string objectName = blockLoc.getObjectName();
            ef->setName(objectName.data(), objectName.size());
            ef->namespaceId = blockLoc.getObjectNamespaceId();
            if (!_metastore->getMeta(*ef, /* get blocks type (unqiue) */ 1)) {
                LOG(ERROR) << "Failed to find the physical location of the duplicated block for file " << name << " referencing a non-existing file " << extFilename;
                delete ef;
                return false;
            }
            // save the queried file metadata for future use
            externalFiles.emplace(std::make_pair(extFilename, ef));
            //DLOG(INFO) << "Duplicated block(s) to be read from an external object " << extFilename << " in namespace " << (int) blockLoc.getObjectNamespaceId() << " version " << ef->version;
        } else {
            // reuse saved file metadata
            ef = fit->second;
        }
        getExtFileMeta.stop();

        // figure out the target stripe (starting offset and length) to read
        CodingMeta &cmeta = ef->codingMeta;
        unsigned long int maxDataSizePerStripe = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize, /* is full chunk */ true);

        unsigned long int extStripeOffset = blockLoc.getBlockOffset();
        unsigned long int extStripeAlignedOffset = extStripeOffset / maxDataSizePerStripe * maxDataSizePerStripe;

        StripeLocation stripe(extFilename, extStripeAlignedOffset);

        accuExtStripe.resume();
        // mark the stripe as pending to read
        bool marked = false;
        std::tie(std::ignore, marked) = externalStripes.emplace(std::make_pair(stripe, std::set<int>()));
        if (!marked) {
            // TODO may init async read here and wait for the results
        }
        externalStripes.at(stripe).insert(stripeIdx);

        // figure out the physical offset of the block in the stripe
        try {
            auto &bit = ef->uniqueBlocks.at(blockLoc.getBlockRange());
            // check if fingerprint matches in the external file
            if (bit.first != duplicateBlockFps.at(i)) {
                LOG(ERROR) << "Fingerprint record mismatch for block location " << blockLoc.print() << ", expect " << duplicateBlockFps.at(i).toHex() << " got " << bit.first.toHex();
                throw std::out_of_range("Fingerprint mismatch error");
            }
            // update the mapping of internal logical address to external stripe mapping (object name, logical offset, physical in-stripe offset)
            stripe._offset = extStripeOffset;
            // TODO: coalesce adjacent blocks (1) batch memcpy() and (2) reduce the number of records, especially for small block sizes
            std::vector<std::pair<int, BlockLocation::InObjectLocation> > locs (1, std::make_pair(bit.second, dit->first));
            std::map<StripeLocation, std::vector<std::pair<int, BlockLocation::InObjectLocation> > >::iterator esIt;
            bool inserted = false;
            std::tie(esIt, inserted) = externalBlockLocs[stripeIdx].emplace(std::make_pair(stripe, locs));
            if (!inserted) {
                esIt->second.emplace_back(std::make_pair(bit.second, dit->first));
            }
        } catch (std::out_of_range &e) {
            LOG(ERROR) << "Cannot find the physcial location of a duplicated block in the source file " << extFilename << " at offset " << extStripeOffset;
            // clean up
            // TODO if stripe read has start, we may need extra handling here
            return false;
        }
        accuExtStripe.stop();
    }

    LOG(INFO) << "Read file " << name 
            << ", (fp-process) = " << fpScan.elapsed().wall * 1.0 / 1e6  << " ms"
            << ", (query-loc) = " << queryLoc.elapsed().wall * 1.0 / 1e6  << " ms"
            << ", (get-ext-meta) = " << getExtFileMeta.elapsed().wall * 1.0 / 1e6 << " ms"
            << ", (add-extra-stripe) = " << accuExtStripe.elapsed().wall * 1.0 / 1e6 << " ms"
    ;
    return true;
}


bool Proxy::readPartialFile(File &f) {
    return readFile(f, /* isPartial */ true);
}

bool Proxy::deleteFile(boost::uuids::uuid fuuid, File &f) {
    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    // get the file name using fuuid
    if (!_metastore->getFileName(fuuid, f))
        return false;
    // delete the file
    return deleteFile(f);
}

bool Proxy::deleteFile(const File &f) {

    File df;

    bool isVersioned = !Config::getInstance().overwriteFiles();

    boost::timer::cpu_times metaDuration, duration;
    boost::timer::cpu_timer all, deleteMeta, deleteData;

    TagPt overallT;
    overallT.markStart();

    deleteMeta.start();
    if (df.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file metadata for delete operaiton";
        return false;
    }
    df.copyVersionControlInfo(f);

    if (df.namespaceId == INVALID_NAMESPACE_ID)
        df.namespaceId = DEFAULT_NAMESPACE_ID;

    // lock file and get metadata for delete
    if (lockFileAndGetMeta(df, "delete file") == false) {
        LOG(ERROR) << "Failed to lock file " << df.name << " for delete";
        return false;
    }

    // journal the chunk deletion operation
    //for (int cidx = 0; cidx < df.numChunks; cidx++) {
    //    int containerId = df.containerIds[cidx];
    //    if (!_metastore->addChunkToJournal(df, df.chunks[cidx], containerId, /* isWrite */ false)) {
    //        LOG(ERROR) << "Failed to journal file " << df.name << " deletion of chunk " << cidx << " in container " << containerId << ".";
    //        unlockFile(df);
    //        return false;
    //    }
    //}

    // delete file metadata
    if (_metastore->deleteMeta(df) == false) {
        LOG(WARNING) << "Failed to find file metadata for file " << f.name;
        unlockFile(df);
        return false;
    }

    // cancel any repair task if file not versioned
    if (!isVersioned) {
        _metastore->markFileAsRepaired(df);
    }

    // metadata time (first part, deleteMeta)
    deleteMeta.stop();

    LOG(INFO) << "Delete file " << f.name << ", metadata deleted";
    DLOG(INFO) << "Delete file " << f.name << ", metadata deleted, v = " << df.version;
    // data time

    deleteData.start();
    // remove data chunks for non-empty files
    if (df.size > 0 && (!isVersioned || df.version != -1)) {
        // check chunk availability
        bool chunkIndices[df.numChunks];
        _coordinator->checkContainerLiveness(df.containerIds, df.numChunks, chunkIndices, /* update first */ true, /* check all */ true, /* UNUSED as not alive */ true);
        // delete the chunks
        if (_chunkManager->deleteFile(df, chunkIndices) == false) {
            LOG(WARNING) << "Failed to delete file " << f.name << " from backend";
            unlockFile(df);
            return false;
        }
        _metastore->markFileAsRepaired(df);
        _metastore->markFileAsWrittenToCloud(df, /* removePending */ true);
    }

    // also delete from staging
    if (_stagingEnabled && !isVersioned) {
        bool success = _staging->deleteFile(df);
        LOG(INFO) << "<STAGING> Delete from Staging " << (success ? "success" : "failed") << ", filename: " << f.name;
    }
    deleteData.stop();

    metaDuration = deleteMeta.elapsed();
    duration = deleteData.elapsed();

    unlockFile(df);

    overallT.markEnd();
    
    const std::map<std::string, double> stats = genStatsMap(duration, metaDuration, df.size);
    _statsSaver.saveStatsRecord(stats, "delete", std::string(f.name, f.nameLength), overallT.getStart().sec(), overallT.getEnd().sec());

    
    LOG(INFO) << "Delete file " << f.name 
            << ", (delete-meta)" << metaDuration.wall * 1.0 / 1e6 << " ms"
            << ", (delete-data)" << duration.wall * 1.0 / 1e6 << " ms";
    LOG(INFO) << "Delete file " << f.name << ", completes in " << all.elapsed().wall * 1.0 / 1e9 << " s";
    
    return true;
}

bool Proxy::renameFile(File &sf, File &df) {
    File srf, drf;

    TagPt overallT;
    overallT.markStart();
    
    if (sf.namespaceId == INVALID_NAMESPACE_ID)
        sf.namespaceId = DEFAULT_NAMESPACE_ID;

    // use the source namespace id if not specified
    if (df.namespaceId == INVALID_NAMESPACE_ID)
        df.namespaceId = sf.namespaceId;

    if (srf.copyNameAndSize(sf) == false) {
        LOG(ERROR) << "Failed to copy file metadata for copy operaiton";
        return false;
    }
    if (drf.copyNameAndSize(df) == false) {
        LOG(ERROR) << "Failed to copy file metadata for copy operaiton";
        return false;
    }

    bool ret = _metastore->markFileAsRepaired(sf);

    // lock source file for rename
    if (lockFileAndGetMeta(srf, "move") == false) {
        LOG(ERROR) << "Failed to lock file " << srf.name << " for rename";
        return false;
    }

    // delete destination file if exists
    if (_metastore->getMeta(drf)) {
        LOG(WARNING) << "Destination " << drf.name << " exists, delete existing file before rename operation";
        if (!deleteFile(drf)) {
            LOG(ERROR) << "Destination " << drf.name << " exists, but failed to delete existing file";
            unlockFile(srf);
            return false;
        }
    }

    // lock destination file for rename
    if (lockFile(drf) == false) {
        LOG(ERROR) << "Failed to lock file " << drf.name << " for rename";
        unlockFile(srf);
        return false;
    }

    // offset and length of data to copy
    srf.offset = 0;
    srf.length = srf.size;
    drf.version = srf.version;

    // move chunks
    if (_chunkManager->moveFile(srf, drf) == false) {
        LOG(ERROR) << "Failed to rename file " << srf.name << ", failed to move chunks";
        unlockFile(srf);
        unlockFile(drf);
        return false;
    }

    // update the unique fingerprint list for the deduplication
    BlockLocation obl, nbl;
    obl.setObjectID(srf.namespaceId, std::string(srf.name, srf.nameLength), srf.version);
    nbl.setObjectID(drf.namespaceId, std::string(drf.name, drf.nameLength), drf.version);
    std::vector<Fingerprint> fps;
    std::vector<BlockLocation> oldBlockLocations;
    std::vector<BlockLocation> newBlockLocations; 
    for (auto &rec : srf.uniqueBlocks) {
        // skip duplicated blocks
        if (rec.second.second == -1) { continue; }
        // set fingerprint, old block location and new block location
        fps.emplace_back(rec.second.first);
        obl.setBlockRange(rec.first);
        oldBlockLocations.emplace_back(obl);
        nbl.setBlockRange(rec.first);
        newBlockLocations.emplace_back(nbl);
    }
    _dedup->commit(_dedup->update(fps, oldBlockLocations, newBlockLocations));

    // update file metadata
    if (_metastore->renameMeta(srf, drf) == false) {
        LOG(WARNING) << "Failed to rename file for file " << sf.name << ", failed to update metadata";
        drf.offset = 0;
        drf.size = srf.size;
        drf.length = srf.length;
        // try to revert data changes
        _chunkManager->moveFile(drf, srf);
        _dedup->commit(_dedup->update(fps, newBlockLocations, oldBlockLocations));
        unlockFile(srf);
        unlockFile(drf);
        return false;
    }
    LOG(INFO) << "Rename file " << sf.name << "(" << sf.uuid << ") to " << df.name << "(" << df.uuid << ")";

    if (ret)
        _metastore->markFileAsNeedsRepair(df);

    overallT.markEnd();

    const std::map<std::string, double> stats;
    _statsSaver.saveStatsRecord(stats, "rename", std::string(srf.name, srf.nameLength), overallT.getStart().sec(), overallT.getEnd().sec(), std::string(df.name, df.nameLength));

    unlockFile(srf);
    unlockFile(drf);
    return true;
}

bool Proxy::copyFile(boost::uuids::uuid fuuid, File &sf, File &df) {
    if (sf.namespaceId == INVALID_NAMESPACE_ID)
        sf.namespaceId = DEFAULT_NAMESPACE_ID;

    // get the file name using fid
    if (!_metastore->getFileName(fuuid, sf))
        return false;
    return copyFile(sf, df);
}

bool Proxy::copyFile(File &sf, File &df) {
    File srf, drf, rf;
    boost::timer::cpu_timer all, copyMeta, copyData, processMeta;

    TagPt overallT;
    overallT.markStart();

    if (sf.namespaceId == INVALID_NAMESPACE_ID)
        sf.namespaceId = DEFAULT_NAMESPACE_ID;
    // use source namespace id if not specified
    if (df.namespaceId == INVALID_NAMESPACE_ID)
        df.namespaceId = sf.namespaceId;

    if (srf.copyNameAndSize(sf) == false) {
        LOG(ERROR) << "Failed to copy file metadata for copy operaiton";
        return false;
    }

    if (drf.copyNameAndSize(df) == false) {
        LOG(ERROR) << "Failed to copy file metadata for copy operaiton";
        return false;
    }

    copyMeta.start();
    // lock both the source and destination
    if (lockFileAndGetMeta(srf, "copy") == false)
        return false;

    LOG(INFO) << "Copy file " << sf.name << " to " << df.name << ", source file metadata found";

    if (_metastore->lockFile(df) == false) {
        LOG(ERROR) << "Failed to lock destination file " << df.name << " for copying\n";
        unlockFile(srf);
        return false;
    }
    copyMeta.stop();

    // offset and length of data to copy
    srf.offset = sf.offset == INVALID_FILE_OFFSET? 0 : sf.offset;
    if (sf.length == INVALID_FILE_LENGTH || sf.length + sf.offset >= srf.size)
        srf.length = srf.size - srf.offset;
    else
        srf.length = sf.length;

    bool destExists = false;
    // if the copying is partial, check out destination file
    rf.namespaceId = df.namespaceId;
    rf.name = df.name;
    rf.nameLength = df.nameLength;
    destExists = _metastore->getMeta(rf);
    drf.version = 0;

    int start, end;

    copyData.start();
    // only copy chunks if deduplication is disabled
    if (_chunkManager->copyFile(srf, drf, &start, &end) == false) {
        LOG(ERROR) << "Failed to copy file " << sf.name << " to " << df.name << " in backend";
        unlockFile(srf);
        unlockFile(df);
        rf.name = 0;
        return false;
    }
    copyData.stop();

    processMeta.start();
    // copy stripe metadata for non-empty file
    drf.copyStoragePolicy(srf);
    if (drf.numStripes > 0) {
        // copy meta of stripes before
        int numChunksPerStripe = drf.numChunks / drf.numStripes;
        for (int i = 0; destExists && i < start * numChunksPerStripe; i++) {
            drf.chunks[i].copyMeta(rf.chunks[i]);
        }
        if (destExists && start > 0)
            memcpy(drf.containerIds, rf.containerIds, sizeof(int) * numChunksPerStripe * start);
        // copy meta of stripes after
        for (int i = end * numChunksPerStripe; destExists && i < rf.numStripes * numChunksPerStripe; i++) {
            drf.chunks[i].copyMeta(rf.chunks[i]);
        }
        if (destExists && end < rf.numStripes) {
            memcpy(drf.containerIds + end * numChunksPerStripe, rf.containerIds + end * numChunksPerStripe, sizeof(int) * numChunksPerStripe * (rf.numStripes - end));
            drf.size = rf.size;
        }
    }
    drf.numStripes = srf.numStripes;

    // copy version and modification of file
    drf.ctime = destExists? rf.ctime : time(NULL);
    drf.mtime = destExists? time(NULL) : drf.ctime;
    drf.atime = drf.mtime;

    // copy md5 checksum
    memcpy(drf.md5, srf.md5, MD5_DIGEST_LENGTH);

    // copy the fingerprints
    drf.duplicateBlocks = srf.duplicateBlocks;
    drf.uniqueBlocks = srf.uniqueBlocks;

    processMeta.stop();

    copyMeta.resume();
    // update metadata
    if (_metastore->putMeta(drf) == false) {
        LOG(ERROR) << "Failed to update file metadata of file " << df.name;
        unlockFile(srf);
        unlockFile(df);
        rf.name = 0;
        return false;
    }
    copyMeta.stop();

    // pass the info back to caller
    df.uuid = drf.uuid;
    df.size = drf.size;
    df.ctime = drf.ctime;
    df.mtime = drf.mtime;
    df.atime = drf.atime;
    memcpy(df.md5, drf.md5, MD5_DIGEST_LENGTH);

    // unlock the files
    unlockFile(srf);
    unlockFile(df);

    rf.name = 0;

    // time reporting
    boost::timer::cpu_times duration = copyData.elapsed(), metaDuration = copyMeta.elapsed();
    overallT.markEnd();

    const std::map<std::string, double> stats = genStatsMap(duration, metaDuration, drf.size);
    _statsSaver.saveStatsRecord(stats, "copy", std::string(sf.name, sf.nameLength), overallT.getStart().sec(), overallT.getEnd().sec(), std::string(df.name, df.nameLength));

    LOG(INFO) << "Copy file " << sf.name << " to " << df.name 
            << ", (meta) = " << metaDuration.wall * 1.0 / 1e6 << " ms"
            << ", (process-meta) = " << processMeta.elapsed().wall * 1.0 / 1e6 << " ms";
    LOG_IF(INFO, duration.wall > 0) << "Copy file " << sf.name << " to " << df.name << ", (data) speed = " << (srf.length * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << srf.length * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";
    LOG(INFO) << "Copy file " << sf.name << " to " << df.name << ", completes in " << all.elapsed().wall * 1.0 / 1e9 << " s";
    return true;
}

bool Proxy::repairFile(const File &f, bool isBg) {
    File rf;
    boost::timer::cpu_timer mytimer;

    // Benchmark
    Benchmark &bm = Benchmark::getInstance();
    BMRepair *bmRepair = dynamic_cast<BMRepair *>(bm.at(rf.reqId));
    if (bmRepair)
        bmRepair->proxyOverallTime.markStart();

    if (rf.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file metadata for delete operaiton";
        return false;
    }
    if (rf.namespaceId == INVALID_NAMESPACE_ID)
        rf.namespaceId = DEFAULT_NAMESPACE_ID;

    rf.copyVersionControlInfo(f);

    // TAGPT (start): getMeta
    if (bmRepair)
        bmRepair->getMeta.setStart(bmRepair->proxyOverallTime.getStart());

    // lock file and get metadata for repair
    if (lockFileAndGetMeta(rf, "repair") == false)
        return false;

    LOG(INFO) << "Repair file " << f.name << ", metadata found";

    // report metadata read time
    LOG(INFO) << "Repair file " << f.name << ", (meta, get) duration = " << mytimer.elapsed().wall * 1.0 / 1e6 << " milliseconds";
    
    // TAGPT (end): getMeta
    if (bmRepair) {
        bmRepair->getMeta.markEnd();
        bmRepair->setMeta(rf.reqId, rf.size, rf.name, rf.nameLength, 0);
    }

    // avoid repairing empty file
    if (rf.size == 0 || rf.numStripes == 0) {
        LOG(WARNING) << "Repair file " << f.name << " with no stripes";
        unlockFile(rf);
        return true;
    }

    mytimer.start();
    //for (int i = 0; i < rf.numChunks; i++) DLOG(INFO) << "Chunk " << i << " container = " << rf.containerIds[i];
    unsigned long int repairSize = 0;
    int numChunksPerStripe = rf.numChunks / rf.numStripes;

    // TAGPT (start): dataRepair
    if (bmRepair)
        bmRepair->dataRepair.markStart();

    std::vector<int> chunksToCheckForJournal;

    for (int i = 0; i < rf.numStripes; i++) {

        File srf;
        if (copyFileStripeMeta(srf, rf, i, "repair") == false) {
            unlockFile(rf);
            return false;
        }

        srf.length = rf.chunks[i * numChunksPerStripe].size * rf.codingMeta.k;
        srf.offset = i * rf.chunks[0].size * numChunksPerStripe / rf.codingMeta.n * rf.codingMeta.k;
        
        // check the chunk availability
        bool chunkIndicator[srf.numChunks];
        int numFailed = _coordinator->checkContainerLiveness(srf.containerIds, srf.numChunks, chunkIndicator);

        // skip if no repair is needed
        if (numFailed == 0) {
            unsetCopyFileStripeMeta(srf);
            continue;
        }

        // save the failed chunks for journal removal
        //for (int cidx = 0; cidx < srf.numChunks; cidx++) {
        //    if (!chunkIndicator[cidx] && srf.containerIds[cidx] == INVALID_CONTAINER_ID) {
        //        chunksToCheckForJournal.push_back(srf.chunks[cidx].getChunkId());
        //    }
        //}

        // check for spare containers for repaired data
        int numChunksPerNode = numChunksPerStripe / rf.codingMeta.n;
        numFailed /= numChunksPerNode;
        int spareContainers[numFailed];
        int selected = _coordinator->findSpareContainers(srf.containerIds, srf.numChunks, chunkIndicator, spareContainers, numFailed, srf.chunks[0].size * srf.codingMeta.k, srf.codingMeta);
        if (selected < numFailed) {
            LOG(ERROR) << "Failed to repair file " << rf.name << " only " << selected << " containers for " << numFailed << " failed chunks";
            unlockFile(rf);
            unsetCopyFileStripeMeta(srf);
            return false;
        }

        // obtain the chunk group information
        int chunkGroups[srf.numChunks * (srf.numChunks + 1)];
        int numChunkGroups = _coordinator->findChunkGroups(srf.containerIds, srf.numChunks, chunkIndicator, chunkGroups);
        DLOG(INFO) << "Repair file " << rf.name << ", alive chunks in " << numChunkGroups << " groups, stripe " << i << ", num failed = " << numFailed;
        /*
        for (int i = 0; i < numChunkGroups; i++)
            for (int j = 0; j < chunkGroups[i * (rf.numChunks + 1)]; j++)
                DLOG(INFO) << "Group " << i << " chunk " << j << " id = " << chunkGroups[i * (rf.numChunks + 1) + j + 1];
        */
        // repair the chunks
        if (
            (!isBg && _chunkManager->repairFile(srf, chunkIndicator, spareContainers, chunkGroups, numChunkGroups) == false)
            ||
            (isBg && _repairChunkManager->repairFile(srf, chunkIndicator, spareContainers, chunkGroups, numChunkGroups) == false)
        ) {
            LOG(WARNING) << "Failed to repair file " << rf.name << " at backend";
            unlockFile(rf);
            unsetCopyFileStripeMeta(srf); // avoid double free if referring to rf.codingMeta.info, i.e., this is not allocated in the repair file process
            return false;
        }

        // update the total repair size
        repairSize += srf.chunks[0].size * numFailed;
        unsetCopyFileStripeMeta(srf);
    }

    // TAGPT (end): data repair
    if (bmRepair)
        bmRepair->dataRepair.markEnd();

    // report data repair speed
    boost::timer::cpu_times duration = mytimer.elapsed();
    LOG_IF(INFO, duration.wall) << "Repair file " << f.name << ", (data) speed = " << (repairSize * 1.0 / (1 << 20)) / (duration.wall * 1.0 / 1e9) << " MB/s "
            << "(" << repairSize * 1.0 / (1 << 20) << "MB in " << (duration.wall * 1.0 / 1e9) << " seconds)";

    // report metadata update time
    mytimer.start();

    // TAGPT (start): update meta
    if (bmRepair)
        bmRepair->updateMeta.setStart(bmRepair->dataRepair.getEnd());

    rf.genUUID();
    // update metadata
    if (_metastore->putMeta(rf) == false) {
        LOG(ERROR) << "Failed to update file metadata after repair for file " << f.name;
        unlockFile(rf);
        return false;
    }
    LOG(INFO) << "Repair file " << f.name << ", (meta, update) duration = " << mytimer.elapsed().wall * 1.0 / 1e6 << " milliseconds";

    // TAGPT (end): update meta
    if (bmRepair) {
        bmRepair->updateMeta.markEnd();
        bmRepair->proxyOverallTime.setEnd(bmRepair->updateMeta.getEnd());
        auto tvMap = bmRepair->calcStats();
        bmRepair->printStats(tvMap);
    }

    // remove the journaled chunk record for repaired chunks
    if (_metastore && _metastore->fileHasJournal(f)) {
        for (const int chunkId : chunksToCheckForJournal) {
            // remove the chunk write journal record
            int containerId = f.containerIds[chunkId];
            if (
                containerId != INVALID_CONTAINER_ID 
                && !_metastore->updateChunkInJournal(f, f.chunks[chunkId], /* isWrite */ true, /* deleteRecord */ true, containerId)
            ) {
                LOG(WARNING) << "Failed to remove journal record of chunk " << chunkId << " of file " << f.name << " in namepsace " << f.namespaceId << " version " << f.version << ".";
            }
        }
    }
    
    LOG(INFO) << "Repair file " << f.name << ", completes";

    unlockFile(rf);
    return true;
}

unsigned long int Proxy::getFileSize(File &f, bool copyMeta) {
    File rf;

    if (rf.copyNameAndSize(f) == false) {
        LOG(ERROR) << "Failed to copy file metadata for get file size operaiton";
        return false;
    }
    rf.copyVersionControlInfo(f);

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        rf.namespaceId = DEFAULT_NAMESPACE_ID;

    // get file metadata
    if (_metastore->getMeta(rf, /* get blocks types (none) */ 0) == false) {
        LOG(WARNING) << "Failed to find file metadata for file " << f.name;
        return INVALID_FILE_LENGTH;
    }

    // copy metadata if needed
    if (copyMeta)
        f.copyAllMeta(rf);

    // if staged file is more recent, return the staged size; otherwise, return file size
    return rf.staged.size > 0 && rf.staged.mtime > rf.mtime? rf.staged.size : rf.size;
}

unsigned long int Proxy::getExpectedAppendSize(std::string storageClass) {
    return _chunkManager->getMaxDataSizePerStripe(storageClass);
}

unsigned long int Proxy::getExpectedAppendSize(const File &f) {
    return _chunkManager->getMaxDataSizePerStripe(f.codingMeta.coding, f.codingMeta.n, f.codingMeta.k, f.codingMeta.maxChunkSize);
}

unsigned long int Proxy::getExpectedAppendSize(int codingScheme, int n, int k, int maxChunkSize) {
    return _chunkManager->getMaxDataSizePerStripe(codingScheme, n, k, maxChunkSize);
}

unsigned long int Proxy::getExpectedReadSize(boost::uuids::uuid fuuid, File &f) {
    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    // get the file name using fid
    if (!_metastore->getFileName(fuuid, f))
        return 0;

    return getExpectedReadSize(f);
}

unsigned long int Proxy::getExpectedReadSize(File &f) {
    File rf;
    boost::timer::cpu_timer mytimer;

    if (f.namespaceId == INVALID_NAMESPACE_ID)
        f.namespaceId = DEFAULT_NAMESPACE_ID;

    if (rf.copyNameAndSize(f)== false) {
        LOG(ERROR) << "Failed to copy file metadata for read operaiton";
        return 0;
    }
    rf.copyVersionControlInfo(f);

    // get file metadata
    if (_metastore->getMeta(rf, /* get blocks type (none) */ 0) == false) {
        LOG(WARNING) << "Failed to find file metadata for file " << f.name;
        return INVALID_FILE_OFFSET;
    }

    // handle staged files
    CodingMeta &cmeta = rf.staged.codingMeta;
    if (rf.staged.size > 0 && rf.staged.mtime >= rf.mtime && cmeta.coding != CodingScheme::UNKNOWN_CODE) {
        return std::min(_chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize), rf.staged.size);
    }

    // handle zero-sized file
    if (rf.size == 0 || rf.numStripes == 0) {
        return 0;
    }

    //int numChunksPerStripe = rf.numChunks / rf.numStripes;
    return _chunkManager->getMaxDataSizePerStripe(rf.codingMeta.coding, rf.codingMeta.n, rf.codingMeta.k, rf.codingMeta.maxChunkSize, /* full chunk size */ true);
}


bool Proxy::copyFileStripeMeta(File &dst, File &src, int stripeId, const char *op) {
    // copy name, file size and timestamps
    if (dst.copyNameAndSize(src) == false) {
        LOG(ERROR) << "Failed to copy metadata for file " << op;
        return false;
    }

    int numChunksPerStripe = src.numChunks / src.numStripes;
    const CodingMeta &cmeta = src.codingMeta;
    //unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, src.chunks[0].size, /* full chunk size */ false);
    unsigned long int maxDataStripeSize = _chunkManager->getMaxDataSizePerStripe(cmeta.coding, cmeta.n, cmeta.k, cmeta.maxChunkSize, /* full chunk size */ true);

    // stripe size
    dst.size = src.size > (stripeId + 1) * maxDataStripeSize? maxDataStripeSize : src.size - stripeId * maxDataStripeSize;
    // stripe (/file) version
    dst.version = src.version;
    dst.numChunks = numChunksPerStripe;
    // stripe chunks and containers
    dst.chunks = src.chunks + stripeId * numChunksPerStripe; 
    dst.containerIds = src.containerIds + stripeId * numChunksPerStripe;
    dst.chunksCorrupted = src.chunksCorrupted + stripeId * numChunksPerStripe;
    // stripe coding metadata
    dst.codingMeta = src.codingMeta;
    dst.codingMeta.codingStateSize /= src.numStripes;
    dst.codingMeta.codingState += stripeId * dst.codingMeta.codingStateSize;

    return true;
}

unsigned int Proxy::getFileList(FileInfo **list, bool withSize, bool withVersions, unsigned char namespaceId, std::string prefix) {
    if (namespaceId == INVALID_NAMESPACE_ID)
        namespaceId = DEFAULT_NAMESPACE_ID;
    return _metastore->getFileList(list, namespaceId, withSize, withSize, withVersions, prefix);
}

unsigned int Proxy::getFolderList(std::vector<std::string> &list, unsigned char namespaceId, std::string prefix) {
    if (namespaceId == INVALID_NAMESPACE_ID)
        namespaceId = DEFAULT_NAMESPACE_ID;
    return _metastore->getFolderList(list, namespaceId, prefix, /* skip subfolders */ true);
}

void Proxy::getFileCountAndLimit(unsigned long int &count, unsigned long int &limit) {
    limit = _metastore->getMaxNumKeysSupported();
    count = _metastore->getNumFiles() + _ongoingRepairCnt;
}

bool Proxy::getNumFilesToRepair(unsigned long int &count, unsigned long &repair) {
    count = _metastore->getNumFiles();
    repair = _metastore->getNumFilesToRepair();
    
    return count >= repair;
}

void Proxy::unsetCopyFileStripeMeta(File &copy) {
    copy.chunks = 0;
    copy.containerIds = 0;
    copy.chunksCorrupted = 0;
    copy.codingMeta.codingState = 0;
}

bool Proxy::lockFile(const File &f) {
    int retryIntv = Config::getInstance().getRetryInterval();
    int numRetry = Config::getInstance().getNumRetry();

    bool locked = false;

    // try locking the file
    for (int j = 0; j < numRetry; j++) {
        locked = _metastore->lockFile(f);
        if (locked)
            break;
        // sleep before retry (avoid error when usleep more than 1e6 us)
        if (retryIntv >= 1e6)
            sleep(retryIntv / 1e6);
        usleep(retryIntv % (int) 1e6);
    }

    return locked;
}

bool Proxy::unlockFile(const File &f) {
    return _metastore->unlockFile(f);
}

bool Proxy::lockFileAndGetMeta(File &f, const char *op) {
    // lock file for overwrite
    if (lockFile(f) == false) {
        LOG(ERROR) << "Failed to lock file " << f.name << " for " << op;
        return false;
    }
    // get the old file metadata for checking
    if (!_metastore->getMeta(f)) {
        LOG(ERROR) << "Failed to find the metadata of file " << f.name << " for " << op;
        unlockFile(f);
        return false;
    }
    return true;
}

bool Proxy::pinStagedFile(const File &f) {
    return _stagingEnabled &&_staging? _staging->pinFile(f) : true;
}

bool Proxy::unpinStagedFile(const File &f) {
    return _stagingEnabled && _staging? _staging->unpinFile(f) : false;
}
