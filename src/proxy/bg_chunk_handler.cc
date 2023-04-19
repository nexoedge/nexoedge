// SPDX-License-Identifier: Apache-2.0

#include <chrono>

#include <glog/logging.h>

#include "bg_chunk_handler.hh"

BgChunkHandler::BgChunkHandler(ProxyIO *io, MetaStore *metastore, bool *running, TaskQueue *queue) {
    _running = running;
    _io = io;
    _metastore = metastore;
    _numWorkers = Config::getInstance().getProxyNumBgChunkWorker();
    _workers = new pthread_t[_numWorkers + 1];
    _queue = queue? queue : new TaskQueue();
    _freeQueue = (queue == 0);
    // init workers
    for (int i = 0; i < _numWorkers; i++)
        pthread_create(&_workers[i], 0, runWorker, this);
}

BgChunkHandler::~BgChunkHandler() {
    LOG(WARNING) << "Terminating background task manager";
    // join the threads
    for (int i = 0; i < _numWorkers; i++)
        pthread_join(_workers[i], 0);
    delete [] _workers;
    if (_freeQueue)
        delete _queue;
    LOG(WARNING) << "Terminated background task manager";
}

bool BgChunkHandler::addChunkTask(ChunkTask task) {
    // lock and (1) push task, (2) increment the task count for the file
    _queue->lock.lock();
    _queue->tasks.push(task);
    std::string name = genFileKey(*task.file);
    auto it = _queue->fileTaskCount.find(name);
    if (it != _queue->fileTaskCount.end())
        it->second++;
    else
        _queue->fileTaskCount.insert(std::pair<std::string, int>(name, 1));
    _queue->lock.unlock();
    // let the worker know about the task
    _queue->newTask.notify_one();
    // update file status
    _metastore->updateFileStatus(*task.file);
    return true;
}

void* BgChunkHandler::runWorker(void *arg) {
    BgChunkHandler *self = (BgChunkHandler *) arg;
    std::unique_lock<std::mutex> lk (self->_queue->lock);
    // stop when proxy shuts down and there is no more pending tasks
    while (*self->_running || !self->_queue->tasks.empty()) {
        // wait for a task if queue is empty
        if (self->_queue->tasks.size() == 0) {
            auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(2);
            self->_queue->newTask.wait_until(lk, deadline);
        }
        // if no task in queue (return due to timeout), do nothing and wait again
        if (self->_queue->tasks.empty())
            continue;
        // get the chunk task
        ChunkTask task = self->_queue->tasks.front();
        self->_queue->tasks.pop();
        // no longer modifying the queue, unlock to allow task adding
        lk.unlock();
        int startIdx = task.numReqs - task.numBgReqs;
        File bgFile;
        bgFile.copyNameAndSize(*task.file);
        bgFile.containerIds = new int[task.events[startIdx].numChunks * task.numBgReqs];
        bgFile.chunks = new Chunk[task.events[startIdx].numChunks * task.numBgReqs];
        bool taskCompleted = false;
        bool bgwrite = Config::getInstance().writeRedundancyInBackground();  
        std::string error;
        switch(task.op) {
        case PUT_CHUNK_REQ:
            if (bgwrite) { // TODO handle file deletion?
                // check version is updated; if so, skip writting the old chunks
                File cfile;
                cfile.copyNameAndSize(*task.file);
                self->_metastore->getMeta(cfile);
                // skip task
                if (cfile.version > task.file->version) {
                    error = "Skip task: the version of file ";
                    error.append(cfile.name);
                    error.append(" is too old (");
                    error.append(std::to_string(cfile.version));
                    error.append(" vs ");
                    error.append(std::to_string(task.file->version));
                    error.append(")");
                    break;
                }
                // issue the request
                for (int i = startIdx; i < task.numReqs; i++) {
                    task.meta[i].io = self->_io;
                    pthread_create(&task.wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &task.meta[i]);
                }
            }
            // check the status of requests
            for (int i = startIdx; i < task.numReqs; i++) {
                void *ptr = 0;
                bool okay = true;
                pthread_join(task.wt[i], &ptr);
                if (ptr != 0) {
                    LOG(ERROR) << "Failed to store chunk " << i << " due to internal failure, container id = " << task.meta[i].containerId << ", " << ptr;
                    okay = false;
                }
                // mark the location of written chunks
                int numChunksPerNode = task.events[i].numChunks;
                for (int j = 0; j < numChunksPerNode; j++) {
                    // mark chunks as failed
                    if (okay == false || task.meta[i].reply->opcode != Opcode::PUT_CHUNK_REP_SUCCESS) {
                        bgFile.chunks[bgFile.numChunks] = task.file->chunks[i + j * numChunksPerNode];
                        bgFile.containerIds[bgFile.numChunks] = INVALID_CONTAINER_ID;
                        bgFile.numChunks += 1;
                        LOG(ERROR) << "Failed to put chunk id = " << i << " due to failure at agent for container id = " << (task.file? task.file->containerIds[i + j * numChunksPerNode] : -1);
                    } else {
                        LOG(INFO) << "Write chunk of size " << task.file->chunks[i + j * numChunksPerNode].size << " in background";
                        LOG(INFO) << "Write file " << task.file->name << " in background, finish " << ((i - startIdx) * numChunksPerNode + j - bgFile.numChunks + 1) * 100.0 / (task.numBgReqs * numChunksPerNode) << "% " << "background requests";
                    }
                }
            }
            if (bgwrite) { // TODO handle file deletion?
                // check version is updated; if so, remove the just written chunks right the way
                File cfile;
                cfile.copyNameAndSize(*task.file);
                self->_metastore->getMeta(cfile);
                if (cfile.version > task.file->version) {
                    for (int i = startIdx; i < task.numReqs ; i++) {
                        task.meta[i].request->opcode = Opcode::DEL_CHUNK_REQ;
                        pthread_create(&task.wt[i], NULL, ProxyIO::sendChunkRequestToAgent, &task.meta[i]);
                        pthread_join(task.wt[i], 0);
                    }
                    error = "Revert task: version of file is too old";
                    break;
                }
            }
            // update the metadata if there is any failures
            if (bgFile.numChunks > 0) {
                int ret = self->_metastore->updateChunks(bgFile, task.file->version);
                taskCompleted = ret == 0;
                if (!taskCompleted) {
                    error = "Failed to update file metadata: ";
                    error = ret == 1? "file version mismatched" : "other errors";
                }
            } else {
                taskCompleted = true;
            }
            break;
        default:
            break;
        }
        // TODO mark down in a task log?
        if (taskCompleted) {
            LOG(INFO) << "Task completed (" << task.numBgReqs << " of " << task.numReqs << " requests) for file " << task.file->name;
        } else {
            LOG(WARNING) << "Task not completed for file " << task.file->name << ", " << error;
        }
        // update file status
        task.file->status = FileStatus::PART_BG_TASK_COMPLETED;
        task.file->tctime = time(NULL);
        self->_metastore->updateFileStatus(*task.file);
        // update file task count
        std::string name;
        name.append(std::to_string(task.file->namespaceId)).append("_").append(task.file->name);
        lk.lock();
        auto it = self->_queue->fileTaskCount.find(name);
        if (it != self->_queue->fileTaskCount.end() && --it->second == 0)
            self->_queue->fileTaskCount.erase(it);
        lk.unlock();
        // clean up
        delete task.file;
        delete [] task.wt;
        delete [] task.meta;
        delete [] task.events;
        // lock before waiting for task again
        lk.lock();
    }
    return 0;
}

bool BgChunkHandler::taskExistsForFile(const File &file) {
    std::lock_guard<std::mutex> lk (_queue->lock);
    std::string name = genFileKey(file);
    return _queue->fileTaskCount.count(name);
}

int BgChunkHandler::getTaskProgress(std::string *&task, int *&progress) {
    std::lock_guard<std::mutex> lk (_queue->lock);
    int numTask = _queue->fileTaskCount.size();
    task = new std::string[numTask];
    progress = new int[numTask];
    int i = 0;
    for (auto it = _queue->fileTaskCount.begin(); it != _queue->fileTaskCount.end(); it++) {
        File file;
        getFileKeyParts(it->first, file.name, file.nameLength, file.namespaceId);
        _metastore->getMeta(file);
        if (file.numStripes > 0) {
            task[i] = std::string(file.name, file.nameLength); 
            progress[i++] = 100 - (it->second * 100.0 / file.numStripes);
        } else {
            numTask--;
        }
        file.name = 0;
    }
    return numTask;
}

std::string BgChunkHandler::genFileKey(const File &file) {
    std::string name;
    name.append(std::to_string(file.namespaceId)).append("_").append(file.name);
    return name;
}

void BgChunkHandler::getFileKeyParts(const std::string &key, char *&name, int &nameLength, unsigned char &namespaceId) {
    name = strchr((char *) key.c_str(), '_') + 1;
    nameLength = key.size() - (name - key.c_str());
    namespaceId = atoi(key.c_str());
}
