// SPDX-License-Identifier: Apache-2.0

#include <sys/types.h> // open()
#include <sys/stat.h>  // open()
#include <fcntl.h>     // open()
#include <sys/mman.h>  // mmap(), munmap()
#include <string.h>
#include <unistd.h>    // close()

#include "zmq.hh"
#include "../../common/io.hh"
#include "../../common/config.hh"

#include <glog/logging.h>

const char *ProxyZMQIntegration::_workerAddr = "inproc://proxzmqworker";

ProxyZMQIntegration::ProxyZMQIntegration() : ProxyZMQIntegration (0) {
}

ProxyZMQIntegration::ProxyZMQIntegration(Proxy *proxy) {
    // start a proxy instance
    if (proxy == 0)
        _proxy = new Proxy();
    else
        _proxy = proxy;
    _cxt = zmq::context_t(Config::getInstance().getProxyNumZmqThread());
    _numWorkers = Config::getInstance().getProxyZmqNumWorkers();
    pthread_barrier_init(&_stopRunning, NULL, 2);
    _isRunning = false;
    _releaseProxy = proxy == 0;
}

ProxyZMQIntegration::ProxyZMQIntegration(ProxyCoordinator *coordinator, std::map<int, std::string> *map, BgChunkHandler::TaskQueue *queue) {
    _proxy = new Proxy(coordinator, map, queue);
    _coordinator = coordinator;
    _containerToAgentMap = map;
    _queue = queue;
    _cxt = zmq::context_t(Config::getInstance().getProxyNumZmqThread());
    _numWorkers = Config::getInstance().getProxyZmqNumWorkers();
    pthread_barrier_init(&_stopRunning, NULL, 2);
    _isRunning = false;
    _releaseProxy = true;
}

ProxyZMQIntegration::~ProxyZMQIntegration() {
    stop();
    delete _frontend;
    delete _backend;
    if (_releaseProxy)
        delete _proxy;
}

bool ProxyZMQIntegration::stop() {
    LOG(WARNING) << "Terminating Zero-mq interface";
    _isRunning = false;
    // stop listening to requests ends
    _frontend->close();
    _backend->close();
    _cxt.close();
    // wait for the workers to stop first
    for (int i = 0; i < _numWorkers; i++)
        pthread_join(_workers[i], NULL);
    // wait for the running thread to stop
    pthread_barrier_wait(&_stopRunning);
    pthread_barrier_destroy(&_stopRunning);
    LOG(WARNING) << "Terminated Zero-mq interface";
    return true;
}

void *ProxyZMQIntegration::run(void *arg) {
    ProxyZMQIntegration *self = (ProxyZMQIntegration *) arg;
    Config &config = Config::getInstance();

    self->_isRunning = true;
    
    int myProxyNum = config.getMyProxyNum();
    std::string proxyIP = config.listenToAllInterfaces()? "0.0.0.0" : config.getProxyIP(myProxyNum);
    std::string proxyAddr = IO::genAddr(proxyIP, config.getProxyZmqPort());

    // start workers
    for (int i = 0; i < self->_numWorkers; i++)
        pthread_create(&self->_workers[i], NULL, ProxyZMQIntegration::handleRequests, self);

    // accept requests
    self->_frontend = new zmq::socket_t(self->_cxt, ZMQ_ROUTER);
    self->_frontend->bind(proxyAddr);

    // dispatch requests
    self->_backend = new zmq::socket_t(self->_cxt, ZMQ_DEALER);
    self->_backend->bind(_workerAddr);

    // start processing requests
    try {
        zmq::proxy(*self->_frontend, *self->_backend, NULL);
    } catch (std::exception &e) {
        LOG(WARNING) << "Proxy reuqeest dispatcher ended, " << e.what();
    }

    // signal the stop of running status
    pthread_barrier_wait(&self->_stopRunning);

    LOG(WARNING) << "Stop listening on Zero-mq interface";

    return NULL;
}

void *ProxyZMQIntegration::handleRequests(void *arg) {
    ProxyZMQIntegration *self = (ProxyZMQIntegration*) arg;

    unsigned long int traffic = 0;
    
    zmq::socket_t socket(self->_cxt, ZMQ_REP);
    try {
        socket.connect(_workerAddr);
    } catch (zmq::error_t &e) {
        LOG(ERROR) << "Failed to connect to request queue: " << e.what();
        return NULL;
    }

    Proxy *proxy = Config::getInstance().reuseDataConn()? new Proxy(self->_coordinator, self->_containerToAgentMap, self->_queue) : self->_proxy;

    while(self->_isRunning) {
        Request req;
        Reply rep;
        File myfile;
        bool success = false, okay = true;

        try  {
            if (getRequest(socket, req) != 0)
                continue;
        } catch (zmq::error_t &e) {
            LOG_IF(ERROR, self->_isRunning) << "Failed to get request message: " << e.what();
            break;
        }

        switch(req.opcode) {
        case ClientOpcode::WRITE_FILE_REQ:
            DLOG(INFO) << "Get a write file request";
            // name
            myfile.nameLength = req.file.name.size();
            myfile.name = (char *) malloc (myfile.nameLength + 1);
            memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
            myfile.name[myfile.nameLength] = 0;
            // namespace id
            myfile.namespaceId = req.file.namespaceId;
            // size and length
            myfile.size = req.file.size;
            myfile.offset = 0;
            myfile.length = myfile.size;
            // timestamps
            myfile.ctime = 0; // req.file.ctime; // TODO
            // storage class
            myfile.storageClass = req.file.storageClass;
            // data
            if (req.file.isCached) {
                struct stat sbuf;
                // check cache file size, to avoid successful mmap but further access gives rise to SIGBUS (accessing memory out of the program's mapped region)
                if (stat(req.file.cachePath.c_str(), &sbuf) != 0 || (unsigned long int) sbuf.st_size < myfile.offset + myfile.length) {
                    okay = false;
                } else if (req.file.size > 0) {
                    int fd = open(req.file.cachePath.c_str(), O_RDONLY);
                    if (fd == -1) {
                        LOG(ERROR) << "Failed to open cache file";
                        okay = false;
                    } else {
                        myfile.data = (unsigned char *) mmap (NULL, myfile.size, PROT_READ, MAP_PRIVATE, fd, 0);
                        if (myfile.data == MAP_FAILED) {
                            LOG(ERROR) << "Failed to memory map cache file, " << strerror(errno);
                            okay = false;
                        } else {
                            madvise(myfile.data, myfile.size, MADV_SEQUENTIAL);
                            // issue request
                            success = proxy->writeFile(myfile);
                            // unmap data
                            munmap(myfile.data, myfile.size);
                        }
                        // avoid auto free
                        myfile.data = 0;
                        close(fd);
                    }
                } else { // handle zero-size file
                    success = proxy->writeFile(myfile);
                }
            } else {
                myfile.data = req.file.data;
                // issue request
                success = proxy->writeFile(myfile);
            }
            rep.opcode = success && okay? ClientOpcode::WRITE_FILE_REP_SUCCESS : ClientOpcode::WRITE_FILE_REP_FAIL;
            break;

        case ClientOpcode::READ_FILE_REQ:
            DLOG(INFO) << "Get a read file request";
            // name
            myfile.nameLength = req.file.name.size();
            myfile.name = (char *) malloc (myfile.nameLength + 1);
            memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
            myfile.name[myfile.nameLength] = 0;
            myfile.offset = 0;
            // namespace id
            myfile.namespaceId = req.file.namespaceId;
            // data
            if (req.file.isCached) {
                // memory-map needs to know the data size first
                myfile.size = proxy->getFileSize(myfile);
                if (myfile.size == INVALID_FILE_LENGTH) {
                    LOG(WARNING) << "Failed to find the size of file " << myfile.name;
                    okay = false;
                } else {
                    // open file
                    int fd = open(req.file.cachePath.c_str(), O_RDWR | O_CREAT, 0666);
                    if (fd == -1) {
                        LOG(ERROR) << "Failed to open cache file " << req.file.cachePath.c_str() << " for write, err = " << strerror(errno);
                        okay = false;
                    } else {
                        // update file size
                        if (ftruncate(fd, myfile.size) == 0) {
                            // memory-map file
                            myfile.data = (unsigned char *) mmap (NULL, myfile.size, PROT_WRITE, MAP_SHARED, fd, 0);
                            if (myfile.data == MAP_FAILED) {
                                LOG(ERROR) << "Failed to memory map cache file, " << strerror(errno);
                                myfile.data = 0;
                                okay = false;
                            } else {
                                // issue request
                                success = proxy->readFile(myfile);
                                if (success && msync(myfile.data, myfile.size, MS_SYNC)) {
                                    LOG(ERROR) << "Failed to sync cache file after write, " << strerror(errno);
                                    okay = false;
                                }
                            }
                        } else {
                            LOG(ERROR) << "Failed to set cache file size, " << strerror(errno);
                            okay = false;
                        }
                        close(fd);
                    }
                }
            } else {
                success = proxy->readFile(myfile);
            }
            // prepare reply
            rep.opcode = success && okay? ClientOpcode::READ_FILE_REP_SUCCESS : ClientOpcode::READ_FILE_REP_FAIL;
            rep.file.size = myfile.size;
            rep.file.data = myfile.data;
            rep.file.isCached = req.file.isCached;
            rep.file.cachePath = req.file.cachePath;
            // unmap data if needed
            if (req.file.isCached && okay) {
                munmap(myfile.data, myfile.size);
                myfile.data = 0;
            }
            break;

        case DEL_FILE_REQ:
            myfile.nameLength = req.file.name.size();
            myfile.name = (char *) malloc (myfile.nameLength + 1);
            memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
            myfile.name[myfile.nameLength] = 0;
            myfile.namespaceId = req.file.namespaceId;

            success = proxy->deleteFile(myfile);

            rep.opcode = success? ClientOpcode::DEL_FILE_REP_SUCCESS : DEL_FILE_REP_FAIL;
            break;

        case APPEND_FILE_REQ:
        case READ_FILE_RANGE_REQ:
        case OVERWRITE_FILE_REQ:
        case COPY_FILE_REQ:
#define isAppend    (req.opcode == APPEND_FILE_REQ)
#define isOverwrite (req.opcode == OVERWRITE_FILE_REQ)
#define isCopy      (req.opcode == COPY_FILE_REQ)
            DLOG(INFO) << "Get an " << (isAppend? "append" : isOverwrite? "overwrite" : isCopy? "copy" : "ranged read") << " file request";
            // name
            myfile.nameLength = req.file.name.size();
            myfile.name = (char *) malloc (myfile.nameLength + 1);
            memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
            myfile.name[myfile.nameLength] = 0;
            // namespace id
            myfile.namespaceId = req.file.namespaceId;
            // size and length
            myfile.size = req.file.length;
            myfile.offset = req.file.offset;
            myfile.length = myfile.size;
            // data
            if (req.file.isCached && !isCopy) {
                if (req.file.size > 0) {
                    struct stat sbuf;
                    stat(req.file.cachePath.c_str(), &sbuf);
                    if (isAppend && (unsigned long int) sbuf.st_size < myfile.offset + myfile.length) {
                        LOG(ERROR) << "Not enough data in cached file for write, file size = " << sbuf.st_size << " offset = " << myfile.offset << " length = " << myfile.length;
                        okay = false;
                    } else {
                        int fd = 0;
                        if (isAppend)
                            fd = open(req.file.cachePath.c_str(), O_RDONLY);
                        else
                            fd = open(req.file.cachePath.c_str(), O_RDWR | O_CREAT, 0666);
                        if (fd == -1) {
                            LOG(ERROR) << "Failed to open cache file";
                            okay = false;
                        } else {
                            long pageSize = sysconf(_SC_PAGE_SIZE);
                            unsigned long int alignOffset = myfile.offset / pageSize * pageSize;
                            unsigned long int alignSize = myfile.size + (myfile.offset - alignOffset);
                            if (!isAppend && ftruncate(fd, myfile.offset + myfile.size) != 0) {
                                myfile.data = (unsigned char *) MAP_FAILED;
                            } else {
                                myfile.data = (unsigned char *) mmap(NULL, alignSize, (isAppend? PROT_READ : PROT_WRITE), MAP_SHARED, fd, alignOffset);
                            }
                            if (myfile.data == MAP_FAILED) {
                                LOG(ERROR) << "Failed to memory map cache file, " << strerror(errno);
                                okay = false;
                            } else {
                                if (isAppend)
                                    // point the start of data to append
                                    madvise(myfile.data, alignSize, MADV_SEQUENTIAL);
                                myfile.data += (myfile.offset - alignOffset);
                                // issue request
                                success = (isAppend ? proxy->appendFile(myfile) : proxy->readPartialFile(myfile));
                                // unmap data
                                myfile.data -= (myfile.offset - alignOffset);
                                munmap(myfile.data, alignSize);

                                if (!isAppend && ftruncate(fd, myfile.offset + myfile.size) != 0)
                                    LOG(WARNING) << "Failed to truncate excess cache size";
                                else if (!isAppend)
                                    DLOG(INFO) << "Adjust file size to " << myfile.offset + myfile.size;
                            }
                            close(fd);
                        }
                    }
                } else { // zero-sized append, do nothing
                }
            } else if (isAppend) {
                myfile.data = req.file.data;
                // issue request
                success = proxy->appendFile(myfile);
            } else if (isOverwrite) {
                myfile.data = req.file.data;
                success = proxy->overwriteFile(myfile);
            } else if (isCopy) {
                File dstFile;
                dstFile.name = (char*) req.file.cachePath.c_str();
                dstFile.nameLength = req.file.cachePath.length();
                success = proxy->copyFile(myfile, dstFile);
                // return the new file size after copying
                myfile.size = dstFile.size;
                // avoid double free
                dstFile.name = 0;
            } else {
                success = proxy->readPartialFile(myfile);
                rep.file.data = myfile.data;
            }
            if (isAppend)
                rep.opcode = success && okay? ClientOpcode::APPEND_FILE_REP_SUCCESS : ClientOpcode::APPEND_FILE_REP_FAIL;
            else if (isOverwrite)
                rep.opcode = success && okay? ClientOpcode::OVERWRITE_FILE_REP_SUCCESS : ClientOpcode::OVERWRITE_FILE_REP_FAIL;
            else if (isCopy)
                rep.opcode = success && okay? ClientOpcode::COPY_FILE_REP_SUCCESS : ClientOpcode::COPY_FILE_REP_FAIL;
            else
                rep.opcode = success && okay? ClientOpcode::READ_FILE_RANGE_REP_SUCCESS : ClientOpcode::READ_FILE_RANGE_REP_FAIL;

            rep.file.size = myfile.size;
            rep.file.offset = myfile.offset;
            rep.file.isCached = req.file.isCached;
            rep.file.cachePath = req.file.cachePath;
            if (req.file.isCached)
                // avoid double free
                myfile.data = 0;

            break;
#undef isOverwrite
#undef isAppend
#undef isCopy

        case RENAME_FILE_REQ:
            {
                File renameFile;

                myfile.nameLength = req.file.name.size();
                myfile.name = (char *) malloc (myfile.nameLength + 1);
                memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
                myfile.name[myfile.nameLength] = 0;
                myfile.namespaceId = req.file.namespaceId;
                
                renameFile.nameLength = req.file.cachePath.size();
                renameFile.name = (char *) malloc (renameFile.nameLength + 1);
                memcpy(renameFile.name, req.file.cachePath.c_str(), renameFile.nameLength);
                renameFile.name[renameFile.nameLength] = 0;

                success = proxy->renameFile(myfile, renameFile);
                rep.opcode = success? ClientOpcode::RENAME_FILE_REP_SUCCESS : ClientOpcode::RENAME_FILE_REP_FAIL;
            }
            break;

        case GET_CAPACITY_REQ:
            proxy->getStorageUsage(rep.stats.usage, rep.stats.capacity);
            proxy->getFileCountAndLimit(rep.stats.fileCount, rep.stats.fileLimit);
            rep.opcode = ClientOpcode::GET_CAPACITY_REP_SUCCESS;
            break;

        case GET_FILE_LIST_REQ:
            rep.list.numFiles = proxy->getFileList(&rep.list.fileInfo, /* withSize */ true, /* withVersions */ false, req.file.namespaceId, req.file.name);
            rep.opcode = ClientOpcode::GET_FILE_LIST_REP_SUCCESS;
            break;

        case GET_APPEND_SIZE_REQ:
            {
            rep.file.length = proxy->getExpectedAppendSize(req.file.storageClass);
            rep.opcode = ClientOpcode::GET_APPEND_SIZE_REP_SUCCESS;
            }
            break;

        case GET_READ_SIZE_REQ:
            // name
            myfile.nameLength = req.file.name.size();
            myfile.name = (char *) malloc (myfile.nameLength + 1);
            memcpy(myfile.name, req.file.name.c_str(), myfile.nameLength);
            myfile.name[myfile.nameLength] = 0;
            // namespace id
            myfile.namespaceId = req.file.namespaceId;

            rep.file.length = proxy->getExpectedReadSize(myfile);
            rep.opcode = rep.file.length > 0? ClientOpcode::GET_READ_SIZE_REP_SUCCESS : ClientOpcode::GET_READ_SIZE_REP_FAIL;
            break;

        case GET_AGENT_STATUS_REQ:
            rep.list.numAgents = proxy->getAgentStatus(&rep.list.agentInfo);
            rep.opcode = ClientOpcode::GET_AGENT_STATUS_REP_SUCCESS;
            break;

        case GET_BG_TASK_PRG_REQ:
            rep.list.bgTasks.num = proxy->getBackgroundTaskProgress(rep.list.bgTasks.name, rep.list.bgTasks.progress);
            rep.opcode = ClientOpcode::GET_BG_TASK_PRG_REP_SUCCESS;
            break;

        case GET_REPAIR_STATS_REQ:
            proxy->getNumFilesToRepair(rep.stats.fileCount, rep.stats.repairCount);
            rep.opcode = ClientOpcode::GET_REPAIR_STATS_REP_SUCCESS;
            break;

        case GET_PROXY_STATUS_REQ:
            proxy->getProxyStatus(rep.proxyStatus);
            rep.opcode = ClientOpcode::GET_PROXY_STATUS_REP_SUCCESS;

        default:
            // unknown request ..
            break;
        }

        try {
            // send reply
            sendReply(socket, rep);
            DLOG(INFO) << "Reply to client, op = " << rep.opcode;
        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to send a reply, " << e.what();
        }
    }

    socket.close();

    if (Config::getInstance().reuseDataConn())
        delete proxy;

    LOG(INFO) << "Request handled with total traffic = " << traffic << "B";

    return NULL;
}

int ProxyZMQIntegration::getRequest(zmq::socket_t &socket, Request &req) {
    zmq::message_t msg;

#define getNextMsg( ) do { \
    msg.rebuild(); \
    if (!socket.recv(&msg)) \
        return EAGAIN; \
} while(0);

    // get op code
    getNextMsg();
    req.opcode = *((int *) msg.data());
    DLOG(INFO) << "Opcode = " << req.opcode;

    if (hasOpcodeOnly(req.opcode))
        return 0;

    // get namespace id
    getNextMsg();
    req.file.namespaceId = *((unsigned char *) msg.data());
    DLOG(INFO) << "Namespace Id = " << (int) req.file.namespaceId;

    if (hasNamespaceIdOnly(req.opcode))
        return 0;

    if (req.opcode == GET_APPEND_SIZE_REQ) {
        if (!msg.more()) return 1;
        getNextMsg();
        req.file.storageClass = std::string((char *) msg.data(), msg.size());
        DLOG(INFO) << "Storage class = " << req.file.storageClass;
        if (req.file.storageClass.empty())
            req.file.storageClass = Config::getInstance().getDefaultStorageClass();
        return 0;
    }

    // get file name
    if (!msg.more()) return 1;
    getNextMsg();
    req.file.name = std::string((char *) msg.data(), msg.size());
    DLOG(INFO) << "Name = " << req.file.name;

    if (req.opcode == GET_READ_SIZE_REQ || req.opcode == GET_FILE_LIST_REQ)
        return 0;
    
    if (hasFileSize(req.opcode)) {
        // get file size
        if (!msg.more()) return 1;
        getNextMsg();
        req.file.size = *((unsigned long int *) msg.data());
        DLOG(INFO) << "Size = " << req.file.size;
    }
    if (req.opcode == WRITE_FILE_REQ) {
        // get file storage class 
        if (!msg.more()) return 1;
        getNextMsg();
        req.file.storageClass = std::string((char *) msg.data(), msg.size());
        DLOG(INFO) << "Storage class = " << req.file.storageClass;
        if (req.file.storageClass.empty())
            req.file.storageClass = Config::getInstance().getDefaultStorageClass();
    } else if (hasFileOffset(req.opcode)) {
        // get file offset 
        if (!msg.more()) return 1;
        getNextMsg();
        req.file.offset = *((unsigned long int *) msg.data());
        DLOG(INFO) << "Offset = " << req.file.offset;
    }

    // check if the file is or needs to be cached locally (instead being sending over the wire)
    if (!msg.more()) return 1;
    getNextMsg();
    req.file.isCached = ((char *) msg.data())[0];
    DLOG(INFO) << "isCached = " << req.file.isCached;

    // figure out where the file content is (or should be) cached
    if (req.file.isCached) {
        // file name if cached or to be cached
        if (!msg.more()) return 1;
        getNextMsg();
        req.file.cachePath = std::string((char *) msg.data(), msg.size());
        DLOG(INFO) << (req.opcode == COPY_FILE_REQ? "Destination name" : "Cache name = ") << req.file.cachePath;
    } else if (hasFileData(req.opcode)) {
        // get file content if necessary without cache
        unsigned long int rb = 0;
        req.file.data = (unsigned char*) malloc (req.file.size + 1);
        while(rb < req.file.size) {
            if (!msg.more()) return 1;
            getNextMsg();
            memcpy(req.file.data + rb, msg.data(), msg.size());
            rb += msg.size();
        }
        DLOG(INFO) << "Data (" << rb << ")";
    }


#undef getNextMsg

    return 0;
}

bool ProxyZMQIntegration::sendReply(zmq::socket_t &socket, Reply &rep) {
    // opcode
    size_t msgLength = sizeof(rep.opcode);
    if (socket.send(&rep.opcode, msgLength, replyOpcodeOnly(rep.opcode)? 0 : ZMQ_SNDMORE) != msgLength) {
        LOG(ERROR) << "Failed to send reply opcode";
        return false;
    }

    if (replyFileData(rep.opcode)) {
        if (rep.opcode == READ_FILE_RANGE_REP_SUCCESS) {
            msgLength = sizeof(rep.file.offset);
            if (socket.send(&rep.file.offset, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file offset on reply";
                return false;
            }
            DLOG(INFO) << "file offset = " << rep.file.offset;
        }

        // file size
        msgLength = sizeof(rep.file.size);
        if (socket.send(&rep.file.size, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send file size on reply";
            return false;
        }
        DLOG(INFO) << "file size = " << rep.file.size;

        // is cached file
        msgLength = 1;
        unsigned char cached = rep.file.isCached;
        DLOG(INFO) << "is Cached = " << (int) cached;
        if (socket.send(&cached, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send is file cached on reply";
            return false;
        }

        if (rep.file.isCached) {
            // cached file name
            msgLength = rep.file.cachePath.size();
            if (socket.send(rep.file.cachePath.c_str(), msgLength, 0) != msgLength) {
                LOG(ERROR) << "Failed to send cached file path on reply";
                return false;
            }
        } else {
            // file data
            msgLength = rep.file.size;
            if (socket.send(rep.file.data, msgLength, 0) != msgLength) {
                LOG(ERROR) << "Failed to send file data on reply";
                return false;
            }
        }
    } else if (replyStats(rep.opcode)) {
        msgLength = sizeof(rep.stats.usage);
        if (socket.send(&rep.stats.usage, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send storage usage on reply";
            return false;
        }
        DLOG(INFO) << "usage = " << rep.stats.usage;
        msgLength = sizeof(rep.stats.capacity);
        if (socket.send(&rep.stats.capacity, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send storage capacity on reply";
            return false;
        }
        DLOG(INFO) << "capacity = " << rep.stats.capacity;
        msgLength = sizeof(rep.stats.fileCount);
        if (socket.send(&rep.stats.fileCount, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send file count on reply";
            return false;
        }
        DLOG(INFO) << "file count = " << rep.stats.fileCount;
        msgLength = sizeof(rep.stats.fileLimit);
        if (socket.send(&rep.stats.fileLimit, msgLength, 0) != msgLength) {
            LOG(ERROR) << "Failed to send file limit on reply";
            return false;
        }
        DLOG(INFO) << "file limit = " << rep.stats.fileLimit;
    } else if (replyFileList(rep.opcode)) {
        // file list count
        msgLength = sizeof(rep.list.numFiles);
        if (socket.send(&rep.list.numFiles, msgLength, rep.list.numFiles > 0? ZMQ_SNDMORE : 0) != msgLength) {
            LOG(ERROR) << "Failed to send file list count on reply";
            return false;
        }
        DLOG(INFO) << "num files = " << rep.list.numFiles;
        for (unsigned int i = 0; i < rep.list.numFiles; i++) {
            bool isLast = i + 1 == rep.list.numFiles;
            // file name
            msgLength = rep.list.fileInfo[i].nameLength;
            if (socket.send(rep.list.fileInfo[i].name, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file name on list (" << i <<  ") on reply";
                return false;
            }
            // file size
            msgLength = sizeof(rep.list.fileInfo[i].size);
            if (socket.send(&rep.list.fileInfo[i].size, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file size on list (" << i <<  ") on reply";
                return false;
            }
            // file creation time
            msgLength = sizeof(rep.list.fileInfo[i].ctime);
            if (socket.send(&rep.list.fileInfo[i].ctime, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file creation time on list (" << i <<  ") on reply";
                return false;
            }
            // file last access time
            msgLength = sizeof(rep.list.fileInfo[i].atime);
            if (socket.send(&rep.list.fileInfo[i].atime, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file last access time on list (" << i <<  ") on reply";
                return false;
            }
            // file last modified time
            msgLength = sizeof(rep.list.fileInfo[i].mtime);
            if (socket.send(&rep.list.fileInfo[i].mtime, msgLength, isLast? 0 : ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send file last modified time on list (" << i <<  ") on reply";
                return false;
            }
            DLOG(INFO) << "file " << i << " name = " << rep.list.fileInfo[i].name << " size = " << rep.list.fileInfo[i].size << " {c,a,m}times (" << rep.list.fileInfo[i].ctime << "," << rep.list.fileInfo[i].atime << "," << rep.list.fileInfo[i].mtime << ")";
        }
    } else if (rep.opcode == GET_APPEND_SIZE_REP_SUCCESS || rep.opcode == GET_READ_SIZE_REP_SUCCESS) {
        // append length 
        msgLength = sizeof(rep.file.length);
        if (socket.send(&rep.file.length, msgLength, 0) != msgLength) {
            LOG(ERROR) << "Failed to send append size on reply";
            return false;
        }
    } else if (rep.opcode == APPEND_FILE_REP_SUCCESS || rep.opcode == OVERWRITE_FILE_REP_SUCCESS) {
        msgLength = sizeof(rep.file.size);
        if (socket.send(&rep.file.size, msgLength, 0) != msgLength) {
            LOG(ERROR) << "Failed to send append size on reply";
            return false;
        }
        if (rep.opcode == APPEND_FILE_REP_SUCCESS)
            DLOG(INFO) << "file size after append = " <<  rep.file.size;
        else
            DLOG(INFO) << "last touch position by overwrite = " << rep.file.size;
    } else if (rep.opcode == GET_AGENT_STATUS_REP_SUCCESS) {
        // number of agents
        msgLength = sizeof(rep.list.numAgents);
        if (socket.send(&rep.list.numAgents, msgLength, rep.list.numAgents > 0? ZMQ_SNDMORE : 0) != msgLength) {
            LOG(ERROR) << "Failed to send number of agents on reply";
            return false;
        }
        DLOG(INFO) << "number of agents = " <<  rep.list.numAgents;
        for (unsigned int i = 0; i < rep.list.numAgents; i++) {
            // liveness
            msgLength = 1;
            char alive = rep.list.agentInfo[i].alive? 1 : 0;
            if (socket.send(&alive, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send alive status for agent " << i + 1 << " on reply";
                return false;
            }
            DLOG(INFO) << "agents liveness = " << (int) alive;
            msgLength = strlen(rep.list.agentInfo[i].addr);
            if (socket.send(rep.list.agentInfo[i].addr, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send address of agent " << i + 1 << " on reply";
                return false;
            }
            DLOG(INFO) << "agents address = " << rep.list.agentInfo[i].addr;
            msgLength = sizeof(rep.list.agentInfo[i].hostType);
            if (socket.send(&rep.list.agentInfo[i].hostType, msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send address of agent " << i + 1 << " on reply";
                return false;
            }
            DLOG(INFO) << "agents hostType = " << (int) rep.list.agentInfo[i].hostType;

#define SEND_SYS_INFO(_SYS_INFO_, _END_) \
    do { \
        /* sysinfo for cpu */ \
        msgLength = sizeof(_SYS_INFO_.cpu.num); \
        if (socket.send(&_SYS_INFO_.cpu.num, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send number of cpus on reply"; \
            return false; \
        } \
        msgLength = _SYS_INFO_.cpu.num * sizeof(float); \
        if (socket.send(_SYS_INFO_.cpu.usage, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send cpu usage on reply"; \
            return false; \
        } \
        /* sysinfo for memory */ \
        msgLength = sizeof(_SYS_INFO_.mem.total); \
        if (socket.send(&_SYS_INFO_.mem.total, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send memory size on reply"; \
            return false; \
        } \
        msgLength = sizeof(_SYS_INFO_.mem.free); \
        if (socket.send(&_SYS_INFO_.mem.free, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send free memory size on reply"; \
            return false; \
        } \
        /* sysinfo for network */ \
        msgLength = sizeof(_SYS_INFO_.net.in); \
        if (socket.send(&_SYS_INFO_.net.in, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send ingress traffic on reply"; \
            return false; \
        } \
        msgLength = sizeof(_SYS_INFO_.net.out); \
        if (socket.send(&_SYS_INFO_.net.out, msgLength, ZMQ_SNDMORE) != msgLength) { \
            LOG(ERROR) << "Failed to send egress traffic on reply"; \
            return false; \
        } \
        /* sysinfo for host type */ \
        msgLength = sizeof(_SYS_INFO_.hostType); \
        if (socket.send(&_SYS_INFO_.hostType, msgLength, _END_) != msgLength) { \
            LOG(ERROR) << "Failed to send host type on reply"; \
            return false; \
        } \
    } while (0) 

            SEND_SYS_INFO(rep.list.agentInfo[i].sysinfo, ZMQ_SNDMORE);

            // number of containers
            msgLength = sizeof(rep.list.agentInfo[i].numContainers);
            bool isLastMsg = rep.list.agentInfo[i].numContainers <= 0 && rep.list.numAgents == i + 1;
            if (socket.send(&rep.list.agentInfo[i].numContainers, msgLength, isLastMsg? 0 : ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send number of containers for agent " << i + 1 << " on reply";
                return false;
            }
            DLOG(INFO) << "number of contianers = " <<  rep.list.agentInfo[i].numContainers;
            if (rep.list.agentInfo[i].numContainers > 0) {
                // container ids
                msgLength = sizeof(int) * rep.list.agentInfo[i].numContainers;
                if (socket.send(rep.list.agentInfo[i].containerIds, msgLength, ZMQ_SNDMORE) != msgLength) {
                    LOG(ERROR) << "Failed to send container ids for agent " << i + 1 << " on reply";
                    return false;
                }
                // container type
                msgLength = sizeof(unsigned char) * rep.list.agentInfo[i].numContainers;
                if (socket.send(rep.list.agentInfo[i].containerType, msgLength, ZMQ_SNDMORE) != msgLength) {
                    LOG(ERROR) << "Failed to send container type for agent " << i + 1 << " on reply";
                    return false;
                }
                // container usage
                msgLength = sizeof(unsigned long int) * rep.list.agentInfo[i].numContainers;
                if (socket.send(rep.list.agentInfo[i].containerUsage, msgLength, ZMQ_SNDMORE) != msgLength) {
                    LOG(ERROR) << "Failed to send container usage for agent " << i + 1 << " on reply";
                    return false;
                }
                isLastMsg = rep.list.numAgents == i + 1;
                // container capacity 
                msgLength = sizeof(unsigned long int) * rep.list.agentInfo[i].numContainers;
                if (socket.send(rep.list.agentInfo[i].containerCapacity, msgLength, isLastMsg? 0 : ZMQ_SNDMORE) != msgLength) {
                    LOG(ERROR) << "Failed to send container capacity for agent " << i + 1 << " on reply";
                    return false;
                }
            }
        }
    } else if (rep.opcode == GET_PROXY_STATUS_REP_SUCCESS) {
        SEND_SYS_INFO(rep.proxyStatus, 0);
    } else if (rep.opcode == GET_BG_TASK_PRG_REP_SUCCESS) {
        // number of task
        msgLength = sizeof(rep.list.bgTasks.num);
        if (socket.send(&rep.list.bgTasks.num, msgLength, rep.list.bgTasks.num == 0? 0 : ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send number of tasks on reply";
            return false;
        }
        for (int i = 0; i < rep.list.bgTasks.num; i++) {
            // name
            msgLength = rep.list.bgTasks.name[i].length();
            if (socket.send(rep.list.bgTasks.name[i].c_str(), msgLength, ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send name of tasks " << i << " on reply";
                return false;
            }
            // progress
            msgLength = sizeof(rep.list.bgTasks.progress[i]);
            if (socket.send(&rep.list.bgTasks.progress[i], msgLength, i + 1 == rep.list.bgTasks.num? 0 : ZMQ_SNDMORE) != msgLength) {
                LOG(ERROR) << "Failed to send progress of tasks " << i << " on reply";
                return false;
            }
        }
    } else if (rep.opcode == GET_REPAIR_STATS_REP_SUCCESS) {
        msgLength = sizeof(rep.stats.fileCount);
        if (socket.send(&rep.stats.fileCount, msgLength, ZMQ_SNDMORE) != msgLength) {
            LOG(ERROR) << "Failed to send file count on reply";
            return false;
        }
        DLOG(INFO) << "file count = " << rep.stats.fileCount;
        msgLength = sizeof(rep.stats.repairCount);
        if (socket.send(&rep.stats.repairCount, msgLength, 0) != msgLength) {
            LOG(ERROR) << "Failed to send repair count on reply";
            return false;
        }
        DLOG(INFO) << "repair count = " << rep.stats.repairCount;
    }

    return true;
}
