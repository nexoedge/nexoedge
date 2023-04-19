// SPDX-License-Identifier: Apache-2.0

#include <pthread.h>

#include <boost/timer/timer.hpp>
#include <glog/logging.h>
#include <zmq.hpp>

#include "agent.hh"
#include "../common/config.hh"
#include "../common/io.hh"
#include "../common/coding/coding_util.hh"
#include "../common/util.hh"

Agent::Agent() {
    _cxt = zmq::context_t(1);
    _io = new AgentIO(&_cxt);
    _numWorkers = Config::getInstance().getAgentNumWorkers();
    _containerManager = new ContainerManager();
    _coordinator = new AgentCoordinator(_containerManager);
    pthread_mutex_init(&_stats.lock, NULL);

    // init statistics
    _stats.traffic = {0, 0};
    _stats.chunk = {0, 0};
    _stats.ops = {0, 0};
}

Agent::~Agent() {
    LOG(WARNING) << "Terminating Agent ...";

    // stop the proxy for delivering chunk events (so the workers will stop)
    delete _io;
    _cxt.close();

    // join worker threads
    for (int i = 0; i < _numWorkers; i++)
        pthread_join(_workers[i], NULL);

    // wait the workers to end working with the coordinator and container manager
    delete _coordinator;
    delete _containerManager;

    LOG(WARNING) << "Terminated Agent";
}

void Agent::run(bool reg) {
    // register itself to proxies
    if (reg && _coordinator->registerToProxy() == false) {
        LOG(ERROR) << "Failed to register to Proxy";
        return;
    }

    // run chunk event handling workers
    for (int i = 0; i < _numWorkers; i++)
        pthread_create(&_workers[i], NULL, handleChunkEvent, (void *) this);

    // listen to incoming requests
    _io->run(_workerAddr);
}

void *Agent::handleChunkEvent(void *arg) {
    // get benchmark instance
    // Benchmark &bm = Benchmark::getInstance();

    Agent *self = (Agent*) arg;
    
    // connect to the worker proxy socket
    zmq::socket_t socket(self->_cxt, ZMQ_REP);
    Util::setSocketOptions(&socket);
    try {
        socket.connect(self->_workerAddr);
    } catch (zmq::error_t &e) {
        LOG(ERROR) << "Failed to connect to event queue: " << e.what();
        return NULL;
    }

    // start processing events distributed by the worker proxy
    while(true) {
        ChunkEvent event;
        unsigned long int traffic = 0;

        boost::timer::cpu_timer mytimer;

        // TAGPT(start): agent listening to chunk event message
        TagPt tagPt_getCnkEvMsg;
        TagPt tagPt_agentProcess;
        TagPt tagPt_rep2Pxy;

        tagPt_getCnkEvMsg.markStart();

        // get next event
        try {
            // get message and translate the message back into an event
            traffic = IO::getChunkEventMessage(socket, event);
            self->addIngressTraffic(traffic);
        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to get chunk event message: " << e.what();
            break;
        }

        // TAGPT(end): agent listening to chunk event message
        tagPt_getCnkEvMsg.markEnd();

        //DLOG(INFO) << "Get a chunk event message in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
        mytimer.start();

        traffic = 0;
        // process the event
        switch(event.opcode) {
        case Opcode::PUT_CHUNK_REQ:
            // TAGPT(start): agent put chunk
            tagPt_agentProcess.markStart();

            if (event.containerIds == NULL) {
                LOG(ERROR) << "[PUT_CHUNK_REQ] Failed to allocate memory for container ids";
                exit(1);
            }

            // Now event.chunks[i].p2a (startTv, endTv) are marked with valid time

            if (self->_containerManager->putChunks(event.containerIds, event.chunks, event.numChunks) == true) {            
                event.opcode = Opcode::PUT_CHUNK_REP_SUCCESS;
                self->incrementOp();

                // TAGPT(end): agent put chunk
                tagPt_agentProcess.markEnd();

                // LOG(INFO) << std::fixed << std::setprecision(6)
                // << tagPt_agentProcess.startTv.sec() << ", " << tagPt_agentProcess.endTv.sec();

                 LOG(INFO) << "Put " << event.numChunks << " chunks into containers speed = " 
                           << event.numChunks * event.chunks[0].size * 1.0 / (1 << 20) / (mytimer.elapsed().wall * 1.0 / 1e9) 
                           << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
            } else {
                event.opcode = Opcode::PUT_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to put " << event.numChunks << " chunks into containers";
                self->incrementOp(false);
            }
            for (int i = 0; i < event.numChunks; i++) {
                traffic += event.chunks[i].size;
            }
            self->addIngressChunkTraffic(traffic);

            break;

        case Opcode::GET_CHUNK_REQ:
            // TAGPT(start): agent get required chunks from container
            tagPt_agentProcess.markStart();

            if (self->_containerManager->getChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                // TAGPT(end): agent listening to chunk event message
                tagPt_agentProcess.markEnd();
                int totalChunkSize = 0;

                for (int i = 0; i < event.numChunks; i++) {
                    totalChunkSize += event.chunks[i].size;
                }

                event.opcode = Opcode::GET_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Get " << event.numChunks << " chunks from containers speed = " 
                          << event.numChunks * event.chunks[0].size * 1.0 / (1 << 20) / (mytimer.elapsed().wall * 1.0 / 1e9) 
                          << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";

                for (int i = 0; i < event.numChunks; i++) {
                    traffic += event.chunks[i].size;
                }

                self->addEgressChunkTraffic(traffic);
                self->incrementOp();
            } else {
                event.opcode = Opcode::GET_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to get " << event.numChunks << " chunks from containers";
                self->incrementOp(false);
            }
            break;

        case Opcode::DEL_CHUNK_REQ:
            // TAGPT(start): agent del chunk
            tagPt_agentProcess.markStart();

            if (self->_containerManager->deleteChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                event.opcode = Opcode::DEL_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Delete " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();

                // TAGPT(end): agent del chunk
                tagPt_agentProcess.markEnd();

            } else {
                event.opcode = Opcode::DEL_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to delete " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            break;

        case Opcode::CPY_CHUNK_REQ:
            if (self->_containerManager->copyChunks(event.containerIds, event.chunks, &(event.chunks[event.numChunks]), event.numChunks) == true) {
                event.opcode = Opcode::CPY_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Copy " << event.numChunks << " chunks in containers speed = " 
                          << event.numChunks * event.chunks[0].size / (mytimer.elapsed().wall * 1.0 / 1e9) 
                          << "MB/s , in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::CPY_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to copy " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            // move the chunk metadata forward for reply
            for (int i = 0; i < event.numChunks; i++)
                event.chunks[i].copyMeta(event.chunks[event.numChunks]);
            break;

        case Opcode::ENC_CHUNK_REQ:
            {
                Chunk *encodedChunk = new Chunk[1];
                if (encodedChunk == NULL) {
                    LOG(ERROR) << "Failed to allocate memory for encoded chunk";
                } else {
                    *encodedChunk = self->_containerManager->getEncodedChunks(event.containerIds, event.chunks, event.numChunks, event.codingMeta.codingState);
                }
                if (encodedChunk && encodedChunk->size > 0) {
                    ChunkEvent temp = event; // let the original event be freed
                    LOG(INFO) << "Encode " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                    event.opcode = Opcode::ENC_CHUNK_REP_SUCCESS;
                    event.numChunks = 1;
                    event.chunks = encodedChunk;     // free the chunk pointer after the event is sent
                    event.chunks[0].freeData = true; // free after the event is sent
                    event.containerIds = 0;
                    event.codingMeta = CodingMeta();
                    self->incrementOp();
                } else {
                    event.opcode = Opcode::ENC_CHUNK_REP_FAIL;
                    LOG(ERROR) << "Failed to encode " << event.numChunks << " chunks in containers";
                    self->incrementOp(false);
                }
                break;
            }

        case Opcode::RPR_CHUNK_REQ:
            // check if coding scheme is valid
            if (event.codingMeta.coding < 0 || event.codingMeta.coding >= CodingScheme::UNKNOWN_CODE) { 
                LOG(ERROR) << "Invalid coding scheme " << (int) event.codingMeta.coding;
                event.opcode = Opcode::RPR_CHUNK_REP_FAIL;
                break;
            } 
        { // scope for declaring variables..
            // start repairing
            bool isCAR = event.repairUsingCAR;
            bool useEncode = isCAR;
            int numChunksPerNode = 1;
            int numReq = isCAR? event.numChunkGroups : event.chunkGroupMap[0];
            // construct the requests for input chunks
            ChunkEvent getInputEvents[numReq * 2];
            IO::RequestMeta meta[numReq];
            pthread_t rt[numReq];
            unsigned char matrix[numReq];
            unsigned char namespaceId = event.chunks[0].getNamespaceId();
            boost::uuids::uuid fileuuid = event.chunks[0].getFileUUID();
            int version = event.chunks[0].getFileVersion();
            //DLOG(INFO) << "Number of chunk groups = " << event.numChunkGroups << " address " << event.agents;
            
            DLOG(INFO) << "START of chunk repair useCar = " << isCAR << " numReq = " << numReq;
            int cpos = 0; // chunk list starting position
            int spos = 0, epos = 0; // agent address positions
            for (int i = 0; i < numReq; i++) {
                // setup the event
                int numChunks = isCAR? event.chunkGroupMap[i + cpos] : numChunksPerNode;
                getInputEvents[i].id = self->_eventCount.fetch_add(1);
                // encode if using CAR with 1 chunk to repair, else get the original chunk
                getInputEvents[i].opcode = useEncode? Opcode::ENC_CHUNK_REQ : Opcode::GET_CHUNK_REQ;
                getInputEvents[i].numChunks = numChunks;
                getInputEvents[i].containerIds = &event.containerGroupMap[cpos];
                getInputEvents[i].chunks = new Chunk[numChunks];
                if (getInputEvents[i].chunks == NULL) {
                    LOG(ERROR) << "Failed to allocate memory for " << numChunks << " chunks";
                    exit(1);
                }
                for (int j = 0; j < numChunks; j++) {
                    int cid = isCAR? event.chunkGroupMap[cpos + i + j + 1]:
                            event.chunkGroupMap[cpos + j + 1];
                    getInputEvents[i].chunks[j].setId(namespaceId, fileuuid, cid);
                    getInputEvents[i].chunks[j].size = 0;
                    getInputEvents[i].chunks[j].data = 0;
                    getInputEvents[i].chunks[j].fileVersion = version;
                }
                if (isCAR) {
                    getInputEvents[i].codingMeta.codingStateSize = numChunks;
                    getInputEvents[i].codingMeta.codingState = &event.codingMeta.codingState[cpos];
                    // set the matrix coefficient (addition/XOR operation)
                    matrix[i] = 1;
                } else if (useEncode) {
                    getInputEvents[i].codingMeta.codingStateSize = numChunks;
                    getInputEvents[i].codingMeta.codingState = matrix + i;
                    matrix[i] = 1;
                }
                // setup request metadata
                meta[i].isFromProxy = false;
                meta[i].containerId = event.containerGroupMap[cpos];
                meta[i].cxt = &(self->_cxt);
                epos = event.agents.find(';', spos);
                meta[i].address = event.agents.substr(spos, epos - spos);
                spos = epos + 1;
                meta[i].request = &getInputEvents[i];
                meta[i].reply = &getInputEvents[numReq + i];
                // send the request
                pthread_create(&rt[i], NULL, IO::sendChunkRequestToAgent, (void *) &meta[i]);
                // increment chunk list position
                cpos += numChunks;
            }
            // check the chunk replies
            bool allsuccess = true;
            unsigned char *input[numReq], *output[event.numChunks];
            int chunkSize = 0;
            for (int i = 0; i < numReq; i++) {
                // wait for the request to complete
                void *ptr = 0;
                pthread_join(rt[i], &ptr);
                // avoid freeing reference to local variables
                getInputEvents[i].containerIds = 0; 
                getInputEvents[i].codingMeta.codingState = 0; 
                Opcode expectedOp = useEncode? ENC_CHUNK_REP_SUCCESS : GET_CHUNK_REP_SUCCESS;
                if (ptr != 0 || meta[i].reply->opcode != expectedOp) {
                    LOG(ERROR) << "Failed to operate on chunk (" << ENC_CHUNK_REQ << ") due to internal failure, container id = " << meta[i].containerId << ", return opcode =" << meta[i].reply->opcode;
                    allsuccess = false;
                    continue;
                }
                input[i] = meta[i].reply->chunks[0].data;
                chunkSize = meta[i].reply->chunks[0].size;
            }
            // start repair after getting all required chunks
            if (allsuccess) {
                for (int i = 0; i < event.numChunks; i++) {
                    event.chunks[i].data = (unsigned char *) malloc (chunkSize);
                    event.chunks[i].size = chunkSize;
                    output[i] = event.chunks[i].data;
                }
                // do decoding
                CodingUtils::encode(input, numReq, output, event.numChunks, chunkSize, isCAR? matrix : event.codingMeta.codingState);
                // compute checksum
                for (int i = 0; i < event.numChunks; i++) {
                    event.chunks[i].computeMD5();
                }
                // send chunks out
                int numChunksToSend = isCAR? 0 : event.numChunks - numChunksPerNode;
                int numChunkReqsToSend = numChunksToSend / numChunksPerNode;
                ChunkEvent storeChunkEvents[numChunkReqsToSend * 2];
                IO::RequestMeta storeChunkMeta[numChunkReqsToSend];
                pthread_t wt[numChunkReqsToSend];
                for (int i = 0; i < numChunkReqsToSend; i++) {
                    // setup the request
                    storeChunkEvents[i].id = self->_eventCount.fetch_add(1);
                    storeChunkEvents[i].opcode = Opcode::PUT_CHUNK_REQ;
                    storeChunkEvents[i].numChunks = numChunksPerNode;
                    storeChunkEvents[i].chunks = new Chunk[storeChunkEvents[i].numChunks];
                    storeChunkEvents[i].containerIds = new int[storeChunkEvents[i].numChunks];
                    if (storeChunkEvents[i].chunks == NULL || storeChunkEvents[i].containerIds == NULL) {
                        LOG(ERROR) << "Failed to allocate memeory for sending repaired chunks (" << i << " of " << event.numChunks - 1 << "chunks)";
                        delete storeChunkEvents[i].chunks;
                        delete storeChunkEvents[i].containerIds;
                        allsuccess = false;
                        break;
                    }
                    // mark the chunks
                    for (int j = 0; j < storeChunkEvents[i].numChunks; j++) {
                        storeChunkEvents[i].chunks[j] = event.chunks[(i + 1) * storeChunkEvents[i].numChunks + j];
                        storeChunkEvents[i].chunks[j].freeData = false;
                        storeChunkEvents[i].containerIds[j] = event.containerIds[i + 1];
                    }
                    // setup the io meta for request and reply
                    storeChunkMeta[i].containerId = event.containerIds[i + 1];
                    storeChunkMeta[i].request = &storeChunkEvents[i];
                    storeChunkMeta[i].reply = &storeChunkEvents[i + numChunkReqsToSend];
                    storeChunkMeta[i].cxt = &(self->_cxt);
                    epos = event.agents.find(';', spos);
                    storeChunkMeta[i].address = event.agents.substr(spos, epos - spos);
                    spos = epos + 1;
                    // send the request and wait for reply
                    pthread_create(&wt[i], NULL, IO::sendChunkRequestToAgent, (void *) &storeChunkMeta[i]);
                }
                int numLocalChunks = isCAR? event.numChunks : numChunksPerNode;
                int localContainerIds[numLocalChunks];
                for (int i = 0; i < numLocalChunks; i++)
                    localContainerIds[i] = event.containerIds[0];
                // put chunk locally
                if (self->_containerManager->putChunks(localContainerIds, event.chunks, numLocalChunks) == true) {
                    LOG(INFO) << "Put " << numLocalChunks << " repaired chunks into containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                } else {
                    LOG(ERROR) << "Failed to put " << numLocalChunks << " repaired chunks into containers";
                    allsuccess = false;
                }
                // TODO check if other chunks are stored successfully
                for (int i = 0; i < numChunkReqsToSend; i++) {
                    void *ptr = 0;
                    pthread_join(wt[i], &ptr);
                    if (ptr != 0 || storeChunkMeta[i].reply->opcode != Opcode::PUT_CHUNK_REP_SUCCESS) {
                        LOG(ERROR) << "Failed to put " << storeChunkMeta[i].request->numChunks 
                                   << " repaired chunk (" << storeChunkMeta[i].request->chunks[0].getChunkId() << ")"
                                   << " to container " << storeChunkMeta[i].containerId
                                   << " at " << storeChunkMeta[i].address;
                        allsuccess = false;
                    }
                }
            }
            DLOG(INFO) << "END of chunk repair useCar = " << isCAR << " numReq = " << numReq;
            // set reply, increment op count
            if (allsuccess) {
                event.opcode = Opcode::RPR_CHUNK_REP_SUCCESS;
                self->incrementOp();
            } else {
                event.opcode = RPR_CHUNK_REP_FAIL;
                self->incrementOp(false);
            }
        } // scope for declaring variables..
            break;

        case CHK_CHUNK_REQ:
            if (self->_containerManager->hasChunks(event.containerIds, event.chunks, event.numChunks)) {
                LOG(INFO) << "Checked " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                event.opcode =  Opcode::CHK_CHUNK_REP_SUCCESS;
            } else {
                LOG(ERROR) << "Failed to find (some of) " << event.numChunks << " chunks in containers for checking";
                event.opcode = Opcode::CHK_CHUNK_REP_FAIL;
            }
            break;

        case MOV_CHUNK_REQ:
            if (self->_containerManager->moveChunks(event.containerIds, event.chunks, &(event.chunks[event.numChunks]), event.numChunks) == true) {
                event.opcode = Opcode::MOV_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Move " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::MOV_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to move " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            // move the chunk metadata forward for reply
            for (int i = 0; i < event.numChunks; i++)
                event.chunks[i].copyMeta(event.chunks[event.numChunks]);
            break;

        case RVT_CHUNK_REQ:
            if (self->_containerManager->revertChunks(event.containerIds, event.chunks, event.numChunks) == true) {
                event.opcode = Opcode::RVT_CHUNK_REP_SUCCESS;
                LOG(INFO) << "Revert " << event.numChunks << " chunks in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                self->incrementOp();
            } else {
                event.opcode = Opcode::RVT_CHUNK_REP_FAIL;
                LOG(ERROR) << "Failed to revert " << event.numChunks << " chunks in containers";
                self->incrementOp(false);
            }
            break;

        case VRF_CHUNK_REQ:
            { 
                int numCorruptedChunks = 0;
                if ((numCorruptedChunks = self->_containerManager->verifyChunks(event.containerIds, event.chunks, event.numChunks)) >= 0) {
                    // report only corrupted chunks (in-place replaced by the function call)
                    LOG(INFO) << "Verify checksums " << event.numChunks << " chunks (" << numCorruptedChunks << " failed) in containers in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";
                    event.numChunks = numCorruptedChunks;
                    event.opcode = Opcode::VRF_CHUNK_REP_SUCCESS;
                    self->incrementOp();
                } else {
                    event.opcode = Opcode::VRF_CHUNK_REP_FAIL;
                    LOG(ERROR) << "Failed to verify checksums for " << event.numChunks << " chunks in containers";
                    self->incrementOp(false);
                }
            }
        }

        // send a reply
        try {
            
            // TAGPT(start): agent send reply to proxy
            tagPt_rep2Pxy.markStart();

            // copy from tagpts to event
            event.p2a.getEnd() = tagPt_getCnkEvMsg.getEnd();
            event.agentProcess = tagPt_agentProcess;
            event.a2p.getStart() = tagPt_rep2Pxy.getStart();

            // mytimer.start();

            traffic = IO::sendChunkEventMessage(socket, event);

            self->addEgressTraffic(traffic);

            // TAGPT(end): agent send reply to proxy
            tagPt_rep2Pxy.markEnd();

            // DLOG(INFO) << "Send reply in " << mytimer.elapsed().wall * 1.0 / 1e9 << " seconds";

        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to send chunk event message: " << e.what();
            break;
        }
    }

    return NULL;
}

void Agent::addIngressTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.traffic.in += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addEgressTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.traffic.out += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addEgressChunkTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.chunk.out += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::addIngressChunkTraffic(unsigned long int traffic) {
    pthread_mutex_lock(&_stats.lock);
    _stats.chunk.in += traffic;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::incrementOp(bool success) {
    pthread_mutex_lock(&_stats.lock);
    if (success)
        _stats.ops.success++;
    else
        _stats.ops.fail++;
    pthread_mutex_unlock(&_stats.lock);
}

void Agent::printStats() {
    printf(
        "----- Agent Stats -----\n"
        "Total Traffic   (in) %10lu (out)  %10lu\n"
        "Chunk Traffic   (in) %10lu (out)  %10lu\n"
        "Operation count (ok) %10lu (fail) %10lu\n"
        "-----------------------\n"
        , _stats.traffic.in
        , _stats.traffic.out
        , _stats.chunk.in
        , _stats.chunk.out
        , _stats.ops.success
        , _stats.ops.fail
    );
}
