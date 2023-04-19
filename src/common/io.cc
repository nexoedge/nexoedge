// SPDX-License-Identifier: Apache-2.0

#include <string.h>

#include <boost/timer/timer.hpp>
#include <glog/logging.h>

#include "io.hh"
#include "../common/define.hh"
#include "../common/util.hh"

#include "../common/benchmark/benchmark.hh"

bool IO::isFromProxy(unsigned short opcode) {
    // all opcodes from proxy
    return (
        opcode == PUT_CHUNK_REQ ||
        opcode == GET_CHUNK_REQ ||
        opcode == DEL_CHUNK_REQ ||
        opcode == CPY_CHUNK_REQ ||
        opcode == ENC_CHUNK_REQ ||
        opcode == REG_AGENT_REQ ||
        opcode == UPD_AGENT_REQ ||
        opcode == SYN_PING ||
        opcode == RPR_CHUNK_REQ ||
        opcode == CHK_CHUNK_REQ ||
        opcode == MOV_CHUNK_REQ ||
        opcode == VRF_CHUNK_REQ ||
        false
    );
}

bool IO::isFromAgent(unsigned short opcode) {
    // all opcodes from agent
    return !(
        IO::isFromProxy(opcode)
    );
}


bool IO::hasData(unsigned short opcode) {
    // all failure replies and delete chunk replies have no data
    return !(
            opcode == Opcode::PUT_CHUNK_REP_FAIL ||
            opcode == Opcode::GET_CHUNK_REP_FAIL ||
            opcode == Opcode::DEL_CHUNK_REP_FAIL ||
            opcode == Opcode::DEL_CHUNK_REP_SUCCESS ||
            opcode == Opcode::ENC_CHUNK_REP_FAIL ||
            opcode == Opcode::CHK_CHUNK_REP_FAIL ||
            opcode == Opcode::VRF_CHUNK_REP_FAIL ||
            false
    );
}

bool IO::hasContainerIds(unsigned short opcode) {
    // all messages with data, except encode chunk replies and verify chunk replies, have container ids
    return (
        opcode != Opcode::ENC_CHUNK_REP_SUCCESS &&
        opcode != Opcode::ENC_CHUNK_REP_FAIL &&
        opcode != Opcode::VRF_CHUNK_REP_SUCCESS &&
        opcode != Opcode::VRF_CHUNK_REP_FAIL &&
        true
    ) && hasData(opcode); 
}

bool IO::hasChunkData(unsigned short opcode) {
    // put chunk requests, get chunk replies, and encode chunk replies contain chunk data
    return (
        opcode == Opcode::PUT_CHUNK_REQ || 
        opcode == Opcode::GET_CHUNK_REP_SUCCESS ||
        opcode == Opcode::ENC_CHUNK_REP_SUCCESS ||
        false
    ) && hasData(opcode) ;
}

bool IO::needsCoding(unsigned short opcode) {
    // only the encoding chunk request contains coding metadata
    return (
        opcode == Opcode::ENC_CHUNK_REQ ||
        opcode == Opcode::RPR_CHUNK_REQ ||
        false
    );
}

bool IO::hasRepairChunkInfo(unsigned short opcode) {
    // only the encoding chunk request contains coding metadata
    return (
        opcode == Opcode::RPR_CHUNK_REQ
    );
}

int IO::getNumChunkFactor(unsigned short opcode) {
    switch (opcode) {
    case Opcode::CPY_CHUNK_REQ:
    case Opcode::MOV_CHUNK_REQ:
        return 2;
    default:
        return 1;
    }
    return 1;
}


unsigned long int IO::getChunkEventMessage(zmq::socket_t &socket, ChunkEvent &event) {
    unsigned long int bytes = 0;
    zmq::message_t req;

#define getNextMsg( ) do { \
    req.rebuild(); \
    socket.recv(&req); \
    bytes += req.size(); \
} while(0);

#define getField(_FIELD_, _CAST_TYPE_) do { \
    getNextMsg(); \
    event._FIELD_= *((_CAST_TYPE_ *) req.data()); \
} while(0)

    // header
    getField(id, unsigned int);

    if (!req.more()) return 0;
    getField(opcode, unsigned short);


    // benchmark: recv TAGPT
    if (isFromProxy(event.opcode)) {
        if (!req.more()) return 0;
        getField(p2a.getStart().get().tv_sec, __time_t);
        if (!req.more()) return 0;
        getField(p2a.getStart().get().tv_nsec, __syscall_slong_t);
    } else if (isFromAgent(event.opcode)) {
        if (!req.more()) return 0;
        getField(p2a.getEnd().get().tv_sec, __time_t);
        if (!req.more()) return 0;
        getField(p2a.getEnd().get().tv_nsec, __syscall_slong_t);
        if (!req.more()) return 0;
        getField(agentProcess.getStart().get().tv_sec, __time_t);
        if (!req.more()) return 0;
        getField(agentProcess.getStart().get().tv_nsec, __syscall_slong_t);
        if (!req.more()) return 0;
        getField(agentProcess.getEnd().get().tv_sec, __time_t);
        if (!req.more()) return 0;
        getField(agentProcess.getEnd().get().tv_nsec, __syscall_slong_t);
        if (!req.more()) return 0;
        getField(a2p.getStart().get().tv_sec, __time_t);
        if (!req.more()) return 0;
        getField(a2p.getStart().get().tv_nsec, __syscall_slong_t);
    }
    
   
    // end if we expect header only
    if (!hasData(event.opcode)) return bytes;

    // data
    if (!req.more()) return 0;
    getField(numChunks, int);


    // data (container ids)
    if (hasContainerIds(event.opcode)) {
        event.containerIds = new int[event.numChunks];
        if (!req.more()) return 0;
        getNextMsg();
        for (int i = 0; i < event.numChunks; i++) {
            event.containerIds[i] = ((int *)req.data())[i];
        }
    }

    // data (chunks)
    int actualNumChunks = event.numChunks * getNumChunkFactor(event.opcode);
    if (event.numChunks > 0)
        event.chunks = new Chunk[actualNumChunks];
    for (int i = 0; i < actualNumChunks; i++) {
        // namespace id
        if (!req.more()) return 0;
        getField(chunks[i].namespaceId, unsigned char);
        // uuid
        if (!req.more()) return 0;
        getNextMsg();
        memcpy(event.chunks[i].fuuid.data, req.data(), boost::uuids::uuid::static_size());
        // chunk id
        if (!req.more()) return 0;
        getField(chunks[i].chunkId, int);
        // file version
        if (!req.more()) return 0;
        getField(chunks[i].fileVersion, int);
        // chunk version
        unsigned char versionLength = 0;
        if (!req.more()) return 0;
        getNextMsg();
        versionLength = *((unsigned char*) req.data());
        if (versionLength > 0) {
            // avoid overflow
            if (versionLength > CHUNK_VERSION_MAX_LEN - 1)
                versionLength = CHUNK_VERSION_MAX_LEN - 1;
            if (!req.more()) return 0;
            getNextMsg();
            memcpy(event.chunks[i].chunkVersion, req.data(), versionLength);
            event.chunks[i].chunkVersion[versionLength] = 0;
        }
        // chunk checksum (md5)
        if (!req.more()) return 0;
        getNextMsg();
        memcpy(event.chunks[i].md5, req.data(), MD5_DIGEST_LENGTH);
        // chunk size
        if (!req.more()) return 0;
        getField(chunks[i].size, int);
        // chunk data
        if (hasChunkData(event.opcode)) {
            if (!req.more()) return 0;
            getNextMsg();
            event.chunks[i].data = (unsigned char*) malloc (event.chunks[i].size);
            memcpy(event.chunks[i].data, req.data(), event.chunks[i].size);
        } else {
            event.chunks[i].data = 0;
        }
        event.chunks[i].freeData = true;
    }

    // conding metadata 
    /*
    if (!req.more()) return 0;
    getField(codingMeta.coding, unsigned int);

    if (!req.more()) return 0;
    getField(codingMeta.n, int);

    if (!req.more()) return 0;
    getField(codingMeta.k, int);
    */

    if (needsCoding(event.opcode)) {
        if (!req.more()) return 0;
        getField(codingMeta.codingStateSize, int);

        if (event.codingMeta.codingStateSize > 0) {
            if (!req.more()) return 0;
            getNextMsg();
            event.codingMeta.codingState = (unsigned char*) malloc (event.codingMeta.codingStateSize);
            memcpy(event.codingMeta.codingState, req.data(), event.codingMeta.codingStateSize);
        }
    }

    // repair chunk info
    if (hasRepairChunkInfo(event.opcode)) {
        if (!req.more()) return 0;
        getField(codingMeta.coding, unsigned char);
        if (!req.more()) return 0;
        getField(numChunkGroups, int);
        if (!req.more()) return 0;
        getField(numInputChunks, int);
        if (!req.more()) return 0;
        getNextMsg();
        int chunkGroupMapSize = sizeof(int) * (event.numChunkGroups + event.numInputChunks);
        event.chunkGroupMap = (int*) malloc (chunkGroupMapSize);
        memcpy(event.chunkGroupMap, req.data(), chunkGroupMapSize);
        if (!req.more()) return 0;
        getNextMsg();
        int containerGroupMapSize = sizeof(int) * event.numInputChunks;
        event.containerGroupMap = (int*) malloc (containerGroupMapSize);
        memcpy(event.containerGroupMap, req.data(), containerGroupMapSize);
        if (!req.more()) return 0;
        getNextMsg();
        event.agents.append((char *) req.data(), req.size());
        if (!req.more()) return 0;
        getField(repairUsingCAR, bool);
    }

    DLOG(INFO) << "Message received (" << bytes << "B)";

#undef getNextMsg
#undef getField

    return bytes;
}

unsigned long int IO::sendChunkEventMessage(zmq::socket_t &socket, const ChunkEvent &event) {

    // TODO endianness
    unsigned long int bytes = 0;

    // header
    bytes += socket.send(&event.id, sizeof(event.id), ZMQ_SNDMORE);
    bytes += socket.send(&event.opcode, sizeof(event.opcode), ZMQ_SNDMORE);

    // benchmark: send TAGPT
    if (isFromProxy(event.opcode)) {
        bytes += socket.send(&event.p2a.getStart_const().get_const().tv_sec, sizeof(__time_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.p2a.getStart_const().get_const().tv_nsec, sizeof(__syscall_slong_t), !hasData(event.opcode) ? 0 : ZMQ_SNDMORE);
    } else if (isFromAgent(event.opcode)) {
        bytes += socket.send(&event.p2a.getEnd_const().get_const().tv_sec, sizeof(__time_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.p2a.getEnd_const().get_const().tv_nsec, sizeof(__syscall_slong_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.agentProcess.getStart_const().get_const().tv_sec, sizeof(__time_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.agentProcess.getStart_const().get_const().tv_nsec, sizeof(__syscall_slong_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.agentProcess.getEnd_const().get_const().tv_sec, sizeof(__time_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.agentProcess.getEnd_const().get_const().tv_nsec, sizeof(__syscall_slong_t), ZMQ_SNDMORE);   
        bytes += socket.send(&event.a2p.getStart_const().get_const().tv_sec, sizeof(__time_t), ZMQ_SNDMORE);
        bytes += socket.send(&event.a2p.getStart_const().get_const().tv_nsec, sizeof(__syscall_slong_t), !hasData(event.opcode) ? 0 : ZMQ_SNDMORE);
    }

    if (!hasData(event.opcode)) return bytes;

    // data
    bytes += socket.send(&event.numChunks, sizeof(int), event.numChunks > 0? ZMQ_SNDMORE: 0);
    if (hasContainerIds(event.opcode)) {
        // no container id for put chunk request
        bytes += socket.send(event.containerIds, sizeof(int) * event.numChunks, ZMQ_SNDMORE);
    }

    int actualNumChunks = event.numChunks * getNumChunkFactor(event.opcode);

    // (in chunks)
    for (int i = 0; i < actualNumChunks; i++) {
        // namespace id
        bytes += socket.send(&event.chunks[i].namespaceId, sizeof(unsigned char ), ZMQ_SNDMORE);
        // uuid
        bytes += socket.send(event.chunks[i].fuuid.data, boost::uuids::uuid::static_size(), ZMQ_SNDMORE);
        // chunk id
        bytes += socket.send(&event.chunks[i].chunkId, sizeof(int), ZMQ_SNDMORE);
        // file version
        bytes += socket.send(&event.chunks[i].fileVersion, sizeof(int), ZMQ_SNDMORE);
        // chunk version
        unsigned char versionLength = strlen(event.chunks[i].chunkVersion);
        if (versionLength > CHUNK_VERSION_MAX_LEN - 1)
            versionLength = CHUNK_VERSION_MAX_LEN - 1;
        bytes += socket.send(&versionLength, sizeof(unsigned char), ZMQ_SNDMORE);
        if (versionLength > 0)
            bytes += socket.send(event.chunks[i].chunkVersion, versionLength, ZMQ_SNDMORE);
        // chunk checksum (md5)
        bytes += socket.send(event.chunks[i].md5, MD5_DIGEST_LENGTH, ZMQ_SNDMORE);
        // chunk size
        bytes += socket.send(&event.chunks[i].size, sizeof(event.chunks[i].size), (!hasChunkData(event.opcode) && !needsCoding(event.opcode) && i + 1 == actualNumChunks)? 0: ZMQ_SNDMORE);
        // chunk data
        if (hasChunkData(event.opcode)) {
            bytes += socket.send(event.chunks[i].data, event.chunks[i].size, (!needsCoding(event.opcode) && i + 1 == actualNumChunks)? 0 : ZMQ_SNDMORE);
        }
    }


    if (!needsCoding(event.opcode)) return bytes;

    // coding metadata
    /*
    bytes += socket.send(&event.codingMeta.coding, sizeof(event.codingMeta.coding), ZMQ_SNDMORE);
    bytes += socket.send(&event.codingMeta.n, sizeof(event.codingMeta.n), ZMQ_SNDMORE);
    bytes += socket.send(&event.codingMeta.k, sizeof(event.codingMeta.k), ZMQ_SNDMORE);
    */
    bytes += socket.send(&event.codingMeta.codingStateSize, sizeof(event.codingMeta.codingStateSize), event.codingMeta.codingStateSize > 0? ZMQ_SNDMORE : 0);
    if (event.codingMeta.codingStateSize > 0)
        bytes += socket.send(event.codingMeta.codingState, event.codingMeta.codingStateSize, hasRepairChunkInfo(event.opcode)? ZMQ_SNDMORE : 0);

    if (!hasRepairChunkInfo(event.opcode)) return bytes;

    // repair chunk info
    bytes += socket.send(&event.codingMeta.coding, sizeof(event.codingMeta.coding), ZMQ_SNDMORE);
    bytes += socket.send(&event.numChunkGroups, sizeof(event.numChunkGroups), ZMQ_SNDMORE);
    bytes += socket.send(&event.numInputChunks, sizeof(event.numInputChunks), ZMQ_SNDMORE);
    bytes += socket.send(event.chunkGroupMap, sizeof(int) * (event.numChunkGroups + event.numInputChunks), ZMQ_SNDMORE);
    bytes += socket.send(event.containerGroupMap, sizeof(int) * event.numInputChunks, ZMQ_SNDMORE);
    bytes += socket.send(event.agents.c_str(), event.agents.size(), ZMQ_SNDMORE);

    bytes += socket.send(&event.repairUsingCAR, sizeof(bool), 0);
    
    DLOG(INFO) << "Message sent (" << bytes << "B)";

    return bytes;
}

std::string IO::genAddr(std::string ip, unsigned port) {
    char portstr[8];
    portstr[0] = ':';
    sprintf(&portstr[1], "%d", port);

    std::string addr("tcp://");
    addr.append(ip);
    addr.append(portstr);
    return addr;
}

std::string IO::getAddrIP(std::string addr) {
    size_t start = addr.find_last_of("/") + 1;
    size_t end = addr.find_last_of(":");
    return addr.substr(start, end - start);
}


void *IO::sendChunkRequestToAgent(void *arg) {
    RequestMeta &meta = *((RequestMeta*) arg);

    bool reuse = meta.isFromProxy && Config::getInstance().reuseDataConn();
    zmq::socket_t *socket = 0;
    try {
        if (reuse) {
            socket = meta.socket;
        } else {
            // new a socket
            socket = new zmq::socket_t(*meta.cxt, ZMQ_REQ);
            // setup socket options (TCP keep alive and Agent timeout)
            Util::setSocketOptions(socket);
            int timeout = Config::getInstance().getFailureTimeout();
            socket->setsockopt(ZMQ_SNDTIMEO, timeout);
            socket->setsockopt(ZMQ_RCVTIMEO, timeout);
            socket->setsockopt(ZMQ_LINGER, timeout);
            // connect to the agent
            socket->connect(meta.address);
        }
        
        // boost::timer::cpu_timer mytimer;
        
        // send the chunk event request
        unsigned long sent = IO::sendChunkEventMessage(*socket, *meta.request);
        if (sent == 0) {
            LOG(ERROR) << "Failed to send chunk event over socket at " << meta.address;
            if (!reuse) {
                socket->close();
                delete socket;
            }
            return (void *) -1;
        }
        // boost::timer::cpu_times sentDuration = mytimer.elapsed();
        // DLOG(INFO) << "Sent chunk event (container id = " << meta.containerId << ") to " << meta.address;
        // mytimer.start();

        // get the chunk event reply
        unsigned long received = IO::getChunkEventMessage(*socket, *meta.reply);
        if (received == 0) {
            LOG(ERROR) << "Failed to get a chunk event reply over socket at " << meta.address;
            if (!reuse) {
                socket->close();
                delete socket;
            }
            return (void *) -2;
        }

        // DLOG(INFO) << "Handle chunk event (container id = " << meta.containerId << ") to " 
        //           << meta.address << ", send (" << (hasChunkData(meta.request->opcode)? "with" : "without")
        //           << " chunks) in " << sentDuration.wall * 1.0 / 1e9 << " s, get reply ("
        //           << (hasChunkData(meta.reply->opcode)? "with" : "without")
        //           << " chunks) in " << mytimer.elapsed().wall * 1.0 / 1e9 << " s";
        
        if (!reuse) {
            socket->close();
            delete socket;
        }
    } catch (zmq::error_t &e) {
        LOG(ERROR) << "Failed to connect agent to send the chunk request opcode = " << meta.request->opcode << ", " << e.what();
        if (!reuse && socket != 0) {
            socket->close();
            delete socket;
        }
        return (void *) -1;
    }

    return NULL;
}
