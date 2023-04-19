// SPDX-License-Identifier: Apache-2.0

#ifndef __CHUNK_EVENT_HH__
#define __CHUNK_EVENT_HH__

#include "file.hh"
#include "../common/define.hh"
#include "../common/benchmark/benchmark.hh"

struct ChunkEvent {
    // event info
    unsigned int id;                   /**< event id */
    unsigned short opcode;             /**< event operation code */

    // file data (chunks and containers)
    int numChunks;                     /**< number of chunks */
    int *containerIds;                 /**< container ids */
    Chunk *chunks;                     /**< chunks */

    // coding metadata
    CodingMeta codingMeta;             /**< coding metadata */

    // repair info
    bool repairUsingCAR;               /**< indicator for CAR repair */

    // chunk group info
    int numChunkGroups;                /**< number of chunk groups */
    int numInputChunks;                /**< number of input chunks */
    int *chunkGroupMap;                /**< chunk group mapping, in form [total,cids;total,cids;...], and its size is numChunkGroup + numInputChunks */
    int *containerGroupMap;            /**< container group mapping, in form [container id, ...], and its size is numInputChunks */
    std::string agents;                /**< agent address for chunk groups, ";" separated list of addresses ([address";"address";"..]), always ends with a ";" */

    // benchmark
    TagPt p2a;                         /**< TagPt proxy to agent */
    TagPt a2p;                         /**< TagPt agent to proxy */
    TagPt agentProcess;                /**< TagPt agent process time of chunk (put to or get from container) */

    ChunkEvent() {
        reset();
    }

    ~ChunkEvent() {
        release();
    }

    bool initChunks(int num = -1) {
        // use the pre-set number of chunks in event
        if (num == -1)
            num = numChunks;
        if (num == -1) {
            return false;
        }
        // init chunks
        delete [] chunks;
        
        delete [] containerIds;
    }

    void release() {
        delete [] containerIds;
        delete [] chunks;
        free(chunkGroupMap);
        free(containerGroupMap);
        reset();
    }

    void reset() {
        id = 0;
        opcode = Opcode::UNKNOWN_OP;
        numChunks = 0;
        containerIds = 0;
        chunks = 0;
        numChunkGroups = 0;
        numInputChunks = 0;
        chunkGroupMap = 0;
        containerGroupMap = 0;
        repairUsingCAR = false;
    }

};

#endif // define __CHUNK_EVENT_HH__
