// SPDX-License-Identifier: Apache-2.0

#include <pthread.h> // pthread_*()
#include <stdio.h> // printf()
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

extern "C" {
#include <oss_c_sdk/aos_http_io.h>
}
#include <zmq.hpp>
#include <glog/logging.h>
#include <aws/core/Aws.h>

#include "../../agent/agent.hh"
#include "../../common/config.hh"
#include "../../common/define.hh"
#include "../../common/io.hh"
#include "../../ds/chunk_event.hh"

/**
 * Agent test
 * 
 * Test flow
 * 1. Run agent on localhost (without registrating to proxy)
 * 2. Put chunks of 'a' to containers, with correct container IDs specified
 *    - Expect successful put 
 * 3. Get chunks from containers, with correct container IDs specified
 *    - Expect successful get
 * 4. Get chunks of 'a' to containers, with incorrect container IDs specified
 *    - Expect get failures
 * 5. Encode chunks from containers, with correct container IDs specified
 *    - Expect successful encode, with one encoded chunk returned
 * 6. Simulate chunk repair using CAR
 * 7. Check the chunks in containers
 *    - Expect successful check
 * 8. Verify chunks in containers
 *    - Expect successful verification
 * 9. Verify corrupt chunks in containers
 *    - Expect verification failures
 * 10. Delete chunks from containers, with correct container IDs specified
 *    - Expect successful delete
 * 11. Check the chunks in containers
 *    - Expect check failures
 * 12. Verify chunks in containers
 *    - Expect verification failures
 * Printing of traffic and requests statistics in Agent
 **/

#define CHUNK_SIZE (1024)
#define NUM_CHUNKS (2)

static void *runAgent(void *arg) {
    Agent *agent = (Agent*) arg;

    agent->run(/* register to proxy */ false);

    return NULL;
}

int main(int argc, char **argv) {
    Config &config = Config::getInstance();
    config.setConfigPath();

    // init aws sdk
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    // init aliyun sdk
    if (aos_http_io_initialize(NULL, 0) != AOSE_OK) {
        LOG(ERROR) << "Failed to init Aliyun OSS interface";
        return 1;
    }

    printf("Start Agent Test\n");
    printf("====================\n");

    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    google::InitGoogleLogging(argv[0]);

    // ---------------------------
    // 1. run agent in background
    // ---------------------------
    Agent *agent = new Agent();
    pthread_t at;
    pthread_create(&at, NULL, runAgent, agent);

    // simulate incoming message from client

    std::string agentIP = Config::getInstance().getAgentIP();
    unsigned short port = Config::getInstance().getAgentPort();

    zmq::socket_t requester(agent->_cxt, ZMQ_REQ);

    sleep(1);

    requester.connect(IO::genAddr(agentIP, port));
    if (!requester.connected()) {
        printf("Failed to connect agent\n");
        return 1;
    }

    ChunkEvent event, event2, event3, event4, event5, event6, event7, event8, event9, event10, event11, event12, event13;

    agent->printStats();

    // ----------------
    // 2. normal put chunk
    // ----------------
    event.id = 12345;
    event.opcode = PUT_CHUNK_REQ;
    event.numChunks = NUM_CHUNKS;
    event.chunks = new Chunk[event.numChunks];
    event.containerIds = new int[event.numChunks];

    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid fileuuid = gen(); 
    printf("uuid1 = %s\n", boost::uuids::to_string(fileuuid).c_str());
    unsigned char  namespaceId = 1;

    for (int i = 0; i < event.numChunks; i++) {
        event.chunks[i].setId(namespaceId, fileuuid, i);
        event.chunks[i].size = CHUNK_SIZE;
        event.chunks[i].data = (unsigned char*) malloc (event.chunks[i].size);
        event.chunks[i].fileVersion = 0;
        event.chunks[i].freeData = true;
        event.containerIds[i] = config.getContainerId(i + 1);
        memset(event.chunks[i].data, 'a', event.chunks[i].size); 
        event.chunks[i].computeMD5();
    }
    IO::sendChunkEventMessage(requester, event);
    IO::getChunkEventMessage(requester, event2);

    // check results
    if (event.id != event2.id) {
        printf("> [Put Chunk] Event id mismatched\n");
        return 1;
    }
    if (event2.opcode != Opcode::PUT_CHUNK_REP_SUCCESS) {
        printf("> [Put Chunk] Unexpected opcode %d\n", event2.opcode);
        return 1;
    }
    if (event.numChunks != event2.numChunks) {
        printf("> [Put Chunk] Incorrect number of chunks\n");
        return 1;
    }
    for (int i = 0; i < event.numChunks; i++) {
        printf("> [Put Chunk] Chunk %s is stored in container %d\n", event2.chunks[i].getChunkName().c_str(), event2.containerIds[i]);
        if (event.chunks[i].getChunkName() != event2.chunks[i].getChunkName()) {
            printf("> [Put Chunk] ID of Chunk %d mismatch (%s vs %s)\n", i, event2.chunks[i].getChunkName().c_str(), event.chunks[i].getChunkName().c_str());
            return 1;
        }
        if (event.chunks[i].size != event2.chunks[i].size) {
            printf("> [Put Chunk] Size of Chunk %d mismatch (%d vs %d)\n", i, event2.chunks[i].size, event.chunks[i].size);
            return 1;
        }
    }

    agent->printStats();

    printf("> Pass normal put chunk test\n");
    
    // --------------------
    // 3. normal get chunk
    // --------------------
    event2.id = 234568;
    event2.opcode = Opcode::GET_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event3);

    if (event2.id != event3.id) {
        printf("> [Get chunk] Event id mismatched\n"); 
        return 1;
    }
    if (event3.opcode != Opcode::GET_CHUNK_REP_SUCCESS) {
        printf("> [Get chunk] Unexpected opcode, expect %d but got %d\n", Opcode::GET_CHUNK_REP_SUCCESS, event3.opcode);
        return 1;
    }
    if (event2.numChunks != event3.numChunks) {
        printf("> [Get chunk] Incorrect number of chunks\n");
        return 1;
    }

    agent->printStats();

    printf("> Pass normal get chunk test\n");

    // ------------------------
    // 4. invalid container id
    // ------------------------
    int correctCid = event2.containerIds[1];
    event2.id = 34566;
    event2.containerIds[1] = 123;
    printf("> Change container Id from %d to %d\n", correctCid, event2.containerIds[0]);
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event4);

    if (event2.id != event4.id) {
        printf("> [Invalid get chunk] Event id mismatched\n");
        return 1;
    }
    if (event4.opcode != Opcode::GET_CHUNK_REP_FAIL) {
        printf("> [Invalid get chunk] Unexpected opcode, expect %d but got %d\n", Opcode::GET_CHUNK_REP_FAIL, event4.opcode);
        return 1;
    }

    agent->printStats();

    printf("> Pass invalid get chunk test\n");

    // ------------------------
    // 5. get an encoded chunk
    // ------------------------
    event2.id = 19384;
    event2.containerIds[1] = correctCid;
    event2.codingMeta.codingStateSize = NUM_CHUNKS;
    event2.codingMeta.codingState = (unsigned char *) malloc (event2.codingMeta.codingStateSize);
    for (int i = 0; i < NUM_CHUNKS; i++)
        event2.codingMeta.codingState[i] = 1;
    event2.opcode = Opcode::ENC_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event5);

    if (event2.id != event5.id) {
        printf("> [Invalid encode chunk] Event id mismatched\n");
        return 1;
    }
    if (event5.opcode != Opcode::ENC_CHUNK_REP_SUCCESS) {
        printf("> [Invalid encode chunk] Unexpected opcode, exptect %d but got %d\n", Opcode::ENC_CHUNK_REP_SUCCESS, event5.opcode);
        return 1;
    }
    if (event5.numChunks != 1) {
        printf("> [Invalid encode chunk] Unexpected number of chunks, exptect 1 but got %d\n", event5.numChunks);
        return 1;
    }
    if (event5.chunks[0].size != CHUNK_SIZE) {
        printf("> [Invalid encode chunk] Unexpected number of chunks, exptect %d but got %d\n", CHUNK_SIZE, event5.numChunks);
        return 1;
    }

    int okay = 0, inv = 0;
    for (int i = 0; i < CHUNK_SIZE; i++) {
        okay += (event5.chunks[0].data[i] == 0)? 1 : 0;
        if (event5.chunks[0].data[i] != 0) inv = i;
    }

    if (okay != CHUNK_SIZE) {
        printf("> [Invalid encode chunk] Unexpected chunks content, exptect %d zeros but got %d zeros (%x)\n", CHUNK_SIZE, okay, event5.chunks[0].data[inv]);
        return 1;
    }

    agent->printStats();

    printf("> Pass generate encoded chunk test\n");

    // -------------------------------------
    // 6. simulate repair via chunk encoding
    // -------------------------------------
    event7.id = 938483;
    event7.opcode = Opcode::RPR_CHUNK_REQ;
    // repair target
    event7.numChunks = 1;
    event7.containerIds = new int[1];
    event7.containerIds[0] = config.getContainerId(NUM_CHUNKS + 1);
    event7.chunks = new Chunk[1];
    event7.chunks[0].setId(namespaceId, fileuuid, NUM_CHUNKS);
    event7.chunks[0].size = 0;
    event7.chunks[0].data = 0;
    event7.chunks[0].fileVersion = 0;
    // how to repair
    event7.codingMeta.coding = CodingScheme::RS;
    event7.codingMeta.codingStateSize = NUM_CHUNKS;
    event7.codingMeta.codingState = (unsigned char *) malloc (NUM_CHUNKS);
    event7.numChunkGroups = NUM_CHUNKS;
    event7.numInputChunks = NUM_CHUNKS;
    event7.chunkGroupMap = (int *) malloc (sizeof(int) * (NUM_CHUNKS + event7.numChunkGroups));
    event7.containerGroupMap = (int *) malloc (sizeof(int) * (NUM_CHUNKS + event7.numChunkGroups));
    for (int i = 0; i < event7.numChunkGroups; i++) {
        event7.codingMeta.codingState[i] = 1;
        event7.chunkGroupMap[i * 2] = 1;
        event7.chunkGroupMap[i * 2 + 1] = i;
        event7.containerGroupMap[i] = config.getContainerId(i + 1);
        event7.agents.append(IO::genAddr(agentIP, port));
        event7.agents.append(";");
    }
    event7.repairUsingCAR = true;
    /*
    for (int i = 0, numChunks = 0; i < event7.numChunkGroups; i++) {
        LOG(INFO) << "Number of chunks " << event7.chunkGroupMap[i + numChunks] << " (" << i + numChunks;
        for (int j = 0; j < event7.chunkGroupMap[i + numChunks]; j++)
            LOG(INFO) << "Group " << i << " chunk " << event7.chunkGroupMap[i + numChunks + j + 1] << " from container " << event7.containerGroupMap[numChunks + j];
        numChunks += event7.chunkGroupMap[i + numChunks];
    }
    */
    IO::sendChunkEventMessage(requester, event7);
    IO::getChunkEventMessage(requester, event8);

    if (event7.id != event8.id) {
        printf("> [Repair chunk, CAR] Event id mismatched\n");
        return 1;
    }

    if (event8.opcode != Opcode::RPR_CHUNK_REP_SUCCESS) {
        printf("> [Repair chunk, CAR] Unexpected opcode, expect %d but got %d\n", Opcode::RPR_CHUNK_REP_SUCCESS, event8.opcode);
        return 1;
    }

    // ----------------
    // 7. check chunks
    // ----------------
    event2.id = 384843;
    event2.opcode = Opcode::CHK_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event6);

    if (event2.id != event6.id) {
        printf("> [Check chunk] Event id mismatched\n");
        return 1;
    }
    if (event6.opcode != Opcode::CHK_CHUNK_REP_SUCCESS) {
        printf("> [Check chunk] Unexpected opcode, expect %d but got %d\n", Opcode::CHK_CHUNK_REP_SUCCESS, event6.opcode);
        return 1;
    }

    event6.release();

    agent->printStats();

    printf("> Pass check chunk test\n");

    // -----------------
    // 8. verify chunks
    // -----------------
    event2.id = 2845958;
    event2.opcode = Opcode::VRF_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event9);

    if (event2.id != event9.id) {
        printf("> [Verify chunk] Event id mismatched\n");
        return 1;
    }
    if (event9.opcode != Opcode::VRF_CHUNK_REP_SUCCESS) {
        printf("> [Verify chunk] Unexpected opcode, expect %d but got %d\n", Opcode::VRF_CHUNK_REP_SUCCESS, event9.opcode);
        return 1;
    }
    if (event9.numChunks != 0) {
        printf("> [Verify chunk] Incorrect number of corrupted chunks, expect %d but got %d\n", 0, event9.numChunks);
        return 1;
    }

    agent->printStats();

    printf("> Pass verify chunk test\n");

    // ---------------------------------------
    // 9. verify chunk (corruption detection)
    // ---------------------------------------
    event10 = event;
    event10.id = 485398;
    event10.opcode = Opcode::PUT_CHUNK_REQ;
    memset(event10.chunks[1].data, 0, event10.chunks[1].size);
    event10.chunks[1].computeMD5();
    IO::sendChunkEventMessage(requester, event10);
    IO::getChunkEventMessage(requester, event11);

    if (event10.id != event11.id) {
        printf("> [Put chunk modification] Event id mismatched\n");
        return 1;
    }

    if (event11.opcode != Opcode::PUT_CHUNK_REP_SUCCESS) {
        printf("> [Put chunk moidification] Unexpected opcode, expect %d but got %d\n", Opcode::PUT_CHUNK_REP_SUCCESS, event11.opcode);
        return 1;
    }

    event10.reset();

    event2.id = 2845958;
    event2.opcode = Opcode::VRF_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event12);

    if (event2.id != event12.id) {
        printf("> [Verify chunk after modification] Event id mismatched\n");
        return 1;
    }
    if (event12.opcode != Opcode::VRF_CHUNK_REP_SUCCESS) {
        printf("> [Verify chunk after modification] Unexpected opcode, expect %d but got %d\n", Opcode::VRF_CHUNK_REP_SUCCESS, event12.opcode);
        return 1;
    }
    if (event12.numChunks != 1) {
        printf("> [Verify chunk after modification] Incorrect number of corrupted chunks, expect %d but got %d\n", 1, event12.numChunks);
        return 1;
    }
    if (event12.chunks[0].getChunkName() != event2.chunks[1].getChunkName()) {
        printf("> [Verify chunk after modification] Incorrect corrupted chunk detected, expect %s but got %s\n", event12.chunks[0].getChunkName().c_str(), event2.chunks[1].getChunkName().c_str());
        return 1;
    }

    agent->printStats();

    printf("> Pass verify chunk test (corrupted chunks)\n");

    // ------------------
    // 10. delete chunks
    // ------------------
    event2.id = 8494859;
    event2.opcode = Opcode::DEL_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event6);
    
    if (event2.id != event6.id) {
        printf("> [Delete chunk] Event id mismatched\n");
        return 1;
    }
    if (event6.opcode != Opcode::DEL_CHUNK_REP_SUCCESS) {
        printf("> [Delete chunk] Unexpected opcode, expect %d but got %d\n", Opcode::DEL_CHUNK_REP_SUCCESS, event6.opcode);
        return 1;
    }

    agent->printStats();

    printf("> Pass delete chunk test\n");

    // -----------------
    // 11. check chunks
    // -----------------
    event2.id = 2734294;
    event2.opcode = Opcode::CHK_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event6);

    if (event2.id != event6.id) {
        printf("> [Check chunk] Event id mismatched\n");
        return 1;
    }
    if (event6.opcode != Opcode::CHK_CHUNK_REP_FAIL) {
        printf("> [Check chunk] Unexpected opcode, expect %d but got %d\n", Opcode::CHK_CHUNK_REP_FAIL, event6.opcode);
        return 1;
    }

    agent->printStats();

    printf("> Pass check chunk test (non-existing chunks)\n");

    // ------------------
    // 12. verify chunks
    // ------------------
    event2.id = 2845958;
    event2.opcode = Opcode::VRF_CHUNK_REQ;
    IO::sendChunkEventMessage(requester, event2);
    IO::getChunkEventMessage(requester, event13);

    if (event2.id != event13.id) {
        printf("> [Verify chunk after delete] Event id mismatched\n");
        return 1;
    }
    if (event13.opcode != Opcode::VRF_CHUNK_REP_SUCCESS) {
        printf("> [Verify chunk after delete] Unexpected opcode, expect %d but got %d\n", Opcode::VRF_CHUNK_REP_SUCCESS, event13.opcode);
        return 1;
    }
    if (event13.numChunks != NUM_CHUNKS) {
        printf("> [Verify chunk after delete] Incorrect number of corrupted chunks detected, expect %d but got %d\n", NUM_CHUNKS, event13.numChunks);
        return 1;
    }
    for (int i = 0; i < NUM_CHUNKS; i++) {
        if (event13.chunks[i].getChunkName() != event.chunks[i].getChunkName()) {
            printf("> [Verify chunk after delete] Incorrect corrupted chunks detected, expect %s but got %s for chunk %d\n", event13.chunks[i].getChunkName().c_str(), event.chunks[i].getChunkName().c_str(), i);
            return 1;
        }
    }

    agent->printStats();

    printf("> Pass verify chunk test (non-existing chunks)\n");


    // clean up at the end of all tests
    requester.close();

    delete agent;
    pthread_join(at, NULL);

    aos_http_io_deinitialize();
    Aws::ShutdownAPI(options);

    printf("End of Agent Test\n");
    printf("====================\n");

    return 0;
}
