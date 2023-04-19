// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <string.h>
#include <linux/limits.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <glog/logging.h>

extern "C" {
#include <oss_c_sdk/aos_http_io.h>
}

#include "../../common/config.hh"
#include "../../ds/chunk.hh"
#include "../../agent/container/all.hh"

/**
 * Container Test
 *
 * Test flow:
 * 1. Init containers
 * 2. Put chunks to containers
 * 3. Put and revert chunks in containers
 * 4. Get chunks from containers
 * 5. List chunks from containers
 * 6. Copy chunks within a container
 * 7. Check chunks existence
 * 8. Move chunks within containers
 * 9. Delete chunks in containers
 * 10. Check chunks existence
 *
 * Expect all operations to finish successfully
 *
 * Usage: ./container_test [chunk size (in bytes)]
 **/


#define NUM_CONTAINER Config::getInstance().getNumContainers()
#define NUM_CHUNK (6)
#define CHUNK_SIZE (1024)

int main(int argc, char **argv) {
    Config &config = Config::getInstance();
    config.setConfigPath();

    int chunkSize = CHUNK_SIZE;

    // take manual chunk size input
    if (argc >= 2) {
        int chunkSizet = atoi(argv[1]);
        if (chunkSizet >= 0)
            chunkSize = chunkSizet;
    }
    
    // configure logging
    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    google::InitGoogleLogging(argv[0]);

    // init aws sdk
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    // init aliyun sdk
    if (aos_http_io_initialize(NULL, 0) != AOSE_OK) {
        LOG(ERROR) << "Failed to init Aliyun OSS interface";
        return 1;
    }

    Container *c[NUM_CONTAINER];

    printf("Start Container Test\n");
    printf("====================\n");
    printf("Chunk size = %dB\n", chunkSize);

    for (int i = 0; i < NUM_CONTAINER; i++) {
        unsigned short ctype = config.getContainerType(i);
        std::string cstr = config.getContainerPath(i);
        int cid = config.getContainerId(i);
        unsigned long int capacity = config.getContainerCapacity(i);
        std::string key = config.getContainerKey(i);
        std::string keyId = config.getContainerKeyId(i);
        std::string region = config.getContainerRegion(i);
        std::string proxyIP = config.getContainerHttpProxyIP(i);
        unsigned short proxyPort = config.getContainerHttpProxyPort(i);
        switch (ctype) {
        case ContainerType::FS_CONTAINER:
            c[i] = new FsContainer(i, cstr.c_str(), capacity);
            break;
        case ContainerType::AWS_CONTAINER:
            c[i] = new AwsContainer(cid, cstr, region, keyId, key, capacity, "", proxyIP, proxyPort);
            break;
        case ContainerType::ALI_CONTAINER:
            c[i] = new AliContainer(cid, cstr, region, keyId, key, capacity);
            break;
        case ContainerType::AZURE_CONTAINER:
            c[i] = new AzureContainer(cid, cstr, key, capacity, proxyIP, proxyPort);
            break;
        default:
            printf("> Container type %d not supported!\n", ctype);
            return -1;
            break;
        }
        printf("> Init container %d path %s\n", i, cstr.c_str());
    }

    Chunk chunks[NUM_CHUNK * 4];

    unsigned long int expected = 0;
    for (int i = 0; i < NUM_CONTAINER; i++) {
        unsigned long int current = c[i]->getUsage(true);
        if (current != expected)
            printf("Failed to get the expected usage (%lu), but get %lu instead\n", expected, current);
    }

    bool okay = true;
    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid fileuuid[(NUM_CHUNK / NUM_CONTAINER + 1) * 3];
    for (int i = 0; i < (NUM_CHUNK / NUM_CONTAINER + 1) * 3; i++) {
        fileuuid[i] = gen(); 
        printf("uuid[%d] = %s\n", i, boost::uuids::to_string(fileuuid[i]).c_str());
    }
    unsigned char  namespaceId = 1;

    // put chunks
    for (int i = 0; i < NUM_CHUNK; i++) {
        chunks[i].setId(namespaceId, fileuuid[i / NUM_CONTAINER], i % NUM_CONTAINER);
        chunks[i].size = chunkSize;
        chunks[i].data = (unsigned char *) malloc (chunkSize * sizeof(unsigned char));
        memset(chunks[i].data, 'a'+i, chunkSize);
        chunks[i].computeMD5();
        if (c[i % NUM_CONTAINER]->putChunk(chunks[i]) == false) {
            printf("Failed to put chunk\n");
            okay = false;
            break;
        } else {
            printf("> Put chunk %s into container %d\n", chunks[i].getChunkName().c_str(), i % NUM_CONTAINER);
        }
    }

    /*
    // overwrite, get, and revert chunks
    {
        // overwrite chunks
        for (int i = 0; i < NUM_CHUNK && okay; i++) {
            chunks[i + NUM_CHUNK].setId(namespaceId, fileuuid[i / NUM_CONTAINER], i % NUM_CONTAINER);
            chunks[i + NUM_CHUNK].size = chunkSize;
            chunks[i + NUM_CHUNK].data = (unsigned char *) malloc (chunkSize * sizeof(unsigned char));
            memset(chunks[i + NUM_CHUNK].data, 'b'+i, chunkSize);
            chunks[i + NUM_CHUNK].computeMD5();
            if (c[i % NUM_CONTAINER]->putChunk(chunks[i + NUM_CHUNK]) == false) {
                printf("Failed to put chunk %d/%d for revert test\n", i + NUM_CHUNK, NUM_CHUNK);
                okay = false;
                break;
            }
        }
        // get chunks
        for (int i = 0; i < NUM_CHUNK && okay; i++) {
            chunks[i + NUM_CHUNK * 2].setId(namespaceId, fileuuid[i / NUM_CONTAINER], i % NUM_CONTAINER);
            memcpy(chunks[i + NUM_CHUNK * 2].md5, chunks[i + NUM_CHUNK].md5, MD5_DIGEST_LENGTH);
            if (c[i % NUM_CONTAINER]->getChunk(chunks[i + NUM_CHUNK * 2]) == false) {
                printf("Failed to get chunk for revert test\n");
                okay = false;
                break;
            }
            if (chunks[i + NUM_CHUNK].size != chunks[i + NUM_CHUNK * 2].size) {
                printf("Chunk size mismatch for revert test, expect %d but got %d\n", chunks[i + NUM_CHUNK].size, chunks[i + NUM_CHUNK * 2].size);
                okay = false;
                break;
            }
            if (memcmp(chunks[i + NUM_CHUNK].data, chunks[i + NUM_CHUNK * 2].data, chunkSize) != 0) {
                printf("Chunk content mismatch for revert test\n");
            }
        }

        // release the allocated resources
        for (int i = 0; i < NUM_CHUNK; i++) {
            free(chunks[i + NUM_CHUNK].data);
            free(chunks[i + NUM_CHUNK * 2].data);
            chunks[i + NUM_CHUNK].data = 0;
            chunks[i + NUM_CHUNK * 2].data = 0;
            chunks[i + NUM_CHUNK].size = 0;
            chunks[i + NUM_CHUNK * 2].size = 0;
        }

        // revert chunks
        for (int i = 0; i < NUM_CHUNK && okay; i++) {
            if (c[i % NUM_CONTAINER]->revertChunk(chunks[i + NUM_CHUNK]) == false) {
                printf("Failed to revert chunk\n");
                okay = false;
                break;
            }
            printf("> Revert chunk %s %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str(), chunks[i + NUM_CHUNK].getChunkVersion());
        }
    }
    */

    // get chunks
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        chunks[i + NUM_CHUNK].setId(namespaceId, fileuuid[i / NUM_CONTAINER], i % NUM_CONTAINER);
        // copy the md5 checksum for verification
        memcpy(chunks[i + NUM_CHUNK].md5, chunks[i].md5, MD5_DIGEST_LENGTH);
        if (c[i % NUM_CONTAINER]->getChunk(chunks[i + NUM_CHUNK]) == false) {
            printf("Failed to get chunk\n");
            okay = false;
            break;
        }
        if (chunks[i].size != chunks[i + NUM_CHUNK].size) {
            printf("Chunk size mismatch, expect %d but got %d\n", chunks[i].size, chunks[i + NUM_CHUNK].size);
            okay = false;
            break;
        }
        if (memcmp(chunks[i].data, chunks[i + NUM_CHUNK].data, chunkSize) != 0) {
            printf("Chunk content mismatch\n");
            okay = false;
            break;
        }
        printf("> Get chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
    }

    for (int i = 0; i < NUM_CONTAINER; i++) {
        unsigned long int current = c[i]->getUsage(true);
        expected = (NUM_CHUNK / NUM_CONTAINER + (NUM_CHUNK % NUM_CONTAINER? 1 : 0)) * chunkSize;
        if (current != expected)
            printf("Failed to get the expected usage (%lu), but get %lu instead\n", expected, current);
    }

    // check integrity of chunks
    if (config.verifyChunkChecksum() && okay) {
        for (int i = 0; i < NUM_CHUNK; i++) {
            if (!c[i % NUM_CONTAINER]->verifyChunk(chunks[i])) {
                printf("Failed to verify checksum of chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
                okay = false;
                break;
            }
        }
    }

    // copy chunks
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        chunks[i + NUM_CHUNK * 2].setId(namespaceId, fileuuid[i / NUM_CONTAINER + NUM_CHUNK / NUM_CONTAINER + 1], i % NUM_CONTAINER);
        if (c[i % NUM_CONTAINER]->copyChunk(chunks[i + NUM_CHUNK], chunks[i + NUM_CHUNK * 2]) == false) {
            printf("Failed to copy chunk %s to %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str(), chunks[i + NUM_CHUNK * 2].getChunkName().c_str());
            okay = false;
            break;
        }
        if (chunks[i + NUM_CHUNK].size != chunks[i + NUM_CHUNK * 2].size) {
            printf("Chunk size mismatch, expect %d but got %d\n", chunks[i + NUM_CHUNK].size, chunks[i + NUM_CHUNK * 2].size);
            okay = false;
            break;
        }
        // copy the md5 checksum for verification
        memcpy(chunks[i + NUM_CHUNK * 2].md5, chunks[i + NUM_CHUNK].md5, MD5_DIGEST_LENGTH);
        if (c[i % NUM_CONTAINER]->getChunk(chunks[i + NUM_CHUNK * 2]) == false || memcmp(chunks[i].data, chunks[i + NUM_CHUNK * 2].data, chunkSize) != 0) {
            printf("Chunk content mismatch\n");
            okay = false;
            break;
        }
        printf("> Copy chunk %s to %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str(), chunks[i + NUM_CHUNK * 2].getChunkName().c_str());
    }

    // check chunks
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        if (c[i % NUM_CONTAINER]->hasChunk(chunks[i + NUM_CHUNK]) == false) {
            printf("Failed to check chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
            okay = false;
            break;
        }
        // simulate size mismatch
        chunks[i + NUM_CHUNK].size = chunkSize - 1;
        if (c[i % NUM_CONTAINER]->hasChunk(chunks[i + NUM_CHUNK]) == true) {
            printf("Failed to detect size-mismatch of chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
            okay = false;
            break;
        }
        chunks[i + NUM_CHUNK].size = chunkSize;
        printf("> Check chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
    }

    // move chunks
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        chunks[i + NUM_CHUNK * 3].setId(namespaceId, fileuuid[i / NUM_CONTAINER + NUM_CHUNK * 2 / NUM_CONTAINER + 1], i % NUM_CONTAINER);
        if (c[i % NUM_CONTAINER]->moveChunk(chunks[i + NUM_CHUNK * 2], chunks[i + NUM_CHUNK * 3]) == false) {
            printf("Failed to move chunk %s to %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str(), chunks[i + NUM_CHUNK * 2].getChunkName().c_str());
            okay = false;
            break;
        }
        if (chunks[i + NUM_CHUNK * 2].size != chunks[i + NUM_CHUNK * 3].size) {
            printf("Chunk size mismatch, expect %d but got %d\n", chunks[i + NUM_CHUNK].size, chunks[i + NUM_CHUNK * 3].size);
            okay = false;
            break;
        }
        // copy the md5 checksum for verification
        memcpy(chunks[i + NUM_CHUNK * 3].md5, chunks[i].md5, MD5_DIGEST_LENGTH);
        if (c[i % NUM_CONTAINER]->getChunk(chunks[i + NUM_CHUNK * 3]) == false || memcmp(chunks[i].data, chunks[i + NUM_CHUNK * 3].data, chunkSize) != 0) {
            printf("Chunk content mismatch\n");
            okay = false;
            break;
        }
        printf("> Move chunk %s to %s\n", chunks[i + NUM_CHUNK * 2].getChunkName().c_str(), chunks[i + NUM_CHUNK * 3].getChunkName().c_str());
    }

    // delete chunks
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        if (c[i % NUM_CONTAINER]->deleteChunk(chunks[i + NUM_CHUNK]) == false) {
            printf("Failed to delete chunk\n");
            okay = false;
            break;
        } else {
            printf("> Delete chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
        }
        if (c[i % NUM_CONTAINER]->deleteChunk(chunks[i + NUM_CHUNK * 3]) == false) {
            printf("Failed to delete chunk\n");
            okay = false;
            break;
        } else {
            printf("> Delete chunk %s\n", chunks[i + NUM_CHUNK * 3].getChunkName().c_str());
        }
    }

    // check chunks (detect non-existing chunks)
    for (int i = 0; i < NUM_CHUNK && okay; i++) {
        if (c[i % NUM_CONTAINER]->hasChunk(chunks[i + NUM_CHUNK]) == true) {
            printf("Failed to detect non-existing chunk %s\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
            okay = false;
            break;
        }
        printf("> Check chunk %s no longer exists\n", chunks[i + NUM_CHUNK].getChunkName().c_str());
    }

    // release resources 
    for (int i = 0; i < NUM_CONTAINER; i++) {
        delete c[i];
    }

    aos_http_io_deinitialize();
    Aws::ShutdownAPI(options);

    printf("End of Container Test\n");
    printf("====================\n");

    // 0 if okay is true, 1 otherwise
    return !okay;
}
