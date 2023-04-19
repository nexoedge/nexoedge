// SPDX-License-Identifier: Apache-2.0

#ifndef __FS_CONTAINER_HH__
#define __FS_CONTAINER_HH__

#include <string>
#include <pthread.h>
#include <linux/limits.h>

#include "container.hh"
#include "../../ds/chunk.hh"

class FsContainer : public Container {
public:
    FsContainer(int id, const char* dir, unsigned long int capacity);
    ~FsContainer();

    /**
     * See Container::putChunk()
     **/
    bool putChunk(Chunk &chunk);

    /**
     * See Container::getChunk()
     **/
    bool getChunk(Chunk &chunk, bool skipVerification = false);

    /**
     * See Container::deleteChunk()
     **/
    bool deleteChunk(const Chunk &chunk);

    /**
     * See Container::copyChunk()
     **/
    bool copyChunk(const Chunk &chunk, Chunk &dst);

    /**
     * See Container::moveChunk()
     **/
    bool moveChunk(const Chunk &src, Chunk &dst);

    /**
     * See Container::hasChunk()
     **/
    bool hasChunk(const Chunk &chunk);

    /**
     * See Container::revertChunk()
     **/
    bool revertChunk(const Chunk &chunk);

    /**
     * See Container::verifyChunk()
     **/
    bool verifyChunk(const Chunk &chunk);

    /**
     * See Container::updateUsage()
     **/
    void updateUsage();

private:
    char _dir[PATH_MAX]; /**< container folder path */
    struct {
        pthread_t th; /**< background chunk cleanup thread */
        pthread_cond_t cond;
        pthread_mutex_t lock;
    } _chunkCleanUp;

    bool _running; /**< whether the container is "running" */

    /**
     * Get the path of chunk file
     *
     * @param[out] fpath      path of the chunk file
     * @param[in]  chunkName  name of the chunk
     *
     * @return whether the path of the chunk file can be generated
     **/
    bool getChunkPath(char *fpath, std::string chunkName);

    void getOldChunkPath(std::string &ofpath, char *fpath, const char *ctime);

    bool getTotalSize(unsigned long int &total, bool needsLock = true);

    bool getChunkInternal(Chunk &chunk, bool skipVerification = false);

    bool readChunkFile(const char fpath[], Chunk &chunk);

    static bool isOldChunks(const char *fpath);

    static void *cleanUpOldChunks(void *arg);
};

#endif // define __FS_CONTAINER_HH__
