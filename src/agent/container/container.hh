// SPDX-License-Identifier: Apache-2.0

#ifndef __CONTAINER_HH__
#define __CONTAINER_HH__

#include <pthread.h>
#include "../../common/define.hh"
#include "../../ds/chunk.hh"

class Container {
public:
    Container(int id, unsigned long int capacity);

    virtual ~Container();

    /**
     * Store/Overwrite a chunk in the container
     *
     * @param[in,out] chunk            chunk to store/overwrite;
     *                                 should have all fields filled
     *
     * @return whether the chunk is successful stored
     **/
    virtual bool putChunk(Chunk &chunk) = 0;

    /**
     * Get a chunk from the container
     *
     * @param[in,out] chunk            chunk to get;
     *                                 should have all fields filled, except Chunk::data and Chunk::size, and Chunk::freeData should be set to true;
     *                                 Chunk::data, Chunk::size would be filled if get is successful
     * @param[in] skipVerify           whether to manually skip checksum verification
     *
     * @return whether the chunk is successful get
     **/
    virtual bool getChunk(Chunk &chunk, bool skipVerification = false) = 0;

    /**
     * Delete a chunk from the container
     *
     * @param[in] chunk                chunk to delete;
     *                                 should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     *
     * @return whether the chunk no longer exists in the container
     **/
    virtual bool deleteChunk(const Chunk &chunk) = 0;

    /**
     * Copy a chunk in the container
     * 
     * @param[in] src                  chunk to copy;
     *                                 should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     * @param[in,out] dst              copied chunk, containing the chunk id; size will be updated upon success;
     *                                 should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     *                                 Chunk::size would be filled if copy is successful
     * 
     * @return whether the chunk is successfully copied
     **/
    virtual bool copyChunk(const Chunk &src, Chunk &dst) = 0;

    /**
     * Move a chunk in the container
     * 
     * @param[in] src                  chunk to move;
     *                                 should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     * @param[in,out] dst              moved chunk, containing the chunk id; size will be updated upon success;
     *                                 should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     *                                 Chunk::size would be filled if move is successful
     * 
     * @return whether the chunk is successfully moved
     **/
    virtual bool moveChunk(const Chunk &src, Chunk &dst) = 0;

    /**
     * Check if a chunk with given size exists in the container
     *
     * @param[in] chunk                chunk to check
     *                                 should have all fields filled, except Chunk::data and Chunk::freeData;
     *
     * @return whether the chunk with the given size exists
     **/
    virtual bool hasChunk(const Chunk &chunk) = 0;

    /**
     * Revert a chunk in the container
     *
     * @param[in] chunk                chunk to revert
     *                                 should have all fields filled, except Chunk::size, Chunk::data and Chunk::freeData;
     *
     * @return whether the chunk is reverted
     **/
    virtual bool revertChunk(const Chunk &chunk) = 0;

    /**
     * Verify the checksums of chunks
     *
     * @param[in] chunk                chunk to check
     *                                 should have all fields filled, except Chunk::size, Chunk::data and Chunk::freeData;
     *
     * @return whether the chunk is good
     **/
    virtual bool verifyChunk(const Chunk &chunk) = 0;

    /**
     * Update the container usage
     **/
    virtual void updateUsage() = 0;

    /**
     * Update the container usage in background
     **/
    void bgUpdateUsage();

    /**
     * Obtain the container id
     *
     * @return container id
     **/
    int getId();

    /**
     * Obtain the container usage
     *
     * @param[in] updateNow            whether to update the current usage before reporting the value
     * @return container usage in bytes
     **/
    unsigned long int getUsage(bool updateNow = false);

    /**
     * Obtain the container capacity
     *
     * @return container capacity in bytes
     **/
    unsigned long int getCapacity();

    /**
     * Obtain the container usage and capacity
     *
     * @param[out] usage               container usage in bytes
     * @param[out] capacity            container capacity in bytes
     * @param[in] updateNow            whether to update the current usage before reporting the value
     **/
    void getUsageAndCapacity(unsigned long int &usage, unsigned long int &capacity, bool updateNow = false);

protected:
    int _id;                           /**< container id */
    unsigned long int _usage;          /**< container usage */
    unsigned long int _capacity;       /**< container capacity */
    bool _running;                     /**< whether the container is running */
    struct {
        pthread_t t;                   /**< background thread for usage update */
        pthread_cond_t cond;           /**< condition for background update */
        pthread_mutex_t lock;          /**< condition for background update */
    } _usageUpdate;

    /**
     * Background container usage update function 
     *
     * @param[in] arg                  container instance to perform update
     **/
    static void* backgroundUsageUpdate(void *arg);

};

#endif // define __CONTAINER_HH__
