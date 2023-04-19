// SPDX-License-Identifier: Apache-2.0

#ifndef __CONTAINER_MANAGER_HH__
#define __CONTAINER_MANAGER_HH__

#include <map>

#include "../ds/chunk.hh"
#include "container/container.hh"

class ContainerManager {
public:
    ContainerManager();
    ~ContainerManager();

    /** 
     * Store the chunks to the corresponding containers
     *
     * @param[in] containerId       ids of containers to store the corresponding chunks
     * @param[in] chunks            chunks to store in containers with the corresponding ids;
     *                              each of them should have all fields filled
     * @param[in] numChunks         number of chunks to store
     *
     * @return if all chunks are successfully stored
     **/
    bool putChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Get the chunks from the coresponding containers
     *
     * @param[in]  containerId      ids of containers storing the corresponding chunks
     * @param[out] chunks           list of chunks to store data obtained from containers with the corresponding ids;
     *                              each of them should have all fields filled, except Chunk::data and Chunk::size, and Chunk::freeData should be set to true;
     *                              Chunk::data, Chunk::size would be filled if get is successful
     * @param[in]  numChunks        number of chunks to get 
     *
     * @return if all chunks are successfully get
     **/
    bool getChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Delete the chunks from the coresponding containers
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in] chunks             chunks to delete from containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData
     * @param[in] numChunks          number of chunks to delete
     *
     * @return if all chunks are successfully deleted
     **/
    bool deleteChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Copy the chunks in the coresponding containers
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in] srcChunks          chunks to copy in containers with the corresponding ids
     * @param[in,out] dstChunks      destinated chunks in containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     *                               Chunk::size would be filled if copy is successful
     * @param[in] numChunks          number of chunks to copy 
     *
     * @return if all chunks are successfully copied
     **/
    bool copyChunks(int containerId[], Chunk srcChunks[], Chunk dstChunks[], int numChunks);

    /**
     * Move (Rename) the chunks in the coresponding containers
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in] srcChunks          chunks to move in containers with the corresponding ids
     * @param[in,out] dstChunks      destinated chunks in containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data, Chunk::size, and Chunk::freeData;
     *                               Chunk::size would be filled if move is successful
     * @param[in] numChunks          number of chunks to move
     *
     * @return if all chunks are successfully moved 
     **/
    bool moveChunks(int containerId[], Chunk srcChunks[], Chunk dstChunks[], int numChunks);

    /**
     * Check if the chunks exist and the sizes match
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in] chunks             chunks to check in containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data and Chunk::freeData;
     * @param[in] numChunks          number of chunks to check
     *
     * @return if all chunks exists with sizes matched
     **/
    bool hasChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Check if the chunks checksum matches
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in,out] chunks         chunks to verify in containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data and Chunk::freeData;
     * @param[in] numChunks          number of chunks to check
     *
     * @return number of corrupted chunks
     **/
    int verifyChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Revert the chunks
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks
     * @param[in] chunks             chunks to check in containers with the corresponding ids;
     *                               each of them should have all fields filled, except Chunk::data and Chunk::freeData;
     * @param[in] numChunks          number of chunks to check
     *
     * @return if all chunks are successfully reverted
     **/
    bool revertChunks(int containerId[], Chunk chunks[], int numChunks);

    /**
     * Generate a partial encoded chunk from chunks in the coresponding containers
     *
     * @param[in] containerId        ids of containers storing the corresponding chunks to encode
     * @param[in] chunks             list of ids of chunks to encode;
     *                               each of them should have all fields filled, except Chunk::data and Chunk::freeData;
     * @param[in] numChunks          number of chunks to encode
     * @param[in] matrix             matrix for encoding chunks
     *
     * @return the partial encoded chunk; Chunk::size > 0 if success and fail otherwise
     * @remark data in the partial encoded chunk is set not to be freed by default
     **/
    Chunk getEncodedChunks(int containerId[], Chunk chunks[], int numChunks, unsigned char matrix[]);

    /**
     * Tell the number of containers managed
     *
     * @return number of containers managed
     **/
    int getNumContainers();

    /**
     * Tell the ids of containers managed
     *
     * @param[out] containerIds      pre-allocated list to store ids of containers managed
     * @remark the size of lists should be at least the one returned by getNumContainers()
     **/
    void getContainerIds(int containerIds[]);

    /**
     * Tell the type of containers managed
     *
     * @param[out] containerType     pre-allocated list to store type of containers managed
     * @remark the size of lists should be at least the one returned by getNumContainers()
     **/
    void getContainerType(unsigned char containerType[]);

    /**
     * Tell the usage of containers managed
     *
     * @param[out] containerUsage    pre-allocated list to store the usage of containers managed
     * @param[out] containerCapcity  pre-allocated list to store usage of containers managed
     * @remark the size of lists should be at least the one returned by getNumContainers()
     **/
    void getContainerUsage(unsigned long int containerUsage[], unsigned long int containerCapacity[]);

private:
    int _numContainers;                              /**< number of containers */
    std::map<int, Container*> _containers;           /**< mapping of containers id to container */
    Container *_containerPtrs[MAX_NUM_CONTAINERS];   /**< list of containers */
};

#endif // define __CONTAINER_MANAGER_HH__
