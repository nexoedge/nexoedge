// SPDX-License-Identifier: Apache-2.0

#ifndef __CHUNK_MANAGER_HH__
#define __CHUNK_MANAGER_HH__

#include <atomic>
#include <string>
#include <map>

#include <zmq.hpp>

#include "bg_chunk_handler.hh"
#include "io.hh"
#include "metastore/metastore.hh"
#include "../common/config.hh"
#include "../common/coding/coding.hh"
#include "../ds/file.hh"
#include "../ds/storage_class.hh"

class ChunkManager {
public:
    ChunkManager(std::map<int, std::string> *containerToAgentMap, ProxyIO *io, BgChunkHandler *handler, MetaStore *metastore = nullptr);
    ~ChunkManager();
    
    /**
     * Write a stripe in the file to storage backend
     *
     * @param[in,out] file          file containing the stripe to write
     * @param[in] spareContainers   spare containers for writing chunks
     * @param[in] numSpare          number of spare containers for writing chunks
     * @param[in] alignDataBuf      whether data buffer needs internal alignment, caller should adjust it manually to the size returned by ChunkManager::getDataStripeSize() before disabling this
     * @param[in] isOverwrite       whether the file stripes are overwritten
     * @param[in] withEncode        whether to encode the file(stripe)
     *
     * @return whether the file stripe is successfully written
     **/
    bool writeFileStripe(File &file, int spareContainers[], int numSpare, bool alignDataBuf = true, bool isOverwrite = false, bool withEncode = true);

    /**
     * Encode a file
     * 
     * @param[in,out] file          file(stripe) to write
     * @param[in] spareContainers   spare containers for writing chunks
     * @param[in] numSpare          number of spare containers for writing chunks
     * @param[in] alignDataBuf      whether data buffer needs internal alignment, caller should adjust it manually to the size returned by ChunkManager::getDataStripeSize() before disabling this
     * @param[in] codebuf           optional buffer to hold the coded chunks, caller should pre-allocate it to the size of number of coded chunks * chunk size
     *
     * @return whether the file is successfully encoded
    */
    bool encodeFile(File &file, int spareContainers[], int numSpare, bool alignDataBuf = true, unsigned char *codebuf = 0);


    /**
     * Read a stripe in the file from storage backend  (sequential proxy)
     *
     * @param[in,out] file          file containing the stripe to read
     * @param[in] chunkIndicator    list of indicators for chunk liveness (true means alive, false means failed), its size is equal to the number of chunks in the file
     *
     * @return whether the file stripe is successfully read
     **/
    bool readFileStripe(File &file, bool chunkIndicator[]);

    /**
     * Read a file from storage backend
     *
     * @param[in,out] file          file to read
     * @param[in] chunkIndicator    list of indicators for failed chunks (1 means failed), its size is equal to the number of chunks in the file
     * @param[out] nodeIndicesOut   pointer of list of chunk id that read successfully (allocated internally)
     * @param[out] eventsOut        pointer of list of chunk events with chunk embedded to be decoded (allocated internally)
     * @param[in] withDecode        indicator for decoding the stripe inside this function (noted that if withDecode is false, 
     *                              nodeIndicesOut & eventsOut must be provided)
     * @param[out] plan             decoding plan
     *
     * @return whether the file is successfully read
     **/
    bool readFile(File &file, bool chunkIndicator[], int **nodeIndicesOut, ChunkEvent **eventsOut, bool withDecode, DecodingPlan &plan);

    /**
     * Decode a file
     *
     * @param[in,out] file          file and data to decode
     * @param[in] nodeIndices       list of chunk id that read successfully
     * @param[in] events            list of chunk events with chunk embedded to be decoded
     * @param[in] plan              decoding plan
     *
     * @return whether the file is successfully decoded
     **/
    bool decodeFile(File &file, int *nodeIndices, ChunkEvent *events, DecodingPlan &plan);

    /**
     * Delete a file from storage backend
     *
     * @param[in,out] file          file to delete
     * @param[in] chunkIndicator    list of indicators for chunk liveness (true means alive, false means failed), its size is equal to the number of chunks in the file
     *
     * @return whether the file is successfully deleted
     **/
    bool deleteFile(const File &file, bool chunkIndicator[]);

    /**
     * Copy a file in storage backend
     *
     * @param[in] srcFile          file to copy, containing all metadata 
     * @param[in,out] dstFile      copied file, containing the name and file id
     * @param[out] start           starting stripe copied if successful
     * @param[out] end             ending stripe (exclusive) copied if successful
     *
     * @return whether the file is successfully copied 
     **/
    bool copyFile(File &srcFile, File &dstFile, int *start = 0, int *end = 0);

    /**
     * Move a file in storage backend
     *
     * @param[in] srcFile          file to move, containing all metadata 
     * @param[in,out] dstFile      moved file, containing the name and file id
     *
     * @return whether the file is successfully moved
     **/
    bool moveFile(File &srcFile, File &dstFile);

    /**
     * Repair a file from storage backend
     *
     * @param[in,out] file file to repair 
     * @param[in] chunkIndicator    list of indicators for chunk liveness (true means alive, false means failed), its size is equal to the number of chunks in the file
     * @param[in] spareContainers   list of container ids for storing repaired chunks, its size is at least the number of chunks failed
     * @param[in] chunkGroups       list of chunk groups of alive chunks, see ProxyCoordinator::findChunkGroups() for its format
     * @param[in] numChunkGroups    number of chunk groups
     *
     * @return whether the file is successfully repair
     **/
    bool repairFile(File &file, bool *chunkIndicator, int spareContainers[], int chunkGroups[], int numChunkGroups);

    /**
     * Check chunk status of a file in storage backend
     *
     * @param[in] file               file to check
     * @param[out] chunkIndicator    list of indicators for failed chunks (1 means failed), its size is equal to the number of chunks in the file
     *
     * @return number of failed chunks
     **/
    int checkFile(File &file, bool chunkIndicator[]);

    /**
     * Revert modification to a file from storage backend
     *
     * @param[in,out] file          file to revert
     * @param[in] chunkIndicator    list of indicators for failed chunks (1 means failed), its size is equal to the number of chunks in the file
     *
     * @return whether the file is successfully reverted
     **/
    bool revertFile(const File &file, bool chunkIndicator[] = NULL);

    /**
     * Check chunk checksum (of a file) in storage backend
     *
     * @param[in] file               file containing chunks to check
     * @param[out] chunkIndicator    list of indicators for failed chunks (1 means failed), its size is equal to the number of chunks in the file
     *
     * @return number of failed chunks if succeeded, -1 otherwise
     **/
    int verifyFileChecksums(File &file, bool chunkIndicator[]);

    /**
     * Get the number of containers required for storing a file under a specific coding scheme
     *
     * @param[in] storageClass      storage class to apply on the file
     *
     * @return the number of containers required
     **/
    int getNumRequiredContainers(std::string storageClass);

    /**
     * Get the number of containers required for storing a file under a specific coding scheme
     *
     * @param[in] coding            coding scheme of the file
     * @param[in] n                 coding parameter n
     * @param[in] k                 coding parameter k
     *
     * @return the number of containers required
     **/
    int getNumRequiredContainers(int codingScheme, int n, int k);

    /**
     * Get the minimum number of containers required for storing a file under a specific coding scheme
     *
     * @param[in] storageClass      storage class to apply on the file
     *
     * @return the minimum number of containers required
     **/
    int getMinNumRequiredContainers(std::string storageClass);

    /**
     * Get the number of chunks per container under a specific coding scheme
     *
     * @param[in] storageClass      storage class to apply on the file
     *
     * @return the number of chunks per container
     **/
    int getNumChunksPerContainer(std::string storageClass);

    /**
     * Get the number of chunks per container under a specific coding scheme
     *
     * @param[in] codingScheme      coding scheme of the file
     * @param[in] n                 coding parameter n
     * @param[in] k                 coding parameter k
     *
     * @return the number of chunks per container
     **/
    int getNumChunksPerContainer(int codingScheme, int n, int k);

    /**
     * Get the max stripe size, using coding configuration
     *
     * @param[in] codingScheme      coding scheme of the file
     * @param[in] n                 coding parameter n
     * @param[in] k                 coding parameter k
     * @param[in] chunkSize         chunk size
     * @param[in] isFullChunkSize   whether the chunk size is full
     *
     * @return the max. size of data stripe allowed
     **/
    unsigned long int getMaxDataSizePerStripe(int codingScheme, int n, int k, int chunkSize, bool isFullChunkSize = true);

    /**
     * Get the max stripe size, using storage class
     *
     * @param[in] storageClass      storage class to apply on the file
     *
     * @return the max. size of data stripe allowed
     **/
    unsigned long int getMaxDataSizePerStripe(std::string storageClass);

    /**
     * Get the data stripe size based on data size
     *
     * @param[in] codingScheme      coding scheme of the file
     * @param[in] n                 coding parameter n
     * @param[in] k                 coding parameter k
     * @param[in] size              data size
     *
     * @return the size of data stripe
     **/
    unsigned long int getDataStripeSize(int codingScheme, int n, int k, unsigned long int size);

    /**
     * Whether the data buffer will be modified due to internal padding
     *
     * @param[in] storageClass      storage class to apply on the file
     *
     * @return whether the data buffer will be modified
     **/
    bool willModifyDataBuffer(std::string storageClass);

    /**
     * Return the extra size of data to added by coding scheme
     * 
     * @param[in] storageClass      storage class to apply on the file
     * 
     * @return size of extra data added in bytes
     **/
    unsigned long int getPerStripeExtraDataSize(std::string storageClass);

    /**
     * Copy coding metadata of a storage class
     *
     * @param[in] className         storage class to apply on the file
     * @param[in,out] codingMeta    coding metadata holder
     *
     * @return whether the storage class exists
     **/
    bool setCodingMeta(std::string className, CodingMeta &codingMeta);

private:

    /**
     * Operate on the alive chunks of a file in the storage backend
     *
     * @param[in,out] file          file to operate on
     * @param[in] chunkIndicator    list of indicators for chunk liveness (true means alive, false means failed), its size is equal to the number of chunks in the file
     *
     * @return whether the operation is successful
     **/
    bool operateOnAliveChunks(const File &file, bool chunkIndicator[], Opcode reqOp, Opcode expectedOpRep);

    /**
     * Access chunks stored in containers 
     *
     * @param[in,out] events        list of chunk events for sending chunk requests and holding the response, its size is a double of the number of chunks
     * @param[in] file              file that contains:
     *                              1. a list of container ids for the chunks to access, its size is at least the number of chunks to access
     *                              2. list of chunks (to access), its size is at least the number of chunks to access
     *                              3. stripe id for benchmark
     *                              4. request id for benchmark
     * @param[in] numChunks         number of chunks to access 
     * @param[in] reqOp             opcode for the request
     * @param[in] expectedOpRep     expected opcode for the response
     * @param[in] numChunksPerNode  number of chunks per node
     * @param[in] chunkIndices      optional list of indices to indicate specific chunks to access in the chunkList, its size is equal to the number of chunks to access
     * @param[in] chunkIndicesSize  optional size of the chunk indices provided
     * @param[out] chunkIndicator   optional list of indicator to indicate liveness of chunks (true means alive, false means failed)
     *
     * @return whether the access to all chunks are successful
     **/
    bool accessChunks(ChunkEvent events[], const File &f, int numChunks, Opcode reqOp, Opcode expectedOp, int numChunksPerNode, int *chunkIndices = 0, int chunkIndicesSize = -1, bool *chunkIndicator = 0);

    /**
     * Access chunks in a group in partial encoded form
     *
     * @param[in,out] events        list of chunk events for sending chunk requests and holding the response, its size is a double of the number of chunk groups
     * @param[in] containerIds      list of container ids for the chunks to access, its size is at least the number of chunks to access
     * @param[in] numChunks         number of chunks to access 
     * @param[in] chunkGroups       list of chunk groups to access, see ProxyCoordinator::findChunkGroups() for its format
     * @param[in] numChunkGroups    number of chunk groups to access
     * @param[in] namespaceId       file namepsace id
     * @param[in] fuuid             file uuid
     * @param[in] matrix            rows of coefficients for encoding chunks, the rows should be in the order of chunkGroups provided and its length is equal to numChunks
     * @param[in] chunkIdOffset     chunk id offset for files with multiple stripes
     * @return whether the access to all chunk groups are successful
     **/
    bool accessGroupedChunks(ChunkEvent events[], int containerIds[], int numChunks, int chunkGroups[], int numChunkGroups, unsigned char  namepsaceId, boost::uuids::uuid fuuid, std::string matrix, int chunkIdOffset);

    /**
     * Modify a file in storage backend
     *
     * @param[in] srcFile          file to modify, containing all metadata 
     * @param[in,out] dstFile      modified file, containing the name and file id
     * @param[in] isCopy           whether it is a copy operation (otherwise a move operation)
     * @param[out] start           starting stripe copied if successful
     * @param[out] end             ending stripe (exclusive) copied if successful
     *
     * @return whether the file is successfully copied 
     **/
    bool fullFileModify(File &srcFile, File &dstFile, bool isCopy, int *start = 0, int *end = 0);

    /**
     * Check if the coding scheme is valid
     * 
     * @param[in] coding            coding scheme
     * 
     * @return whether the coding scheme is valid
     **/
    bool isValidCoding(int coding);
 
    /**
     * Obtain the coding scheme by class name search
     * 
     * @param[in] className name of the storage class
     * 
     * @return coding scheme if class exists, CodingScheme::UNKNOWN_CODE otherwise
     **/
    int getCodingScheme(std::string className);

    /**
     * Get the max stripe size, using coding intance
     *
     * @param[in] coding            coding instance
     * @param[in] maxChunkSize      max chunk size allowed
     *
     * @return the max. size of data stripe allowed
     **/
    unsigned long int getMaxDataSizePerStripe(Coding *coding, int maxChunkSize);

    /**
     * Get the number of chunks per container under a specific coding scheme
     *
     * @param[in] coding            coding instance
     *
     * @return the number of chunks per container
     **/
    int getNumChunksPerContainer(Coding *coding);

    /**
     * Get the number of containers required for storing a file under a specific coding scheme
     *
     * @param[in] coding            coding instance
     *
     * @return the number of containers required
     **/
    int getNumRequiredContainers(Coding *coding);

    /**
     * Obtain the coding instance by class name search
     * 
     * @param[in] className name of the storage class
     * 
     * @return pointer to coding instance if exists, NULL otherwise
     **/
    Coding* getCodingInstance(std::string className);

    /**
     * Obtain the coding instance by coding scheme and parameters search
     * 
     * @param[in] codingScheme coding scheme
     * @param[in] n coding parameter n
     * @param[in] k coding parameter k
     * 
     * @return pointer to coding instance if exists, NULL otherwise
     **/
    Coding* getCodingInstance(int codingScheme, int n, int k);

    /**
     * Obtain the coding instance key by coding scheme and parameters search
     * 
     * @param[in] codingScheme coding scheme
     * @param[in] n coding parameter n
     * @param[in] k coding parameter k
     * 
     * @return key of the coding instance
     **/
    std::string genCodingInstanceKey(int codingScheme, int n, int k);

    std::atomic<int> _eventCount;                              /**< evnet id counter */

    std::map<std::string, StorageClass*> _storageClasses;      /**< storage classes mapping */
    std::map<std::string, Coding*> _codings;                   /**< coding instances mapping */
    std::mutex _codingsLock;                                   /**< coding instance mapping lock */
    ProxyIO *_io;                                              /**< IO module */
    BgChunkHandler *_bgChunkHandler;                           /**< background chunk handler */
    MetaStore*_metastore;                                      /**< metastore */

    std::map<int, std::string> *_containerToAgentMap;          /**< map of containers [container id]->agent socket*/

    
};

#endif // define __CHUNK_MANAGER_HH__
