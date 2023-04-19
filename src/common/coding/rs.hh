// SPDX-License-Identifier: Apache-2.0

#ifndef __RS_CODE_HH__
#define __RS_CODE_HH__

#include <stdint.h> // uint8_t
#include "coding.hh"
#include "../config.hh"

class RSCode : public Coding {
public:

    RSCode(CodingOptions options);
    ~RSCode() {}
    /**
     * see Coding::getNumDataChunks()
     **/
    num_t getNumDataChunks();

    /**
     * see Coding::getNumCodeChunks()
     **/
    num_t getNumCodeChunks();

    /**
     * see Coding::getNumChunks()
     **/
    num_t getNumChunks();

    /**
     * see Coding::getNumChunksPerNode()
     **/
    num_t getNumChunksPerNode();

    /**
     * see Coding::getCodingStateSize()
     **/
    length_t getCodingStateSize();

    /**
     * see Coding::encode()
     * 
     * @remark coding state is ignored for RS
     **/
    bool encode(data_t *data, length_t dataSize, std::vector<Chunk> &stripe, data_t **codingState);

    /**
     * see Coding::decode()
     * 
     * @remark coding state is ignored for RS
     **/
    bool decode(std::vector<Chunk> &inputChunks, data_t **decodedData, length_t &decodedSize, DecodingPlan &plan, data_t *codingState, bool isRepair = false, std::vector<chunk_id_t> repairTargets = std::vector<chunk_id_t>());

    /**
     * see Coding::preDecode()
     * 
     * @remark coding state is ignored for RS
     **/
    bool preDecode(const std::vector<chunk_id_t> &failedNodeIdx, DecodingPlan &plan, data_t *codingState, bool isRepair = false);

    /**
     * see Coding::getChunkSize()
     **/
    length_t getChunkSize(length_t dataSize);


private:

    /**
     * Final decoding step for repair using CAR (xor all chunks)
     *
     * @param[in] inputp                  array of input chunk buffer pointers
     * @param[in] numInputChunks          number of input chunks
     * @param[in] chunkSize               chunk size
     * @param[out] decodep                array of decoded chunk buffer pointers
     *
     * @return true if decoding is successful, false otherwise
     **/
    bool carRepairFinalize(unsigned char *inputp[], num_t numInputChunks, length_t chunkSize, unsigned char *decodep[]);

    uint8_t _encodeMatrix[CODING_MAX_N * CODING_MAX_N];
    uint8_t _gftbl[CODING_MAX_N * CODING_MAX_N * 32];

};

#endif // define __RS_HH__
