// SPDX-License-Identifier: Apache-2.0

#ifndef __CODING_HH__
#define __CODING_HH__

#include <string>
#include <vector>
#include "../define.hh"
#include "../../ds/chunk.hh"
#include "coding_options.hh"
#include "decoding_plan.hh"

#define CODING_MAX_N  ( 128 )

class Coding {
public:
    virtual ~Coding() {}

    /**
     * Tell the name of the coding scheme (optional)
     *
     * @return name of the coding scheme
     **/
    std::string getName() const {
        return _name;
    }


    // ---------------------------------- //
    //  Basic codoing scheme information  //
    // ---------------------------------- //

    /**
     * Tell the coding parameter n
     *
     * @return value of coding parameter n
     **/
    coding_param_t getN() {
        return _options.getN();
    }

    /**
     * Tell the coding parameter k
     *
     * @return value of coding parameter k
     **/
    coding_param_t getK() {
        return _options.getK();
    }

    /**
     * Tell the number of data chunks
     * 
     * @return the number of data chunks
     **/
    virtual num_t getNumDataChunks() = 0;

    /**
     * Tell the number of code chunks
     * 
     * @return the number of code chunks
     **/
    virtual num_t getNumCodeChunks() = 0;

    /**
     * Tell the number of chunks in a stripe
     * 
     * @return the number of chunks in a stripe
     **/
    virtual num_t getNumChunks() = 0;

    /**
     * Tell the number of chunks to store in each node
     * 
     * @return number of chunks to store in each node
     **/
    virtual num_t getNumChunksPerNode() = 0;


    // -------------------------------------------------------------------- //
    //  Extra coding scheme information on additional information to store  //
    // -------------------------------------------------------------------- //

    /**
     * Tell the size of state (in bytes)
     *
     * @return size of state
     **/
    virtual length_t getCodingStateSize() = 0;


    // ----------------------------- //
    //  Coding operation properties  //
    // ----------------------------- //

    /**
     * Tell the size of extra data added to data buffer upon encoding
     *
     * @return size of extra data added in bytes
     **/
    length_t getExtraDataSize() {
        return _extraDataSize;
    }

    /**
     * Tell whether data may be modified upon encoding
     *
     * @return whether data may be modified
     **/
    bool modifyDataBuffer() {
        return _modifyDataBuffer;
    }

    /**
     * Tell whether only code chunks need to be stored
     * 
     * @return whether only code chunks need to be stored
     **/ 
    bool storeCodeChunksOnly() {
        return _storeCodeChunksOnly;
    }


    // ------------------------ //
    //  Pre-coding preparation  //
    // ------------------------ //

    /**
     * Tell the size of chunks based on the input data size
     * 
     * @return size of chunks based on the input data size
     **/
    virtual length_t getChunkSize(length_t dataSize) = 0;

    /**
     * Get list of chunks to retrieve for decode/repair
     *
     * @param[in] failedChunkIdx         ids of failed chunks
     * @param[out] plan                  decoding plan; the coding implementation should set the ids of the set of input chunks, and the minimal number of chunks to retrieve for decoding. Optionally, the implementation can set the repair matrix for CAR repair
     * @param[in,out] codingState        coding state; caller should pass in the last obtained/updated coding state
     * @param[in] isRepair               whether the decoding is for repair; the coding implementation should give the plan for decoding the data chunks if this is set to 'false', and that for decoding the failed chunks if this is set to 'true'
     *
     * @return if a repair is possible
     **/
    virtual bool preDecode(const std::vector<chunk_id_t> &failedChunkIdx, DecodingPlan &plan, data_t *codingState, bool isRepair = false) = 0;


    // ------------------- //
    //  Coding operations  //
    // ------------------- //

    /**
     * Encode data chunks into code chunks
     *
     * @param[in] data                   pointer to buffered data to encode
     * @param[in] dataSize               size of the buffered data
     * @param[out] stripe                chunks in the stripe; the coding implementation should set the chunk id (using Chunk::setChunkId()) and data for all chunks
     * @param[out] codingState           a pointer to the placeholder of coding state; the coding state, if any, will be allocated by the function
     *
     * @return if data is successfully encoded 
     **/
    virtual bool encode(data_t *data, length_t dataSize, std::vector<Chunk> &stripe, data_t **codingState) = 0;

    /**
     * Decode data chunks using input chunks
     *
     * @param[in] inputChunks            input chunk buffers
     * @param[out] decodedData           pointer placeholder for storing decoded data, buffer will be allocated by the function; if the pointer is null, new buffer is allocated; otherwise, the function reuse the provided buffer (i.e., caller should ensure the buffer is sufficient large to hold the decoded data
     * @param[out] decodedSize           size of the decoded data
     * @param[in] plan                   decoding plan obtained from preDecode()
     * @param[in,out] codingState        coding state; caller should pass in the last obtained/updated coding state
     * @param[in] isRepair               whether the decoding is for repair. the coding implementation should decode the data chunks if this is set to 'false' and decode the failed chunks if this is set to 'true' 
     * @param[in] repairTargets          ids of chunk to repair; if none is specified, repair all chunks absent in the input
     *
     * @return if data is successfully decoded 
     **/
    virtual bool decode(std::vector<Chunk> &inputChunks, data_t **decodedData, length_t &decodedSize, DecodingPlan &plan, data_t *codingState, bool isRepair = false, std::vector<chunk_id_t> repairTargets = std::vector<chunk_id_t>()) = 0;


protected:
    Coding() {
        _extraDataSize = 0;
        _modifyDataBuffer = false;
        _storeCodeChunksOnly = false;
    }

    bool _storeCodeChunksOnly;
    bool _modifyDataBuffer;
    length_t _extraDataSize;


    CodingOptions _options;
    std::string _name;
};

#endif // define __CODING_HH__
