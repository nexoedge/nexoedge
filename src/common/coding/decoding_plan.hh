// SPDX-License-Identifier: Apache-2.0

#ifndef __DECODING_PLAN_HH__
#define __DECODING_PLAN_HH__

#include <stdint.h>
#include "../../common/define.hh"
#include "../../ds/byte_buffer.hh"

class DecodingPlan {
public:

    DecodingPlan() {
        reset();
    }

    ~DecodingPlan() {
        release();
    }

    void release() {
        releaseRepairMatrix();
        releaseInputChunks();
    }

    // --------------- //
    //  Repair matrix  //
    // --------------- //

    bool allocateRepairMatrix(length_t size) {
        return _repairMatrix.allocate(size);
    }

    data_t *getRepairMatrix() {
        return _repairMatrix.data();
    }

    length_t getRepairMatrixSize() {
        return _repairMatrix.size();
    }

    void releaseRepairMatrix() {
        _repairMatrix.release();
    }

    // ----------------- //
    //  Input chunk ids  //
    // ----------------- //

    void addInputChunkId(chunk_id_t chunkId) {
        _inputChunkIds.push_back(chunkId);
    }

    std::vector<chunk_id_t> getInputChunkIds() const {
        return _inputChunkIds;
    }

    size_t getNumInputChunks() const {
        return _inputChunkIds.size();
    }

    size_t getMinNumInputChunks() const {
        return _minNumChunksToRetrieve;
    }

    void releaseInputChunks() {
        _inputChunkIds.clear();
        _minNumChunksToRetrieve = 0;
    }

    bool setMinNumInputChunks(num_t num) {
        if (num > getNumInputChunks()) {
            return false;
        }

        _minNumChunksToRetrieve = num;
        return true;
    }
    
private:

    void reset() {
        resetRepairMatrix();
        resetInputChunks();
    }

    void resetRepairMatrix() {
        _repairMatrix.reset();
    }

    void resetInputChunks() {
        _inputChunkIds.clear();
    }

    ByteBuffer _repairMatrix;                  /**< repair matrix */
    std::vector<chunk_id_t> _inputChunkIds;    /**< ids of input chunks */
    num_t _minNumChunksToRetrieve;             /**< minimum number of chunks to retrieve */
    
};

#endif //define __DECODING_PLAN_HH__

