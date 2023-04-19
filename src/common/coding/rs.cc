// SPDX-License-Identifier: Apache-2.0

#include "rs.hh"

extern "C" {
#include <isa-l/erasure_code.h>
}

#include <glog/logging.h>

RSCode::RSCode(CodingOptions options) {
    coding_param_t n = options.getN();
    coding_param_t k = options.getK();

    // check the coding parameters
    if (n <= 0 || k <= 0 || n < k) {
        throw std::invalid_argument("RS codes only support n>=k, n > 0, and k > 0");
    }

    // set the coding options
    _options = options;

    _name = "RS";

    // initialize and generate RS matrix for coding
    gf_gen_rs_matrix(_encodeMatrix, n, k);
    ec_init_tables(k, n - k, &_encodeMatrix[k * k], _gftbl);

    DLOG(INFO) << "RS codes init with n=" << (int) n << ",k=" << (int) k << ",useCAR=" << (bool) _options.repairUsingCAR();
}

num_t RSCode::getNumDataChunks() {
    return _options.getK();
}

num_t RSCode::getNumCodeChunks() {
    return _options.getN() - _options.getK();
}

num_t RSCode::getNumChunks() {
    return _options.getN();
}

num_t RSCode::getNumChunksPerNode() {
    return 1;
}

length_t RSCode::getCodingStateSize() {
    return 0;
}

length_t RSCode::getChunkSize(length_t dataSize) {
    coding_param_t k = _options.getK();
    return (dataSize + k - 1) / k;
}

bool RSCode::encode(data_t *data, length_t dataSize, std::vector<Chunk> &stripe, data_t **codingState) {
    coding_param_t k = _options.getK(), n = _options.getN();

    unsigned char *codep[n - k], *datap[k];

    length_t chunkSize = getChunkSize(dataSize);

    // init the stripe with n chunks
    stripe.clear();
    stripe.resize(n);

    // set the pointers for data and code chunks
    for (coding_param_t i = 0; i < n; i++) {
        stripe.at(i).setChunkId(i); 
        // try allocate space for chunks and revert previous ones if fails
        if (!stripe.at(i).allocateData(chunkSize, /* aligned */ true)) {
            LOG(ERROR) << "Failed to allocate memory for chunk " << i << " in stripe with " << n << " chunks of size " << chunkSize;
            stripe.clear();
            return false;
        }
        if (i < k) {
            // copy data to chunk output and set the buffer pointers for encoding
            unsigned char *datacp = stripe.at(i).data;
            memcpy(datacp, data + i * chunkSize, chunkSize);
            datap[i] = datacp;
        } else {
            // set the buffer pointers for encoding
            codep[i - k] = (unsigned char *) stripe.at(i).data;
        }
    }

    // encode data chunks to code chunks
    ec_encode_data(chunkSize, k, n - k, _gftbl, datap, codep);

    return true;
}

bool RSCode::carRepairFinalize(data_t *inputp[], num_t numInputChunks, length_t chunkSize, data_t *decodep[]) {
    DLOG(INFO) << "Decode using partially encoded chunks, input chunks = " << numInputChunks;
    // if there is only 1 input chunk from 1 rack, no further decoding is required
    if (numInputChunks == 1) {
        memcpy(decodep[0], inputp[0], chunkSize);
        return true;
    }
    // construct decode matrix, which xor all partial encoded chunks
    uint8_t decodeMatrix[numInputChunks], gftbl[numInputChunks * 32];
    memset(decodeMatrix, 1, numInputChunks);
    ec_init_tables(numInputChunks, 1, decodeMatrix, gftbl);
    // decode (i.e., xor all chunks)
    ec_encode_data(chunkSize, numInputChunks, 1, gftbl, inputp, decodep);

    return true;
}

bool RSCode::decode(std::vector<Chunk> &inputChunks, data_t **decodedData, length_t &decodedSize, DecodingPlan &plan, data_t *codingState, bool isRepair, std::vector<chunk_id_t> repairTargets) {
    coding_param_t k = _options.getK(), n = _options.getN();

    num_t numDecodedChunks = k;
    num_t numInputChunks = inputChunks.size();
    length_t chunkSize = inputChunks.empty()? 0 : inputChunks.at(0).size; 

    int matrixSize = n * n;
    uint8_t decodeMatrix [matrixSize];
    uint8_t invertedMatrix [matrixSize];
    unsigned char *decodep[n], *inputp[n];
    data_t *decodedDataTmp = NULL;
    uint8_t *finalMatrix = NULL;

    bool allDataInput = true;
    bool repairTargetSpecified = !repairTargets.empty();

    DLOG_IF(INFO, isRepair && !repairTargetSpecified) << "Repair all missing chunks by default";
    DLOG_IF(INFO, isRepair && repairTargetSpecified) << "Repair specific chunks";

    // check if the number of alive chunks is sufficient for decode
    // must have at least k chunks except it is a repair using CAR
    if ( numInputChunks < k && (!isRepair || !_options.repairUsingCAR()) ) {
        LOG(ERROR) << "Insufficient input chunks for decoding, got " << numInputChunks << " but requires " << (int) k << " chunks or more";
        return false;
    }

    // (1) form a matrix that encodes the input rows
    // (2) figure out repair targets if not specified by function caller 
    // (3) set the input buffer pointer arrays for decoding
    for (chunk_id_t i = 0, inputIdx = 0, chunkId = 0; i < n; i++) {
        if (i < numInputChunks) {
            // set up the input buffer pointers
            inputp[i] = inputChunks.at(i).data;
        }
        if (inputIdx < numInputChunks && ((chunkId = inputChunks.at(inputIdx).chunkId) == i)) { // alive chunks
            // check if all input are data chunks
            allDataInput &= inputIdx == i;
            // prepare matrix for inversion and decoding
            memcpy(decodeMatrix + inputIdx * k, _encodeMatrix + chunkId * k, k);
            // increment the input index
            inputIdx++;
        } else if (isRepair && !repairTargetSpecified) { // failed chunk that should be the repair targets
            // accumulate repair targets
            repairTargets.push_back(i);
        }
    }

    // update the number of chunks to decode for repair after scanning for failed chunks when target was not specified by caller
    if (isRepair) {
        numDecodedChunks = repairTargets.size();
    }

    // allocate the decode buffer if nill, or reuse existing one
    if (*decodedData == NULL) {
        decodedDataTmp = (data_t*) malloc (sizeof(data_t) * numDecodedChunks * chunkSize);
        if (decodedDataTmp == NULL) {
            LOG(ERROR) << "Failed to allocate memory for decoded data of size " << numDecodedChunks * chunkSize;
            return false;
        }
    } else {
        decodedDataTmp = *decodedData;
    }
    // set up the decode buffer pointers
    for (num_t i = 0; i < numDecodedChunks; i++) {
        // set up the decode buffer pointers
        decodep[i] = (decodedDataTmp) + i * chunkSize;
    }

    // set the number of decoded chunks
    decodedSize = numDecodedChunks * chunkSize;

    // [sepcical case 1] single chunk repair using CAR
    if (isRepair && numDecodedChunks == 1 && _options.repairUsingCAR()) {
        bool repaired = carRepairFinalize(inputp, numInputChunks, chunkSize, decodep);
        if (repaired) {
            *decodedData = decodedDataTmp;
        } else if (*decodedData != decodedDataTmp) {
            free(decodedDataTmp);
        }
        return repaired;
    }

    // normal decoding flow (invert the encoding matrix of input chunks and apply it for decoding)
    // get the inverse of the encoding matrix
    if (gf_invert_matrix(decodeMatrix, invertedMatrix, k) < 0) {
        LOG(ERROR) << "Failed to invert the matrix for decoding";
        // if unsuccessful, free locally allocated buffer
        if (*decodedData != decodedDataTmp) free(decodedDataTmp);
        return false;
    }

    // final matrix to apply for decoding, which is the inverted one by default
    finalMatrix = invertedMatrix;

    // generate a new matrix for repair 
    if (isRepair) {
        // copy the rows for decoding
        num_t i = 0, j = 0, l = 0;
        uint8_t s;
        for (i = 0; i < numDecodedChunks && repairTargets.at(i) < k; i++) {
            memcpy(decodeMatrix + k * i, invertedMatrix + k * repairTargets.at(i), k);
        }
        // code chunks
        for (; i < numDecodedChunks; i++) {
            for (j = 0; j < k; j++) {
                s = 0;
                for (l = 0; l < k; l++)
                    s ^= gf_mul(invertedMatrix[l * k + j], _encodeMatrix[repairTargets.at(i) * k + l]);
                decodeMatrix[i * k + j] = s;
            }
        }
        // use the generated matrix for decoding instead
        finalMatrix = decodeMatrix;
    }

    // decode
    uint8_t gftbl [matrixSize * 32];
    ec_init_tables(k, numDecodedChunks, finalMatrix, gftbl);
    ec_encode_data(chunkSize, k, numDecodedChunks, gftbl, inputp, decodep);

    // set decode output
    *decodedData = decodedDataTmp;

    return true;
}

bool RSCode::preDecode(const std::vector<chunk_id_t> &failedChunkIdx, DecodingPlan &plan, data_t *codingState, bool isRepair) {
    coding_param_t k = _options.getK(), n = _options.getN();
    num_t numFailedChunks = failedChunkIdx.size();

    // cannot decode if there is less than k chunks available
    if (numFailedChunks > (uint32_t) n - k) {
        LOG(ERROR) << "The number of failure = " << numFailedChunks << " is greater than n-k=" << n - k;
        return false;
    }

    plan.release();

    // select first k chunks available, and mark the failed nodes for constructing decoding matrix
    // TODO an optimal approach in selecting which of the k chunks
    chunk_id_t erasures[n], i = 0, e = 0;
    for (i = 0, e = 0; i < n; i++) {
        // mark failed nodes, and do not use it as input chunks for decoding
        if (e < numFailedChunks && failedChunkIdx.at(e) == i) {
            erasures[e++] = i;
            continue;
        }
        // mark alive chunks as input for decoding
        plan.addInputChunkId(i);
    }

    num_t numInputChunks = plan.getNumInputChunks();
    std::vector<chunk_id_t> inputChunkIds = plan.getInputChunkIds();
    plan.setMinNumInputChunks(getNumDataChunks());

    // report failure if less than k chunks are available
    if (numInputChunks < k) {
        LOG(ERROR) << "Failed to find at least " << k << " chunks for decode (got " << numInputChunks << ")";
        plan.release();
        return false;
    }

    // only proceed to generate the repair matrix in plan when preparing for a repair
    if (!isRepair) {
        return true;
    }

    // find the invert matrix for decoding first
    int matrixSize = n * n;
    uint8_t decodeMatrix [matrixSize];
    uint8_t invertedMatrix [matrixSize];

    // get the row where data is alive
    for (num_t i = 0; i < numInputChunks; i++) {
        memcpy(decodeMatrix + i * k, _encodeMatrix + inputChunkIds.at(i) * k, k);
    }

    // get the inverse of the matrix of alive data
    if (gf_invert_matrix(decodeMatrix, invertedMatrix, k) < 0) {
        LOG(ERROR) << "Failed to invert the matrix for repair";
        plan.release();
        return false;
    }

    // allocate space for outputting repair matrix
    if (!plan.allocateRepairMatrix(e * k)) {
        LOG(ERROR) << "Failed to allocate space for repair matrix";
        plan.release();
        return false;
    }

    // copy the rows for decoding failed strips into extraInfo
    data_t *repairMatrix = plan.getRepairMatrix();
    int j = 0, l = 0;
    uint8_t s = 0;
    // data chunks
    for (i = 0; i < e && erasures[i] < k; i++) {
        memcpy(repairMatrix + k * i, invertedMatrix + k * erasures[i], sizeof(uint8_t) * k);
    }
    // code chunks
    for (; i < e; i++) {
        for (j = 0; j < k; j++) {
            s = 0;
            for (l = 0; l < k; l++)
                s ^= gf_mul(invertedMatrix[l * k + j], _encodeMatrix[erasures[i] * k + l]);
            repairMatrix[i * k + j] = s;
        }
    }

    return true;
}
