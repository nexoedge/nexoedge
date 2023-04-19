// SPDX-License-Identifier: Apache-2.0

#include <stdio.h> // fprintf()
#include <stdlib.h> // exit(), rand()
#include <string.h> // strcmp(), memset()

#include <glog/logging.h>

#include <boost/timer/timer.hpp>

#include "../../ds/byte_buffer.hh"
#include "../../common/config.hh"

#include "../../common/coding/coding.hh"
#include "../../common/coding/coding_util.hh"
#include "../../common/coding/decoding_plan.hh"
#include "../../common/coding/all.hh"
#include "../../common/coding/coding_generator.hh"

#define N (12)
#define ROUNDS (3)  // rounds for repair single failure, esp. for non-exact repairing of F-MSR

#define HASH_SIZE CODING_HASH_SIZE

void usage(char *prg) {
    fprintf(stderr, "%s rand_num_seed file [more files to test]\n", prg);
    exit(1);
}

bool parameterValidationTest(int codingScheme, CodingOptions options, bool isValid) {
    Coding *code = CodingGenerator::genCoding(codingScheme, options);
    bool okay = code != 0;
    delete code;
    return (okay && isValid) || (!okay && !isValid);
}

/**
 * Test functions in Coding
 * @remark this function read the whole file into memory for testing, make sure the memory size of your machine is large enough to handle the file
 **/
bool codingTest(CodingOptions options, coding_param_t r, const char *codename, Coding *code, char *filename) {
    num_t numChunksPerNode = 0;
    length_t chunkSize = 0;
    length_t decodedSize = 0;
    num_t numDataChunks = 0;
    num_t numCodeChunks = 0;
    num_t numFailedChunks = 0;
    length_t codingStateSize = 0;

    coding_param_t n = options.getN();
    coding_param_t k = options.getK();

    data_t *fdata = NULL;
    data_t *originalData = NULL;
    data_t *codingState = NULL;
    data_t *decodeOutput = 0;
    data_t *recoveryOutput = 0;

    std::vector<Chunk> decodeInput;
    std::vector<Chunk> recoveryInput;
    std::vector<Chunk> stripe;
    std::vector<chunk_id_t> inputChunksInPlan;
    std::vector<chunk_id_t> repairTargets;
    std::vector<chunk_id_t> failedChunks;

    DecodingPlan plan;

    offset_t fsize = 0;
    offset_t readSize = 0;

    int ret = 0;

    chunk_id_t failedNode[2];
    num_t numChunksSelected = 0;
    num_t startidx = 0;

    bool isRS = false;

    bool isRSCAR = options.repairUsingCAR();
    bool tolerateDoubleFailure = n - k >= 2;

    bool okay = true;
    bool decodeSuccess = false;
    bool fileContentCorrect = false;

    boost::timer::cpu_times duration;
    boost::timer::cpu_timer mytimer;

    FILE *f = fopen(filename, "r");
    if (f == NULL) {
        printf("Failed to open file %s for testing", filename);
        okay = false;
        goto CODE_TEST_EXIT;
    }

    fseek(f, 0, SEEK_END);
    fsize = ftell(f);
    rewind(f);

    // expected values
    if (strcmp("RS", codename) == 0) {
        numChunksPerNode = 1;
        chunkSize = (fsize + k - 1) / k;
        numDataChunks = k;
        numCodeChunks = n - k;
        isRS = true;
    } else {
        fclose(f);
        return false;
    }

    // --------------------------- //
    //  verify coding information  //
    // --------------------------- //

    if (code->getNumChunksPerNode() != numChunksPerNode) {
        printf("  Incorrect number of chunks per node: expect %d, but got %d\n", numChunksPerNode, code->getNumChunksPerNode());
        fclose(f);
        return false;
    }

    if (code->getChunkSize(fsize) != chunkSize) {
        printf("  Incorrect chunk size: expect %d, but got %d\n", chunkSize, code->getChunkSize(fsize));
        fclose(f);
        return false;
    }

    if (code->getNumDataChunks() != numDataChunks) {
        printf("  Incorrect number of data chunk: expect %d, but got %d\n", numDataChunks, code->getNumDataChunks());
        fclose(f);
        return false;
    }

    if (code->getNumCodeChunks() != numCodeChunks) {
        printf("  Incorrect number of code chunk: expect %d, but got %d\n", numCodeChunks, code->getNumCodeChunks());
        fclose(f);
        return false;
    }

    if (code->getCodingStateSize() != codingStateSize) {
        printf("  Incorrect coding state size: expect %d, but got %d\n", codingStateSize, code->getCodingStateSize());
        fclose(f);
        return false;
    }


    // --------------------------- //
    //  read input data from file  //
    // --------------------------- //

    // allocate buffer for file data
    if (posix_memalign((void **) &fdata, 32, chunkSize * numDataChunks) != 0) {
        printf("  Failed to allocate memory for the file! (maybe the file is too large?..)\n");
        fclose(f);
        return false;
    }
    memset(fdata, 0, chunkSize * numDataChunks);

    // read file data
    while (readSize < fsize) {
        ret = fread(fdata + readSize, sizeof(char), fsize - readSize, f);
        if (ret < 0) {
            printf("  Failed to read file!\n");
            fclose(f);
            okay = false;
            goto CODE_TEST_EXIT;
        }
        readSize += ret;
    }
    fclose(f);

    // allocate buffer for decoding check in secret sharing
    if (posix_memalign((void **) &originalData, 32, chunkSize * numDataChunks) != 0) {
        printf("  Failed to allocate memory for data\n");
        okay = false;
        goto CODE_TEST_EXIT;
    }
    memcpy(originalData, fdata, chunkSize * numDataChunks);

    // adjust file size
    if (code->getExtraDataSize() > 0) {
        fsize = numDataChunks * chunkSize - code->getExtraDataSize();
    }


    // test coding operations

    // --------------- //
    //  test encoding  //
    // --------------- //

    mytimer.start();
    if (code->encode(fdata, fsize, stripe, &codingState) == false) {
        printf("  Failed to encode data\n");
        okay = false;
        goto CODE_TEST_EXIT;
    }
    duration = mytimer.elapsed();
    printf(" Encoding speed = %.3lf MB/s\n", (fsize * 1.0 / (1 << 20))  / (duration.wall * 1.0 / 1e9));

    // --------------- //
    //  test decoding  //
    // --------------- //

    if (posix_memalign((void **) &decodeOutput, 32, chunkSize * numDataChunks) != 0) {
        printf("  Failed to allocate memory for decode input or output!\n");
        okay = false;
        goto CODE_TEST_EXIT;
    }

    failedChunks.clear();
    // use the minimal number of chunks at the end of the stripe, and mark others as failed
    numFailedChunks = stripe.size() - numDataChunks;
    for (num_t i = 0; i < numFailedChunks; i++) {
        failedChunks.push_back(i);
    }

    // get the decoding plan
    plan.release();
    if (code->preDecode(failedChunks, plan, codingState) == false) {
        printf("  Failed to find a decoding plan!\n");
        okay = false;
        goto CODE_TEST_EXIT;
    }

    // prepare input accroding to decoding plan
    numChunksSelected = plan.getMinNumInputChunks();
    inputChunksInPlan = plan.getInputChunkIds();
    decodeInput.clear();
    decodeInput.resize(numChunksSelected);
    for (num_t i = 0; i < numChunksSelected; i++) {
        //printf("  Choose chunk %u\n", inputChunksInPlan.at(i));
        if (decodeInput.at(i).copy(stripe.at(inputChunksInPlan.at(i))) == false) {
            printf("  Failed to allocate memory for chunks according to the decoding plan! (chunk %u out of %u chunks of sizes %u each)\n", i, numChunksSelected, stripe.at(inputChunksInPlan.at(i)).size);
            okay = false;
            goto CODE_TEST_EXIT;
        }
    }

    free(decodeOutput);
    decodeOutput = 0;
    
    // decode
    mytimer.start();
    decodeSuccess = code->decode(decodeInput, &decodeOutput, decodedSize, plan, codingState);
    duration = mytimer.elapsed();
    printf(" Decoding speed = %.3lf MB/s\n", chunkSize * numDataChunks * 1.0 / (1 << 20) / (duration.wall * 1.0 / 1e9));
    //printf("fdata %x %x\n", fdata[0], fdata[chunkSize]);
    //printf("decodeOutput %x %x\n", decodeOutput[0], decodeOutput[chunkSize]);

    // check decoded data
    fileContentCorrect = memcmp(originalData, decodeOutput, fsize) == 0;
    if (!decodeSuccess || !fileContentCorrect) {
        printf(
            "Failed to decode data, decode %s, content is %s\n"
            , decodeSuccess? "success" : "failed"
            , fileContentCorrect? "correct" : "incorrect"
        );

        f = fopen("debug_file_output.tmp", "w");
        fwrite(decodeOutput, 1, fsize, f);
        fclose(f);
        okay = false;
        goto CODE_TEST_EXIT;
    }


    // single failure
    for (num_t round = 0; round < n && okay; round++) {

        // -------------------------------- //
        //  test recovery (single failure)  //
        // -------------------------------- //

        // release previous decoding plan
        plan.release();

        // choose a failed node
        failedNode[0] = round;
        printf("   > Node %d failed\n", failedNode[0]);

        repairTargets.clear();
        // mark all chunks on the failed node as failed
        for (num_t i = 0; i < numChunksPerNode; i++) {
            repairTargets.push_back(failedNode[0] * numChunksPerNode + i);
        }

        // get repair plan
        if (code->preDecode(repairTargets, plan, codingState, /* is repair */ true) == false) {
            printf("  Failed to get required information on data as repair input\n");
            okay = false;
            goto CODE_TEST_EXIT;
        }
        numChunksSelected = plan.getMinNumInputChunks();
        inputChunksInPlan = plan.getInputChunkIds();

        if (isRS && numChunksSelected != numDataChunks) {
            printf("   Number of chunks selected is %u instead of %u for single failiure under RS\n", numChunksSelected, numDataChunks);
        }

        // prepare recovery inputs
        recoveryInput.clear();
        recoveryInput.resize(numChunksSelected);

        if (!isRSCAR) {

            // assume all inputs are successfully collected - set the repair inputs
            for (num_t i = 0; i < numChunksSelected; i++) {
                recoveryInput.at(i).copy(stripe.at(inputChunksInPlan.at(i)));
            }

        } else {

            data_t *partialInput[numChunksSelected];
            data_t *partialOutput[1];
            data_t *repairMatrix = plan.getRepairMatrix();
            num_t numChunksInRack = 0, numRacksSelected = 0;
            // for each rack, combine the partial result
            //printf("  Do partial encoding for selected chunks %u in %u racks\n", numChunksSelected, r);
            for (num_t cr = 0, rn = 0, cidx = 0; cr < (num_t) r && cidx < numChunksSelected; cr++, rn += numChunksInRack) {
                // find the number of chunks in this rack. If the remainder is greater than the current rack id, add this rack should store one more chunk than others 
                numChunksInRack = n / r + (cr < n % r);
                chunk_id_t rackEndChunkId = rn + numChunksInRack;
                chunk_id_t rackStartChunkIdx = cidx;
                // count the rack if at least one chunk will be chosen from this rack, otherwise, skip this rack
                if (inputChunksInPlan.at(cidx) < rackEndChunkId) {
                    numRacksSelected++;
                } else {
                    continue;
                }
                //printf("  Do partial encoding for chunk %u - %u in rack %u\n", rn, rackEndChunkId, cr);
                // for chunks distributed in each rack
                for (num_t i = rn; i < rackEndChunkId && cidx < numChunksSelected; i++) {
                    chunk_id_t selectedChunkId = inputChunksInPlan.at(cidx);
                    // skip if chunk is not selected
                    if (i != selectedChunkId) continue;
                    // set input data pointer to the selected input chunk
                    partialInput[cidx - rackStartChunkIdx] = stripe.at(selectedChunkId).data;
                    //printf("   Chunk %u in rack %u with coefficient %02x\n", i, cr, repairMatrix[cidx]);
                    // advance to next input chunk
                    cidx++;
                }
                if (!recoveryInput.at(numRacksSelected - 1).allocateData(chunkSize)) {
                    printf("  Failed to allocate recovery input for CAR\n");
                    okay = false;
                    goto CODE_TEST_EXIT;
                }
                partialOutput[0] = recoveryInput.at(numRacksSelected - 1).data;
                // encode the partial chunk
                CodingUtils::encode(partialInput, cidx - rackStartChunkIdx, partialOutput, 1, chunkSize, repairMatrix + rackStartChunkIdx);
            }
            // number of input chunks = number of partially encoded chunks from selected racks
            //numChunksSelected = numRacksSelected;
            recoveryInput.resize(numRacksSelected);
        } 

        // mark the failed chunks
        repairTargets.clear();
        for (num_t i = 0; i < numChunksPerNode; i++)
            repairTargets.push_back(failedNode[0] * numChunksPerNode + i);

        free(recoveryOutput);
        recoveryOutput = 0;

        // do recovery
        mytimer.start();
        if (code->decode(recoveryInput, &recoveryOutput, decodedSize, plan, codingState, /* is repair */ true, repairTargets) == false) {
            printf("  Failed to repair data under single failure!\n");
            okay = false;
            goto CODE_TEST_EXIT;
        }
        duration = mytimer.elapsed();
        printf(" Repair (single failure) speed = %.3lf MB/s\n", chunkSize * numChunksPerNode * 1.0 / (1 << 20) / (duration.wall * 1.0 / 1e9));

        // copy the repaired data to the stripe (i.e., write the recovered chunk as part of the stripe)
        for (num_t i = 0; i < repairTargets.size(); i++) {
            memcpy(stripe.at(repairTargets.at(i)).data, recoveryOutput + i * chunkSize, chunkSize);
        }

        // clean up previous decode inputs
        decodeInput.clear();
        decodeInput.resize(numDataChunks);

        // use the recovered data to decode
        failedChunks.clear();
        startidx = failedNode[0] + k >= n? n - k : failedNode[0];
        for (num_t i = 0; i < startidx * numChunksPerNode; i++) {
            failedChunks.push_back(i);
        }

        // get the decoding plan
        plan.release();
        if (!code->preDecode(failedChunks, plan, codingState)) {
            printf("  Failed to find a plan for decoding after repairing single failure!\n");
            okay = false;
            goto CODE_TEST_EXIT;
        }

        // prepare input accroding to decoding plan
        numChunksSelected = plan.getMinNumInputChunks();
        inputChunksInPlan = plan.getInputChunkIds();
        for (num_t i = 0; i < numChunksSelected; i++) {
            //printf("  Choose chunk %u\n", inputChunksInPlan.at(i));
            if (!decodeInput.at(i).copy(stripe.at(inputChunksInPlan.at(i)))) {
                printf("  Failed to allocate memory for chunks according to the decoding plan! (chunk %u out of %u chunks of sizes %u each)\n", i, numChunksSelected, stripe.at(inputChunksInPlan.at(i)).size);
                okay = false;
                goto CODE_TEST_EXIT;
            }
        }

        free(decodeOutput);
        decodeOutput = 0;
        // decode
        decodeSuccess = code->decode(decodeInput, &decodeOutput, decodedSize, plan, codingState);
        // check contect correctness
        fileContentCorrect = memcmp(originalData, decodeOutput, fsize) == 0;
        if (!decodeSuccess || !fileContentCorrect) {
            printf("   Failed to decode after repairing single failiure (decode = %s, file content is %s)\n"
                , decodeSuccess? "success" : "failed"
                , fileContentCorrect? "correct" : "incorrect"
            );
            f = fopen("debug_file_output.tmp", "w");
            fwrite(decodeOutput, 1, fsize, f);
            fclose(f);
            okay = false;
            goto CODE_TEST_EXIT;
        }

        if (!tolerateDoubleFailure)
            continue;

        // -------------------------------- //
        //  test recovery (double failure)  //
        // -------------------------------- //

        chunk_id_t first = failedNode[0];

        for (chunk_id_t fnode = first + 1; fnode < n; fnode++) {
            failedNode[0] = first;
            failedNode[1] = fnode;

            if (failedNode[0] > failedNode[1]) {
                std::swap(failedNode[0], failedNode[1]);
            }

            printf("   > Nodes (%d,%d) failed\n", failedNode[0], failedNode[1]);

            // mark repair targets
            repairTargets.clear();
            for (num_t i = 0; i < 2; i++) {
                for (num_t j = 0; j < numChunksPerNode; j++) {
                    chunk_id_t failedChunkId = failedNode[i] * numChunksPerNode + j;
                    repairTargets.push_back(failedChunkId);
                }
            }

            // get repair plan 
            plan.release();
            if (code->preDecode(repairTargets, plan, codingState, /* is repair */ true) == false) {
                printf("  Failed to get required information on data as repair input\n");
                okay = false;
                goto CODE_TEST_EXIT;
            }

            // set the repair input
            numChunksSelected = plan.getMinNumInputChunks();
            inputChunksInPlan = plan.getInputChunkIds();
            recoveryInput.clear();
            recoveryInput.resize(numChunksSelected);
            for (num_t i = 0; i < numChunksSelected; i++) {
                //printf("   Chunk %u at index %u\n", inputChunksInPlan.at(i), i);
                recoveryInput.at(i).copy(stripe.at(inputChunksInPlan.at(i)));
            }

            free(recoveryOutput);
            recoveryOutput = 0;

            // do recovery
            mytimer.start();
            if (code->decode(recoveryInput, &recoveryOutput, decodedSize, plan, codingState, /* is repair */ true, repairTargets) == false) {
                printf("  Failed to repair data for double failure!\n");
                okay = false;
                goto CODE_TEST_EXIT;
            }
            duration = mytimer.elapsed();
            printf(" Repair (double failure) speed = %.3lf MB/s\n", chunkSize * numChunksPerNode * 2.0 / (1 << 20) / (duration.wall * 1.0 / 1e9));

            // copy the recovered data back to stripe
            for (num_t i = 0; i < repairTargets.size(); i++) {
                chunk_id_t chunkId = repairTargets.at(i);
                assert(memcmp(stripe.at(chunkId).data, recoveryOutput + i * chunkSize, chunkSize) == 0);
                memcpy(stripe.at(chunkId).data, recoveryOutput + i * chunkSize, chunkSize);
            }

            // try using the recovered data to decode
            decodeInput.clear();
            decodeInput.resize(numDataChunks);
            startidx = (failedNode[0] >= n - k)? n - k : 0;
            bool notBoth = true;
            for (num_t i = startidx; i < k + startidx - (notBoth? 1 : 0); i++) {
                // check if the second recovered input is included
                notBoth &= (failedNode[1] != i);
                // copy all chunks on the node
                for (num_t j = 0; j < numChunksPerNode; j++) {
                    decodeInput.at((i - startidx) * numChunksPerNode + j).copy(stripe.at(i * numChunksPerNode + j));
                }
            }
            // manual choose the second failure node as the last input
            if (notBoth) {
                for (num_t j = 0; j < numChunksPerNode; j++) {
                    decodeInput.at((numDataChunks - numChunksPerNode) + j).copy(stripe.at(failedNode[1] * numChunksPerNode + j));
                }
            }

            free(decodeOutput);
            decodeOutput = 0;

            decodeSuccess = code->decode(decodeInput, &decodeOutput, decodedSize, plan, codingState);
            fileContentCorrect = memcmp(originalData, decodeOutput, fsize) == 0;
            if (!decodeSuccess || !fileContentCorrect ) {
                printf("   Failed to decode after repairing double failiure (decode = %s, file content is %s)\n"
                    , decodeSuccess? "success" : "failed"
                    , fileContentCorrect? "correct" : "incorrect"
                );
                f = fopen("debug_file_output.tmp", "w");
                fwrite(decodeOutput, 1, fsize, f);

                okay = false;
                fclose(f);
                goto CODE_TEST_EXIT;
            }
        }
    }

CODE_TEST_EXIT:

    recoveryInput.clear();
    free(recoveryOutput);
    decodeInput.clear();
    free(decodeOutput);
    delete [] codingState;
    plan.release();
    stripe.clear();
    free(originalData);
    free(fdata);

    return okay;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        usage(argv[0]);
    }

    Config &config = Config::getInstance();
    config.setConfigPath();

    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    google::InitGoogleLogging(argv[0]);

    // use input number as random seed
    srand(atoi(argv[1]));

    printf("Start of Coding Test\n");
    printf("====================\n");

    CodingOptions options;

    bool pass = true;

    // test invalid parameter checking
    printf("| Check invalid coding parameters detection\n");
    options.setN(1);
    options.setK(1);
    for (int c = 0; c < CodingScheme::UNKNOWN_CODE && pass; c++)
        pass = parameterValidationTest(c, options, true);

    printf("> (valid) n = 19, k = 17\n");
    options.setN(19);
    options.setK(17);
    for (int c = 0; c < CodingScheme::UNKNOWN_CODE && pass; c++)
        pass = parameterValidationTest(c, options, true);

    if (!pass)
        exit(-1);

    // test coding schemes
    printf("| Check coding operations");
    Coding *code = 0;
    for (coding_param_t n = 4; n <= N && pass; n++) {
        for (coding_param_t m = 1; m < n - 1; m++) {
            coding_param_t k = n - m;

            options.setN(n);
            options.setK(k);
            coding_param_t r = n-k; // here, assume one agent per datacenter and represents a 'rack'

            printf("> RS, n=%d, k=%d, r=%d\n", n, k, r);
            code = CodingGenerator::genCoding(CodingScheme::RS, options);
            for (int i = 2; i < argc && pass; i++)
                pass = codingTest(options, r, "RS", code, argv[i]);
            delete code;
            printf("\n");
            
            if (!pass)
                break;
        }
    }    

    if (!pass)
        printf("Test Failed!!!\n");
    else 
        printf("End of Coding Test. All passed!\n");
    printf("====================\n");

    return 0;
}
