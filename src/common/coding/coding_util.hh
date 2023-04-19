// SPDX-License-Identifier: Apache-2.0

#ifndef __CODING_UTIL_HH__
#define __CODING_UTIL_HH__

extern "C" {
#include <isa-l/erasure_code.h>
}

class CodingUtils {
public:
    static bool encode(unsigned char *data, int numDataChunks, unsigned char *code, int numCodeChunks, int chunkSize, unsigned char *matrix) {
        unsigned char *codep[numCodeChunks], *datap[numDataChunks];
        for (int i = 0; i < numDataChunks; i++)
            datap[i] = (unsigned char *) data + i * chunkSize;
        for (int i = 0; i < numCodeChunks; i++)
            codep[i] = (unsigned char *) code + i * chunkSize;

        uint8_t gftbl[numDataChunks * numCodeChunks * 32];
        ec_init_tables(numDataChunks, numCodeChunks, matrix, gftbl);
        ec_encode_data(chunkSize, numDataChunks, numCodeChunks, gftbl, datap, codep);

        return true;
    }
    static bool encode(unsigned char **data, int numDataChunks, unsigned char **code, int numCodeChunks, int chunkSize, unsigned char *matrix) {
        uint8_t gftbl[numDataChunks * numCodeChunks * 32];
        ec_init_tables(numDataChunks, numCodeChunks, matrix, gftbl);
        ec_encode_data(chunkSize, numDataChunks, numCodeChunks, gftbl, data, code);

        return true;
    }
};

#endif // define __CODING_UTIL_HH__
