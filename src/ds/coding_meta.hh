// SPDX-License-Identifier: Apache-2.0

#ifndef __CODING_META_HH__
#define __CODING_META_HH__

class CodingMeta {
public:
    CodingMeta() {
        reset();
    };

    CodingMeta(unsigned char cs, int cn, int ck, int maxcs, int cf = 0) {
        reset();
        coding = cs;
        maxChunkSize = maxcs;
        n = cn;
        k = ck;
        f = cf;
    }

    ~CodingMeta() {
        delete [] codingState;
    }

    bool copyMeta(const CodingMeta &src, bool parametersOnly = false) {
        n = src.n;
        k = src.k;
        f = src.f;
        coding = src.coding;
        maxChunkSize = src.maxChunkSize;

        if (parametersOnly)
            return true;

        codingStateSize = src.codingStateSize;

        if (codingStateSize > 0) {
            delete [] codingState ;
            codingState = new unsigned char [codingStateSize];
            if (codingState == NULL)
                return false;
            memcpy(codingState, src.codingState, codingStateSize);
        }
        return true;
    }

    void reset() {
        coding = CodingScheme::UNKNOWN_CODE;
        n = 0;
        k = 0;
        f = 0;
        maxChunkSize = 0;
        codingState = 0;
        codingStateSize = 0;
    }

    bool operator == (const CodingMeta &y) {
        return true 
            && coding == y.coding
            && n == y.n
            && k == y.k
            && f == y.f
            && maxChunkSize == y.maxChunkSize
            && codingStateSize == y.codingStateSize
            && codingState? memcmp(codingState, y.codingState, codingStateSize) == 0 : true
        ;
    }

    std::string print() {
        return 
            std::string("coding = ").append(std::to_string(coding))
            .append(", n = ").append(std::to_string(n))
            .append(", k = ").append(std::to_string(k))
            .append(", f = ").append(std::to_string(f))
            .append(", maxChunkSize = ").append(std::to_string(maxChunkSize))
            .append(", codingStateSize = ").append(std::to_string(codingStateSize))
        ;
    }

    unsigned char coding;        /**< coding scheme */
    unsigned char *codingState;  /**< extra state information for coding */
    int n;                       /**< coding parameter n */
    int k;                       /**< coding parameter k */
    int f;                       /**< placement constraint parameter f */
    int codingStateSize;         /**< size of the extra codingStatermation */
    int maxChunkSize;            /**< max. chunk size */

};


#endif // ifndef __CODING_META_HH__

