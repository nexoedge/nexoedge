// SPDX-License-Identifier: Apache-2.0

#ifndef __DEDUP_CHUNKER_HH__
#define __DEDUP_CHUNKER_HH__

class DedupChunker {
public:
    DedupChunker() {};
    virtual ~DedupChunker() {};

    virtual unsigned int findOffsetToNextAnchor(const char *data, const unsigned int length) = 0;

protected:
};

#endif // define __DEDUP_CHUNKER_HH__
