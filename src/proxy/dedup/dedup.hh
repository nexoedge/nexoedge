// SPDX-License-Identifier: Apache-2.0

#ifndef __DEDUP_MODULE_HH__
#define __DEDUP_MODULE_HH__

#include <map>
#include <vector>

#include "block_location.hh"
#include "fingerprint/fingerprint.hh"
#include "chunking/chunker.hh"

class DeduplicationModule {
public:

    DeduplicationModule() {
        _chunker = 0;

    }

    virtual ~DeduplicationModule() {
        delete _chunker;
    };


    /**
     * Scan buffer for unique and duplicated data
     *
     * @param[in] data                     data buffer
     * @param[in] dataInObjectLocation     location of data in the object, with object namespace id, object name, object version, in-object data offset, and data length specified
     * @param[out] blocks                  the list of blocks (ordered by offset), with their in-object offset, length, corresponding fingerprints and whether they are a duplicate of existing blocks specified
     *
     * @return commit id for the list of blocks
     **/

    virtual std::string scan(const unsigned char *data, const BlockLocation &dataInObjectLocation, std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, bool> >& blocks) = 0;

    /**
     * Commit a list of blocks
     *
     * @param[in] commitId                 commit id returned by the scan() for the list of blocks to commit
     *
     **/
    virtual void commit(std::string commitId) = 0;

    /**
     * Abort a list of blocks
     *
     * @param[in] commitId                 commit id returned by the scan() for the list of blocks to abort
     *
     **/
    virtual void abort(std::string commitId) = 0;

    /**
     * Query the block locations for a list of fingerprints
     *
     * @param[in] namespaceId              namespace id of the fingerprints
     * @param[in] fingerprints             list of fingerprints to query
     *
     * @return the list of block locations ordered by the list of fingerprints
     **/
    virtual std::vector<BlockLocation> query(const unsigned char namespaceId, const std::vector<Fingerprint>& fingerprints) = 0;

    /**
     * Mark the block locations to update for a list of fingerprints
     *
     * @param[in] fingerprints             list of fingerprints to update
     * @param[in] oldLocations             list of old block locations
     * @param[in] newLocations             list of new block locations
     *
     * @return commit id for the list of updates
     **/
    virtual std::string update(const std::vector<Fingerprint> &fingerprints, const std::vector<BlockLocation> &oldLocations, const std::vector<BlockLocation> &newLocations) = 0;


protected:

    DedupChunker *_chunker;                   /**< block chunking module */

};

#endif // define __DEDUP_MODULE_HH__
