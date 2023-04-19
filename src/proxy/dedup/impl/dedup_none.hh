// SPDX-License-Identifier: Apache-2.0

#ifndef __DEDUP_NONE_HH__
#define __DEDUP_NONE_HH__


#include "../dedup.hh"

class DedupNone : public DeduplicationModule {
public:

    /**
     * Deduplication module that does no deduplication
     **/

    DedupNone() {};
    ~DedupNone() {};

    /**
     * refer to DeduplicationModule::scan()
     **/

    std::string scan(const unsigned char *data, const BlockLocation &dataInObjectLocation, std::map<BlockLocation::InObjectLocation, std::pair<Fingerprint, bool> >& blocks);

    /**
     * refer to DeduplicationModule::commit()
     **/
    void commit(std::string commitId);

    /**
     * refer to DeduplicationModule::abort()
     **/
    void abort(std::string commitId);

    /**
     * refer to DeduplicationModule::query()
     **/
    std::vector<BlockLocation> query(const unsigned char namespaceId, const std::vector<Fingerprint> &fingerprints);

    /**
     * refer to DeduplicationModule::update()
     **/
    std::string update(const std::vector<Fingerprint> &fingerprints, const std::vector<BlockLocation> &oldLocations, const std::vector<BlockLocation> &newLocations);

private:
};

#endif // define __DEDUP_NONE_HH__
