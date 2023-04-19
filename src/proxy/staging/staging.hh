// SPDX-License-Identifier: Apache-2.0

#ifndef __STAGING_HH__
#define __STAGING_HH__

#include "../../ds/file.hh"
#include "../../common/benchmark/benchmark.hh"
#include "../../common/config.hh"
#include "storage/staging_storage.hh"

class Staging {

public:
    Staging();
    ~Staging();

    bool openFileForWrite(const File &f);
    bool closeFileForWrite(const File &f);

    /**
     * Write a file to Staging data store
     *
     * @param[in] f                         file to write. Containing file data, metadata
     * @param[in] readFromCloud             whether the file is read from cloud
     * @param[in] isTruncated               whether the file is truncated (to zero)
     *
     * @return whether the write is successful
     **/
    bool writeFile(File &f, bool readFromCloud = false, bool isTruncated = true);

    /**
     * Read a file from Staging data store
     *
     * @param[in] f                         file to read. Containing file metadata
     *
     * @return whether the read is successful
     **/
    bool readFile(File &f);

    /**
     * Delete a file from Staging data store
     *
     * @param[in] f                         file to delete. Containing file metadata
     *
     * @return whether the delete is successful
     **/
    bool deleteFile(const File &f);

    bool commitReadCache(File &f);
    bool abortReadCache(File &f);

    bool pinFile(const File &f);
    bool unpinFile(const File &f);
    bool isFilePinned(const File &f);

    bool getFileInfo(FileInfo &info);

    bool running();

private:

    /************************************* methods **************************************/

    static void *cleanIdleFiles(void *arg);

    /************************************* members **************************************/

    StagingStorage *_storage;                       /**< Staging storage */
    bool _running;                                  /**< whether Staging is running */
    pthread_t _act;                                 /**< thread for auto-clean */

};

#endif //__STAGING_HH__
