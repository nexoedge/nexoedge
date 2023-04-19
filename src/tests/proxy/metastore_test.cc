// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h>
#include <map>

#include <boost/timer/timer.hpp>

#include "../../common/define.hh"
#include "../../common/config.hh"
#include "../../common/checksum_calculator.hh"
#include "../../proxy/metastore/metastore.hh"
#include "../../proxy/metastore/redis_metastore.hh"

static const size_t numFilesToTest = 1024;
static const int maxFileNameLength = 1024;
static const unsigned long maxFileSize = (unsigned long) (1 << 30) * 4; // 4GB
static int chunkSize = (1 << 20); // 1MB

static File f[numFilesToTest];
static MetaStore *metastore = NULL;
static std::map<int, std::map<std::string, File*>> fileMapByNamespace;

static MetaStore *newMetaStore();
static void initFiles();
static void updateFiles();
static bool compareFile(size_t, const File&, const File&);
static void exitWithError();
static void readAndCheckFileMeta();

int main(int argc, char **argv) {

    /**
     * Tests for metastore
     *
     * 1. File metadata write
     * 2. File metadata update
     * 3. File lock
     * 4. File unlock
     * 5. File listing
     * 6. File metadata delete
     * 7. File repair list
     *
     **/

    // config
    Config &config = Config::getInstance();
    if (argc > 1) {
        config.setConfigPath(std::string(argv[1]));
    } else {
        config.setConfigPath();
    }
    
    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    google::InitGoogleLogging(argv[0]);

    // seed the random number sequence
    srand(987123);

    // metastore
    metastore = newMetaStore();

    printf("Start MetaStore Test\n");
    printf("====================\n");

    int testCount = 0;

    initFiles();
    printf("> Init %lu files\n", numFilesToTest);

    boost::timer::cpu_timer mytimer;
    // test 1: file metadata write
    {
        // write file metadata
        for (size_t i = 0; i < numFilesToTest; i++)
            if (!metastore->putMeta(f[i])) {
                printf(">> Failed to put file %lu metadata\n", i);
                exitWithError();
            }
        // read back and check
        readAndCheckFileMeta();
    }
    printf("> Test %d completes: Wirte metadata of %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 2: file metadata update
    mytimer.start();
    {
        // update file metadata
        updateFiles();
        for (size_t i = 0; i < numFilesToTest; i++)
            metastore->putMeta(f[i]);
        // read back and check
        readAndCheckFileMeta();
    }

    printf("> Test %d completes: Update metadata of %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 3: file lock
    mytimer.start();
    {
        // lock files
        for (size_t i = 0; i < numFilesToTest; i++)
            if (!metastore->lockFile(f[i])) {
                printf(">> Failed to lock file %lu\n", i);
                exitWithError();
            }

        // suppress error message due to failed file locking attempts
        fclose(stderr);

        // lock should fail on next attempt
        for (size_t i = 0; i < numFilesToTest; i++)
            if (metastore->lockFile(f[i])) {
                printf(">> Failed to prevent locking of locked file %lu\n", i);
                exitWithError();
            }
    }
    printf("> Test %d compeltes: Lock %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 4: file unlock
    mytimer.start();
    {
        // unlock files
        for (size_t i = 0; i < numFilesToTest; i++)
            if (!metastore->unlockFile(f[i])) {
                printf(">> Failed to unlock file %lu (first attempt)\n", i);
                exitWithError();
            }
        // lock should suceed after unlock
        for (size_t i = 0; i < numFilesToTest; i++)
            if (!metastore->lockFile(f[i])) {
                printf(">> Failed to lock file %lu after unlock\n", i);
                exitWithError();
            }
        // unlock files
        for (size_t i = 0; i < numFilesToTest; i++)
            if (!metastore->unlockFile(f[i])) {
                printf(">> Failed to unlock file %lu (second attempt)\n", i);
                exitWithError();
            }
    }
    printf("> Test %d completes: Unlock %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 5: file listing
    mytimer.start();
    {
        FileInfo *flist;
        size_t totalFileCount = 0, fileCount = 0;
        for (size_t i = 0; i < 255; i++, totalFileCount += fileCount) {
            fileCount = metastore->getFileList(&flist, i);
            try {
                std::map<std::string, File*> files = fileMapByNamespace.at(i);
                // sub file count in this namespace
                if (files.size() != fileCount) {
                    printf(">> Number of files in namespace %lu mismatched (%lu vs %lu)\n", i, fileCount, files.size());
                    exitWithError();
                }
                // check the files
                for (size_t fc = 0; fc < fileCount; fc++) {
                    try {
                        std::string fname (flist[fc].name, flist[fc].nameLength);
                        File *f = files.at(fname);
                        // file size
                        if (f->size != flist[fc].size) {
                            printf(">> File %lu in namespace %lu size mismatched (%lu vs %lu)\n", fc, i, flist[fc].size, f->size);
                            exitWithError();
                        }
                        // file timestamps
                        if (f->ctime != flist[fc].ctime ||
                                f->atime != flist[fc].atime ||
                                f->mtime != flist[fc].mtime
                        ) {
                            printf(">> File %lu in namespace %lu timestamps mismatched "
                                    "(ctime %lu vs %lu)"
                                    "(atime %lu vs %lu)"
                                    "(mtime %lu vs %lu)"
                                    "\n"
                                    , fc, i
                                    , flist[fc].ctime, f->ctime
                                    , flist[fc].atime, f->atime
                                    , flist[fc].mtime, f->mtime
                            );
                            exitWithError();
                        }
                        // file checksum (md5)
                        if (memcmp(f->md5, flist[fc].md5, MD5_DIGEST_LENGTH) != 0) {
                            printf("File %lu in namespace %lu md5 mismatched (%s vs %s)\n"
                                , fc, i
                                , ChecksumCalculator::toHex(flist[fc].md5, MD5_DIGEST_LENGTH).c_str()
                                , ChecksumCalculator::toHex(f->md5, MD5_DIGEST_LENGTH).c_str()
                            );
                            exitWithError();
                        }
                    } catch (std::out_of_range &e) {
                        printf(">> Failed to find the file from metadata store in namespace %lu\n", i);
                        exitWithError();
                    }
                }
                // release the file list
                delete [] flist;
                flist = NULL;
            } catch (std::out_of_range &e) {
                if (fileCount != 0) {
                    printf(">> Number of files in namespace %lu mismatched (%lu vs 0)\n", i, fileCount);
                }
            }
            
        }
        // total number of files obtained from the metastore
        if (totalFileCount != numFilesToTest) {
            printf(">> Number of files in metadata store mismatched (%lu vs %lu)\n", totalFileCount, numFilesToTest);
            std::string line;
            std::getline (std::cin, line);
            exitWithError();
        }
        // total file count kept reported by metastore
        if (metastore->getNumFiles() != numFilesToTest) {
            printf(">> File count in metadata store mismatched (%lu vs %lu)\n", metastore->getNumFiles(), numFilesToTest);
            exitWithError();
        }
    }
    printf("> Test %d completes: List %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 6: file metadata delete
    mytimer.start();
    { 
        // delete file metadata
        for (size_t i = 0; i < numFilesToTest; i++) {
            if (!metastore->deleteMeta(f[i])) {
                printf(">> Failed to delete file %lu\n", i);
                exitWithError();
            }
        }
    }
    printf("> Test %d completes: Delete %lu files in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    // test 7: file repair list
    mytimer.start();
    {
        // mark files for repair
        for (size_t i = 0; i < numFilesToTest; i++) {
            if (!metastore->markFileAsNeedsRepair(f[i])) {
                printf(">> Failed to mark file %lu for repair\n", i);
                exitWithError();
            }
        }
        // check the number of files to repair
        if (metastore->getNumFilesToRepair() != numFilesToTest) {
            printf(">> Number of files to repair mismatched (%lu vs %lu)\n", metastore->getNumFilesToRepair(), numFilesToTest);
            exitWithError();
        }
        // get files to repair (TODO test batch?), and mark them as repaired
        for (size_t i = 0; i < numFilesToTest; i++) {
            File rf[1];
            if (!metastore->getFilesToRepair(1, rf)) {
                try {
                    File *of = fileMapByNamespace.at(rf[0].namespaceId).at(std::string(rf[0].name, rf[0].nameLength));
                    // check the file metadata
                    if (!compareFile(i, *of, rf[0])) {
                        exitWithError();
                    }
                    // mark as repaired
                    if (!metastore->markFileAsRepaired(rf[0])) {
                        printf(">> Failed to mark file as repaired\n");
                        exitWithError();
                    }
                    // check the number of files to repair
                    if (metastore->getNumFilesToRepair() != numFilesToTest - i) {
                        printf(">> Number of files to repair mismatched (%lu vs %lu)\n", metastore->getNumFilesToRepair(), numFilesToTest - i);
                        exitWithError();
                    }
                } catch (std::out_of_range &e) {
                    printf(">> Got a non-existing file in namespace %d from metastore for repair\n", rf[0].namespaceId);
                    exitWithError();
                }
            }
        }
        // check the number of files to repair
        if (metastore->getNumFilesToRepair() != 0) {
            printf(">> Number of files to repair mismatched (%lu vs 0)\n", metastore->getNumFilesToRepair());
            exitWithError();
        }
    }
    printf("> Test %d completes: Mark and unmark %lu files for repair in %.3lf seconds\n", ++testCount, numFilesToTest, mytimer.elapsed().wall / 1e9);

    printf("End of MetaStore Test\n");
    printf("=====================\n");

    return 0;
}

MetaStore *newMetaStore() {
    Config &config = Config::getInstance();
    
    switch (config.getProxyMetaStoreType()) {
    case MetaStoreType::REDIS:
        return new RedisMetaStore();
    return new RedisMetaStore();
}

void addSpecialChars(File *f) {
    // add some special characters
    if (f->nameLength >= 2)  f->name[1] = ' ';
    if (f->nameLength >= 4)  f->name[3] = '_';
    if (f->nameLength >= 6)  f->name[5] = '-';
    if (f->nameLength >= 8)  f->name[7] = '=';
    if (f->nameLength >= 10) f->name[9] = '+';
    if (f->nameLength >= 13) f->name[12] = '=';
    if (f->nameLength >= 17) f->name[16] = '(';
    if (f->nameLength >= 20) f->name[19] = '(';
    if (f->nameLength >= 23) f->name[22] = ')';
    if (f->nameLength >= 24) f->name[23] = '[';
    if (f->nameLength >= 28) f->name[27] = ']';
    if (f->nameLength >= 30) f->name[29] = '{';
    if (f->nameLength >= 33) f->name[32] = '}';
    if (f->nameLength >= 37) f->name[36] = '/';
    if (f->nameLength >= 39) f->name[38] = '/';
    if (f->nameLength >= 40) f->name[39] = '\\';
    if (f->nameLength >= 42) f->name[41] = '.';
    if (f->nameLength >= 44) f->name[43] = ',';
    if (f->nameLength >= 48) f->name[47] = '@';
    if (f->nameLength >= 52) f->name[51] = '!';
    if (f->nameLength >= 54) f->name[53] = '%';
    if (f->nameLength >= 60) f->name[59] = '&';
    if (f->nameLength >= 78) f->name[77] = '~';
}

static void initFiles() {
    Config &config = Config::getInstance();

    int k = config.getK();
    int n = config.getN();
    chunkSize = config.getMaxChunkSize();

    // simply the calculation by assuming there is no reserved area in each chunk
    unsigned long dataStripeSize = chunkSize * k;

    // construct file metadata
    for (size_t i = 0; i < numFilesToTest; i++) {
        // file name
        f[i].nameLength = (rand() % maxFileNameLength) + 1;
        f[i].name = (char *) malloc (f[i].nameLength + 1);
        for (int j = 0; j < f[i].nameLength; j++)
            f[i].name[j] = (rand() % 26) + 65; // make sure the char is between A-Z
        addSpecialChars(f + i);
        f[i].name[f[i].nameLength] = 0; // null-terminated file name

        // file UUID
        f[i].genUUID();

        // file namespace (255 is INVALID_NAMESPACE_ID)
        f[i].namespaceId = rand() % 255;

        // file size
        f[i].size = rand() % maxFileSize;

        // file chunks
        f[i].numStripes = (f[i].size + dataStripeSize - 1) / dataStripeSize;
        f[i].numChunks = f[i].numStripes * n;
        f[i].chunks = new Chunk[f[i].numChunks];
        
        for (int s = 0; s < f[i].numStripes; s++) {
            int chunkSize = (std::min(f[i].size - dataStripeSize * i, dataStripeSize) + k - 1) / k * k; // round up to the nearest multiple of k
            for (int c = 0; c < n; c++) {
                f[i].chunks[s * n + c].setId(f[i].namespaceId, f[i].uuid, s * n + c);
                f[i].chunks[s * n + c].size = chunkSize;
            }
        }

        // file containers
        f[i].containerIds = new int[f[i].numChunks];
        for (int c = 0; c < f[i].numChunks; c++)
            f[i].containerIds[c] = rand() % 256;

        // file timestamps
        f[i].ctime = rand();
        f[i].atime = rand();
        f[i].mtime = rand();
        f[i].tctime = rand();

        // file version
        f[i].version = rand() % 256;

        // file status
        f[i].status = rand() % 3;

        // file checksum
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            f[i].md5[j] = rand() % 256;

        // file coding scheme
        f[i].codingMeta.coding = rand() % UNKNOWN_CODE;
        f[i].codingMeta.k = k;
        f[i].codingMeta.n = n;
        f[i].codingMeta.codingStateSize = rand() % 128;
        if (f[i].codingMeta.codingStateSize > 0) {
            f[i].codingMeta.codingState = (unsigned char *) malloc (f[i].codingMeta.codingStateSize);
            memset(f[i].codingMeta.codingState, rand() % 256, f[i].codingMeta.codingStateSize);
        }

        //printf("Init file %d with name %s and size %lu in namespace %d using code %d\n", i, f[i].name, f[i].size, f[i].namespaceId, f[i].codingMeta.coding);
        // add it to the map by namespace id
        if (fileMapByNamespace.count(f[i].namespaceId) == 0) {
            std::map<std::string, File*> fileMap;
            fileMap.insert(std::make_pair(std::string(f[i].name), f + i));
            fileMapByNamespace.insert(std::make_pair(f[i].namespaceId, fileMap));
        } else {
            fileMapByNamespace.at(f[i].namespaceId).insert(std::make_pair(std::string(f[i].name), f + i));
        }
    }
}

static void updateFiles() {
    Config &config = Config::getInstance();

    int k = config.getK();
    int n = config.getN();
    chunkSize = config.getMaxChunkSize();

    // simply the calculation by assuming there is no reserved area in each chunk
    unsigned long dataStripeSize = chunkSize * k;
    for (size_t i = 0; i < numFilesToTest; i++) {
        // file version
        f[i].version += 2;

        // file size
        f[i].size = rand() % maxFileSize;

        // file chunks
        f[i].numStripes = (f[i].size + dataStripeSize - 1) / dataStripeSize;
        f[i].numChunks = f[i].numStripes * n;
        delete [] f[i].chunks;
        f[i].chunks = new Chunk[f[i].numChunks];
        
        for (int s = 0; s < f[i].numStripes; s++) {
            int chunkSize = (std::min(f[i].size - dataStripeSize * i, dataStripeSize) + k - 1) / k * k; // round up to the nearest multiple of k
            for (int c = 0; c < n; c++) {
                f[i].chunks[s * n + c].setId(f[i].namespaceId, f[i].uuid, s * n + c);
                f[i].chunks[s * n + c].size = chunkSize;
            }
        }

        // file containers
        delete [] f[i].containerIds;
        f[i].containerIds = new int[f[i].numChunks];
        for (int c = 0; c < f[i].numChunks; c++)
            f[i].containerIds[c] = rand() % 256;

        // file timestamps
        f[i].ctime = rand();
        f[i].atime = rand();
        f[i].mtime = rand();
        f[i].tctime = rand();

        // file checksum
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            f[i].md5[j] = rand() % 256;
    }
}

static bool compareFile(size_t i, const File &origin, const File &retrieved) {
    // file name
    if (origin.nameLength != retrieved.nameLength || strcmp(origin.name, retrieved.name) != 0) {
        printf("File %lu name mismatched (%s vs %s)\n", i, retrieved.name, origin.name);
        return false;
    }
    // file uuid
    if (origin.uuid != retrieved.uuid) {
        printf("File %lu UUID mismatched (%s vs %s)\n"
            , i
            , boost::uuids::to_string(retrieved.uuid).c_str()
            , boost::uuids::to_string(origin.uuid).c_str()
        );
        return false;
    }
    // file namespace
    if (origin.namespaceId != retrieved.namespaceId) {
        printf("File %lu namespace Id mismatched (%d vs %d)\n", i, retrieved.namespaceId, origin.namespaceId);
        return false;
    }
    // file size
    if (origin.size != retrieved.size) {
        printf("File %lu size mismatched (%lu vs %lu)\n", i, retrieved.size, origin.size);
        return false;
    }
    // file chunks
    if (origin.numStripes != retrieved.numStripes ||
        origin.numChunks != retrieved.numChunks
    ) {
        printf("File %lu num. of stripes/chunks mismatched (%d vs %d stripes, %d vs %d chunks)\n"
                , i
                , retrieved.numStripes
                , origin.numStripes
                , retrieved.numChunks
                , origin.numChunks
        );
        return false;
    }
    if (retrieved.numChunks > 0 && (retrieved.chunks == NULL || retrieved.containerIds == NULL)) {
        printf("File %lu chunk (%p) or container (%p) not retrieved\n", i, retrieved.chunks, retrieved.containerIds);
        return false;
    }
    // file chunks
    for (int c = 0; c < origin.numChunks; c++) {
        // chunk meta
        if (origin.chunks[c].namespaceId != retrieved.chunks[c].namespaceId ||
                origin.chunks[c].fuuid != retrieved.chunks[c].fuuid ||
                origin.chunks[c].chunkId != retrieved.chunks[c].chunkId ||
                origin.chunks[c].size != retrieved.chunks[c].size
        ) {
            printf("File %lu chunk %d metadata mismatched "
                    "(namespace Id %d vs %d) "
                    "(fuuid %s vs %s) "
                    "(chunkId %d vs %d) "
                    "(size %d vs %d) "
                    "\n"
                    , i, c
                    , retrieved.chunks[c].namespaceId, origin.chunks[c].namespaceId
                    , boost::uuids::to_string(retrieved.chunks[c].fuuid).c_str(), boost::uuids::to_string(origin.chunks[c].fuuid).c_str()
                    , retrieved.chunks[c].chunkId, origin.chunks[c].chunkId
                    , retrieved.chunks[c].size, origin.chunks[c].size
            );
            return false;
        }
        // container ids
        if (origin.containerIds[c] != retrieved.containerIds[c]) {
            printf("File %lu container %d id mismatched (%d vs %d)\n", i, c, retrieved.containerIds[c], origin.containerIds[c]);
            return false;
        }
    }
    // file timestamps
    if (origin.ctime != retrieved.ctime ||
            origin.atime != retrieved.atime ||
            origin.mtime != retrieved.mtime ||
            origin.tctime != retrieved.tctime
    ) {
        printf("File %lu timestamps mismatched "
                "(ctime %ld vs %ld) "
                "(atime %ld vs %ld) "
                "(mtime %ld vs %ld) "
                "(tctime %ld vs %ld) "
                "\n"
                , i
                , retrieved.ctime, origin.ctime
                , retrieved.atime, origin.atime
                , retrieved.mtime, origin.mtime
                , retrieved.tctime, origin.tctime
        );
        return false;
    }
    // file version
    if (origin.version != retrieved.version) {
        printf("File %lu version mismatched (%d vs %d)\n", i, retrieved.version, origin.version);
        return false;
    }
    // file status
    //if (origin.status != retrieved.status) {
    //    printf("File %d status mismatched (%d vs %d)\n", i, origin.status, retrieved.status);
    //    return false;
    //}
    // file checksum
    if (memcmp(origin.md5, retrieved.md5, MD5_DIGEST_LENGTH) != 0) {
        printf("File %lu md5 mismatched (%s vs %s)\n"
            , i
            , ChecksumCalculator::toHex(retrieved.md5, MD5_DIGEST_LENGTH).c_str()
            , ChecksumCalculator::toHex(origin.md5, MD5_DIGEST_LENGTH).c_str()
        );
        return false;
    }
    // file coding parameters
    if (origin.codingMeta.coding != retrieved.codingMeta.coding ||
            origin.codingMeta.k != retrieved.codingMeta.k ||
            origin.codingMeta.n != retrieved.codingMeta.n ||
            origin.codingMeta.codingStateSize != retrieved.codingMeta.codingStateSize
    ) {
        printf("File %lu coding parameters mismatched"
                "(coding %d vs %d) "
                "(k %d vs %d) "
                "(n %d vs %d) "
                "(codingState size %d vs %d) "
                "\n"
                , i
                , retrieved.codingMeta.coding, origin.codingMeta.coding
                , retrieved.codingMeta.k, origin.codingMeta.k
                , retrieved.codingMeta.n, origin.codingMeta.n
                , retrieved.codingMeta.codingStateSize, origin.codingMeta.codingStateSize
        );
        return false;
    }
    if (
        (retrieved.codingMeta.codingStateSize > 0 && retrieved.codingMeta.codingState == NULL)
    ) {
        printf("File %lu coding state (%p) not found\n", i, retrieved.codingMeta.codingState);
        return false;
    }
    if (retrieved.codingMeta.codingStateSize > 0 && memcmp(origin.codingMeta.codingState, retrieved.codingMeta.codingState, retrieved.codingMeta.codingStateSize) != 0) {
        printf("File %lu coding state mismatched\n", i);
        return false;
    }
    return true;
}

static void exitWithError() {
    // clean up files
    for (size_t i = 0; i < numFilesToTest; i++)
        metastore->deleteMeta(f[i]);
    // exit with non-zero value
    exit(1);
}

static void readAndCheckFileMeta() {
    for (size_t i = 0; i < numFilesToTest; i++) {
        // get metadata
        File rf;
        rf.copyNameAndSize(f[i]);
        if (!metastore->getMeta(rf)) {
            printf(">> Failed to get file %lu metadata\n", i);
            exitWithError();
        }
        // check metadata
        if (!compareFile(i, f[i], rf)) {
            exitWithError();
        }
    }
}

