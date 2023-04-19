// SPDX-License-Identifier: Apache-2.0

#ifndef __METASTORE_HH__
#define __METASTORE_HH__

#include <string>
#include <vector>
#include <boost/uuid/uuid.hpp>

#include "../../ds/file.hh"
#include "../../ds/chunk.hh"

class MetaStore {
public:
    MetaStore() {}
    virtual ~MetaStore() {}

    /**
     * Store the file metadata into the metadata store
     *
     * @param[in] f the file structure containing the metadata to store
     *
     * @return whether the metadata is successful stored
     **/
    virtual bool putMeta(const File &f) = 0;

    /**
     * Get the file metadata from the metadata store
     *
     * @param[in,out] f the file structure containing the name and namespace id of the file to get, and other fields would be filled with info from the metadata store
     * @param[in] getBlocks type of blocks fingerprints to get; none = 0, unique only = 1, duplicate only = 2, all = 3; default is 3 (all)
     *
     * @return whether the metadata is successful retrieved
     **/
    virtual bool getMeta(File &f, int getBlocks = 3) = 0;

    /**
     * Delete the file metadata from the metadata store
     *
     * @param[in,out] f the file structure containing the name and namespace id of the file to delete, and other fields would be filled with info from the metadata store
     *
     * @return whether the metadata is successful deleted; if versioning is enabled, return the metadata of data to delete with version number set to != -1
     **/
    virtual bool deleteMeta(File &f) = 0;

    /**
     * Rename the file metadata in the metadata store
     *
     * @param[in] sf the file structure containing the name and namespace id of the file to rename
     * @param[in] df the file structure containing the name and namespace id of the file after rename
     *
     * @return whether the metadata is successful renamed
     **/
    virtual bool renameMeta(File &sf, File &df) = 0;

    /**
     * Update the file time stamp in metadata
     * 
     * @param[in] f the file structure containing the name, namespace id, and updated timestamps (access time and modified time)
     *
     * @return whether the timestamps are successfully updated
     **/
    virtual bool updateTimestamps(const File &f) = 0;

    /**
     * Update the file chunks in metadata
     * 
     * @param[in] f the file structure containing the name, namespace id, version id and updated chunk metadata
     * @param[in] version optional version number of file to check
     *
     * @return 0 if the chunk metadata are successfully updated, 1 if the version is outdated, 2 for other errors
     **/
    virtual int updateChunks(const File &f, int version) = 0;

    /**
     * Get the file name using the file uuid from the metadata store
     *
     * @param[in] uuid         the file uuid
     * @param[in,out] f        the file structure containing the namespace id, and will hold the file name upon success
     *
     * @return whether the file name is found in the metadata store
     **/
    virtual bool getFileName(boost::uuids::uuid fuuid, File &f) = 0;

    /**
     * Get a list of all file names
     *
     * @param[out] list        address of the pointer, which will hold the allocated list of file info (name and size)
     * @param[in]  namespaceId the namespace id of the files to list
     * @param[in]  withSize    whether to include file size in the list
     * @param[in]  withTime    whether to include file timestamps in the list
     * @param[in]  withVersions  whether to include versions in the file info record
     * @param[in]  prefix      the prefix of files to list
     *
     * @return the number of files in the list
     **/
    virtual unsigned int getFileList(FileInfo **list, unsigned char namespaceId = INVALID_NAMESPACE_ID, bool withSize = true, bool withTime = true, bool withVersions = false, std::string prefix = "") = 0;

    /**
     * Get a list of all folder names
     *
     * @param[out] list        folder list holder
     * @param[in]  namespaceId the namespace id of the folders to list
     * @param[in]  prefix      the prefix of folders to list
     * @param[in]  skipSubfolders  whether to skip subfolders
     *
     * @return the number of folders in the list
     **/
    virtual unsigned int getFolderList(std::vector<std::string> &list, unsigned char namespaceId = INVALID_NAMESPACE_ID, std::string prefix = "", bool skipSubfolders = true) = 0;

    /**
     * Tell the maximum number of keys supported
     *
     * @return maximum number of keys supported
     **/
    virtual unsigned long int getMaxNumKeysSupported() = 0;

    /**
     * Tell the number of files stored
     *
     * @return number of files stored 
     **/
    virtual unsigned long int getNumFiles() = 0;

    /**
     * Tell the number of files pending for repair
     *
     * @return number of files pending for repair
     **/
    virtual unsigned long int getNumFilesToRepair() = 0;

    /**
     * Pop a number of file names for repair
     *
     * @param[in] numFiles     number of files to pop for repair
     * @param[out] files       pointer to an array of pre-allocated file structures in size numFiles, which will hold the file name and namespace id of files to repair
     *
     * @return the number of files popped for repair
     **/
    virtual int getFilesToRepair(int numFiles, File files[]) = 0;

    /**
     * Mark file as needs repair
     *
     * @param[in] file          file structure containing the name and namespace id of file to repair
     *
     * @return whether the file is marked as need repair
     **/
    virtual bool markFileAsNeedsRepair(const File &file) = 0;

    /**
     * Mark file as repaired
     *
     * @param[in] file          file structure containing the name and namespace id of file to mark as repaired
     *
     * @return whether the file is marked as repaired
     **/
    virtual bool markFileAsRepaired(const File &file) = 0;

    /**
     * Mark file as pending write to cloud
     *
     * @param[in] file          file structure containing the name and namespace id of file to mark as pending write to cloud
     *
     * @return whether the file is marked
     **/
    virtual bool markFileAsPendingWriteToCloud(const File &file) = 0;

    /**
     * Mark file as written to cloud
     *
     * @param[in] file          file structure containing the name and namespace id of file to mark as written to cloud
     *
     * @return whether the file is marked
     **/
    virtual bool markFileAsWrittenToCloud(const File &file, bool removePending = false) = 0;

    /**
     * Pop a number of file names for pending write to cloud
     *
     * @param[in] numFiles     number of files to pop
     * @param[out] files       pointer to an array of pre-allocated file structures in size numFiles, which will hold the file name and namespace id of files
     *
     * @return the number of files popped
     **/
    virtual int getFilesPendingWriteToCloud(int numFiles, File files[]) = 0;

    /**
     * Change the file status
     *
     * @param[in] file          file structure containing the name, namespace id and new status
     *
     * @return whether the file status is successfully changed
     **/
    virtual bool updateFileStatus(const File &file) = 0;

    /**
     * Return a file for task checking
     *
     * @param[out] file         file structure to hold the name and namespace id of file for task checking 
     *
     * @return whether a file is returned for task checking
     **/
    virtual bool getNextFileForTaskCheck(File &file) = 0;

    /**
     * Lock file
     *
     * @param[in] file          file structure containing the name and namespace id of file to lock
     *
     * @return whether the file is locked
     **/
    virtual bool lockFile(const File &file) = 0;

    /**
     * Unlock file
     *
     * @param[in] file          file structure containing the name and namespace id of file to unlock
     *
     * @return whether the file is unlocked
     **/
    virtual bool unlockFile(const File &file) = 0;

    /**
     * Add a chunk modification to the file journal
     *
     * @param[in] file          file structure containing the name, namespace id, and version of a file
     * @param[in] chunk         chunk structure containing the chunk id, checksum, and size
     * @param[in] container_id  container id
     * @param[in] isWrite       whether the operation is write (or delete otherwise)
     *
     * @return true if the journaling record is added successfully; false otherwise
     **/
    virtual bool addChunkToJournal(const File &file, const Chunk &chunk, int containerId, bool isWrite) = 0;

    /**
     * Update/Remove a chunk modification record in the file journal
     *
     * @param[in] file          file structure containing the name, namespace id, and version of a file
     * @param[in] chunk         chunk structure containing the chunk id, checksum, and size
     * @param[in] container_id  container id for the chunk
     * @param[in] isWrite       whether the operation is write (or delete otherwise)
     * @param[in] delete        whether the record should be deleted instead of updated
     *
     * @return true if the journaling record is updated/removed successfully; false otherwise
     **/
    virtual bool updateChunkInJournal(const File &file, const Chunk &chunk, bool isWrite, bool deleteRecord, int containerId) = 0;

    /**
     * Get the list of journaled chunk modifications of a file
     *
     * @param[in] file          file structure containing the name, namespace id, and version of a file
     * @param[out] records      list of the chunk modifications, each modification contains the chunk metadata (id, size, checksum), container id, whether the modification is a write, and the status of modification (pre-operation or post-operation)
     **/
    virtual void getFileJournal(const FileInfo &file, std::vector<std::tuple<Chunk, int /* container id*/, bool /* isWrite */, bool /* isPre */>> &records) = 0;

     /**
      * Get the list of files with journal
      *
      * @param[out] list        list of file structures containing the name, namespace id and version of a file
      * @return the number of files in the returned list
      **/
    virtual int getFilesWithJounal(FileInfo **list) = 0;

    
    /**
      * Tell whether the file has any journal record
      *
      * @param[in] file         list of file structures containing the name, namespace id, and version of a file
      * @return true if the file has one or more journaled records, false otherwise
     **/
    virtual bool fileHasJournal(const File &file) = 0;

private:

};

#endif // define __METASTORE_HH__
