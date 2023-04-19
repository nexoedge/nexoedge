// SPDX-License-Identifier: Apache-2.0

#ifndef __BG_CHUNK_HANDLER_HH__
#define __BG_CHUNK_HANDLER_HH__

#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <zmq.hpp>

#include "io.hh"
#include "metastore/metastore.hh"
#include "../common/config.hh"
#include "../common/define.hh"
#include "../ds/chunk_event.hh"
#include "../ds/file.hh"

class BgChunkHandler {
public:
    struct ChunkTask;
    struct TaskQueue;

    BgChunkHandler(ProxyIO *io, MetaStore *metastore, bool *running, TaskQueue *queue = 0);
    ~BgChunkHandler();

    struct ChunkTask {
        Opcode op;
        File *file;
        int numReqs;
        int numBgReqs;
        pthread_t *wt;
        ProxyIO::RequestMeta *meta;
        ChunkEvent *events;
        void *codebuf;
        
        ChunkTask(Opcode op, File *file, int num, int numBg, pthread_t *wt, ProxyIO::RequestMeta *meta, ChunkEvent *events, void *codebuf) {
            this->op = op;
            this->file = file;
            this->numReqs = num;
            this->numBgReqs = numBg;
            this->wt = wt;
            this->meta = meta;
            this->events = events;
            this->codebuf = codebuf;
        }
    };

    struct TaskQueue {
        std::queue<ChunkTask> tasks;                              /**< chunk task queue */
        std::map<std::string, int> fileTaskCount;                 /**< number of task for a file name */
        std::mutex lock;                                          /**< chunk task queue lock */
        std::condition_variable newTask;                          /**< new task arrived */
    };

    /**
     * Add a background chunk task
     *
     * @param[in] task                  chunk task to add
     *
     * @return whether the task is added
     **/
    bool addChunkTask(ChunkTask task);

    /**
     * Main process of each task handling work
     *
     * @param[in] arg                   an instance of background chunk handler
     *
     * @return always 0
     **/
    static void *runWorker(void *arg);

    /**
     * Tell whether there are pending tasks for a file
     * 
     * @param[in] file                  file structure with the name and namespace id of file to check
     * 
     * @return whether there are some pending tasks in the queue for the file
     **/
    bool taskExistsForFile(const File &file);

    /**
     * Get a copy of the onging tasks (for progress report)
     *
     * @param[out] task                 names of tasks
     * @param[out] progress             progress of tasks (in percentage) 
     *
     * @return number of tasks
     **/
    int getTaskProgress(std::string *&task, int *&progress);

private:
    zmq::context_t _cxt;                                          /**< zero-mq context (for dispatching jobs to workers) */
    ProxyIO *_io;                                                 /**< chunk io */
    MetaStore *_metastore;                                        /**< metadata store */
    int _numWorkers;                                              /**< number of background workers to execute the tasks */
    bool *_running;                                               /**< whether the proxy is still running */
    pthread_t *_workers;                                          /**< workers */

    TaskQueue *_queue;                                            /**< task queue */
    bool _freeQueue;                                              /**< whether to free task queue */

    std::string genFileKey(const File &file);
    void getFileKeyParts(const std::string &key, char *&name, int &nameLength, unsigned char &namespaceId);
};

#endif //define __BG_CHUNK_HANDLER_HH__
