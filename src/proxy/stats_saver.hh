#ifndef __STAT_SAVER_HH__
#define __STAT_SAVER_HH__

#include <mutex>

#include <nlohmann/json.hpp>
#include <hiredis/hiredis.h>

#include "../ds/ring_buffer.hh"

class StatsSaver {
public:
    StatsSaver();
    ~StatsSaver();

    /**
     * Save a record (blocking)
     *
     * @param stats                     map of stats
     * @param opType                    operation type
     * @param opFile                    file that operated on
     * @param opStartTime               operation start time
     * @param opEndTime                 operation end time
     * @param dstFile                   destination file (for operations that involves two files, e.g., copy and rename)
     **/
    void saveStatsRecord(const std::map<std::string, double> &stats, const char *opType, const std::string opFile, double opStartTime = time(NULL), double opEndTime = time(NULL), const std::string dstFile = "");

private:
    /**
     * Add a statistics record
     *
     * @param rec                   stats record to add
     **/
    void queueRecord(nlohmann::json* rec);

    /**
     * Start the statistic saver
     *
     * @param param                 an instance of StatsSaver
     * @return void*(0)
     **/
    static void* run(void *param);

    bool connectToRedis();
    void saveToDB(nlohmann::json* rec);

    RingBuffer<nlohmann::json*> *_statsQueue;       /**< stats record queue */

    redisContext *_cxt;                             /**< redis reporter db */
    std::mutex _lock;                               /**< redis connection lock */

    pthread_t _rt;                                  /**< thread for running */

    bool _running;                                  /**< whether the instance is running */
};

#endif //define __STAT_SAVER_HH__
