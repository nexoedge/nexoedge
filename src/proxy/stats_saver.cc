// SPDX-License-Identifier: Apache-2.0

#include <glog/logging.h>

#include "stats_saver.hh"
#include "../common/config.hh"

StatsSaver::StatsSaver() {
    Config &config = Config::getInstance();

    _cxt = NULL;

    // do nothing if stats saving is disabled
    if (!config.sendStatsToReporterDB())
        return;

    // init connection to reporter db (redis)
    if (!connectToRedis())
        exit(1);

    _statsQueue = new RingBuffer<nlohmann::json*>(config.getProxyReporterDBRecordBufferSize());
    if (_statsQueue == NULL) {
        LOG(ERROR) << "Failed to allocate buffer queue for stats records";
        exit(1);
    }

    // run the saver thread
    _running = true;
    pthread_create(&_rt, NULL, StatsSaver::run, this);

}

StatsSaver::~StatsSaver() {
    // do nothing if stats saving is disabled
    if (!Config::getInstance().sendStatsToReporterDB())
        return;

    // stop the saver thread
    _running = false;
    pthread_join(_rt, NULL);
}

void StatsSaver::saveStatsRecord(const std::map<std::string, double> &stats, const char *opType, const std::string opFile, double opStartTime, double opEndTime, const std::string dstFile) {
    Config &config = Config::getInstance();

    // skip if stats saving is disabled
    if (!config.sendStatsToReporterDB())
        return;

    // construct the json object
    nlohmann::json *j = new nlohmann::json();
    *j = stats;
    j->push_back(nlohmann::json::object_t::value_type("opType", opType));
    j->push_back(nlohmann::json::object_t::value_type("opFile", opFile.c_str()));
    j->push_back(nlohmann::json::object_t::value_type("opStart", opStartTime));
    j->push_back(nlohmann::json::object_t::value_type("opEnd", opEndTime));
    if (!dstFile.empty())
        j->push_back(nlohmann::json::object_t::value_type("opDstFile", dstFile));

    queueRecord(j);
}



void StatsSaver::queueRecord(nlohmann::json* rec) {
    // do nothing if stats saving is disabled
    if (!Config::getInstance().sendStatsToReporterDB())
        return;

    // insert record to buffer queue
    _statsQueue->Insert(&rec, sizeof(rec));
}

void* StatsSaver::run(void *param) {
    nlohmann::json *rec = 0;

    StatsSaver *self = static_cast<StatsSaver*>(param);

    while(self->_running) {
        // extract record and send to reporter db
        self->_statsQueue->Extract(&rec);
        self->saveToDB(rec);
        // release the record
        delete rec;
    }

    return (void *) 0;
}

void StatsSaver::saveToDB(nlohmann::json* rec) {
    if (_cxt->err && !connectToRedis()) {
        LOG(WARNING) << "Failed to send stats to reporter DB due to Redis connection error";
        return;
    }

    _lock.lock();

    // push the record to reporter db
    redisReply *r = (redisReply *) redisCommand(
        _cxt
        , "RPUSH ncloud_activity_hist %s"
        , rec->dump().c_str()
    );

    if (r == NULL) {
        LOG(WARNING) << "Failed to save record, " << *rec;
        redisReconnect(_cxt);
        return;
    }

    freeReplyObject(r);

    _lock.unlock();
}

bool StatsSaver::connectToRedis() {
    Config &config = Config::getInstance();

    if (_cxt)
        redisFree(_cxt);

    _cxt = redisConnect(config.getProxyReporterDBIP().c_str(), config.getProxyReporterDBPort());
    if (_cxt == NULL || _cxt->err) {
        if (_cxt) {
            LOG(ERROR) << "StatsSaver Redis connection error " << _cxt->errstr;
            redisFree(_cxt);
        } else {
            LOG(ERROR) << "Failed to allocate Redis context in StatsSaver";
        }

        _cxt = NULL;
        return false;
    }

    LOG(INFO) << "StatsSaver Redis connection init";
    return true;
}
