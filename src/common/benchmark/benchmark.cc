// SPDX-License-Identifier: Apache-2.0

#include <limits.h>

#include "../../common/config.hh"
#include "benchmark.hh"

/********************************* BaseBMInstance *****************************************/
BaseBMInstance::BaseBMInstance() {
    _size = 0;
}

bool BaseBMInstance::setMeta(unsigned long int size) {
    if (size < 0) {
        LOG(FATAL) << "invalid size: " << size;
        return false;
    }
    
    // copy instance meta
    _size = size;
    
    return true;
}

std::string BaseBMInstance::log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, const char *format, va_list &arglist, const std::string upperLevelStr) {
    std::stringstream ss;
    ss.clear();
    ss << std::setprecision (5);
    ss << "[" << eventName << "] "; // event name

    char _fmtStrBuffer[MAX_LOGMSG_FMT_LENGTH];
    vsnprintf(_fmtStrBuffer, MAX_LOGMSG_FMT_LENGTH, format, arglist);
    if (strlen(_fmtStrBuffer) > 0)
        ss << "\"" << _fmtStrBuffer << "\" ";

    ss << upperLevelStr; // append string

    double usedTime = (endTime - startTime).sec();
    if (usedTime > 0) {
        ss << "time: " << usedTime << "s (" << startTime << ", " << endTime << ") ";
        if (_size > 0) {
            double sizeMB = byte2MB(_size);
            ss << "size: " << sizeMB << "MB ";
            ss << "speed: " << sizeMB / usedTime << "MB/s ";
        }
    }

    return ss.str();
}

/********************************* BaseBMUnit *****************************************/

BaseBMUnit::BaseBMUnit() {
    _func = NULL;
}

bool BaseBMUnit::setMeta(int id, unsigned long int size, BaseBMFunc *func) {

    if (!BaseBMInstance::setMeta(size)) {
        return false;
    }
    _id = id;
    _func = func;
    
    return true;
}

bool BaseBMUnit::isValid() {
    return (_id != INVALID_BM_ID);
}

std::string BaseBMUnit::log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, 
                                const char *format, ...) {

    std::stringstream ss;
    ss.clear();

    va_list arglist;
    va_start(arglist, format);

    ss << "reqId: " << _func->getReqId() << " ";
    ss << "id: " << _id << " ";
    ss << "name: " << _func->getName() << " ";

    std::string finalStr = BaseBMInstance::log(startTime, endTime, eventName, format, arglist, ss.str());
    va_end(arglist);
    
    return finalStr;
}

/********************************* BaseBMFunc *****************************************/

BaseBMFunc::BaseBMFunc() {
    _size = -1;
    _reqId = INVALID_BM_ID;
    _numAgents = -1;
}

bool BaseBMFunc::setMeta(int reqId, unsigned long int size, const char *name, int nameLength, int numAgents) {

    if (!BaseBMInstance::setMeta(size)) {
        return false;
    }

    // copy file meta
    _reqId = reqId;

    if (nameLength <= 0 || nameLength >= MAX_FILENAME_LENGTH) {
        LOG(FATAL) << "invalid name length";
        return false;
    }

    if (numAgents < 0) { // if it is used in client, enter 0 since it do not need to consider number of agents
        LOG(FATAL) << "invalid number of agents";
        return false;
    }

    _nameLength = nameLength;
    memcpy(_name, name, _nameLength);
    _name[_nameLength] = '\0';
    _numAgents = numAgents;

    return true;
};

bool BaseBMFunc::updateNumAgents(int numAgents) {
    if (numAgents < 0) {
        LOG(FATAL) << "invalid number of agents";
        return false;
    }

    _numAgents = numAgents;

    return true;
}

bool BaseBMFunc::hasStripeFunc() {
    switch(_type) {
        case Type::WRITE:
        case Type::READ:
        case Type::STAGED_WRITE:
        case Type::STAGED_READ:
            return true;

        default:
            return false;
    }
}

std::string BaseBMFunc::log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, 
                                const char *format, ...) {

    std::stringstream ss;
    ss.clear();

    va_list arglist;
    va_start(arglist, format);

    ss << "reqId: " << _reqId << " ";
    ss << "name: " << _name << " ";

    std::string finalStr = BaseBMInstance::log(startTime, endTime, eventName, format, arglist, ss.str());
    va_end(arglist);

    return finalStr;
}

void BaseBMFunc::printStats(std::map<std::string, double>* tvMap) {

    double fileSizeMB = byte2MB(_size);
    double tTotal = proxyOverallTime.usedTime();

    std::stringstream ss;
    ss.clear();

    ss << std::fixed << std::setprecision(5) << "\n"
          "--------------------------------- " << BaseBMFunc::typeToString(_type) << " ----------------------------------\n"
          " Request Id: " << _reqId << "\n"
          " File Size (MB): " << fileSizeMB << "MB\n"
          " Sub-Task Name      " << ((_size != 0) ? "Speed(MB/s)" : "Time(s)") << "\n";

    if (tvMap != NULL) { // it maybe null, since no information needs to be print
        size_t maxLength = 0;
        for (auto const& x : *tvMap) {
            maxLength = x.first.length() > maxLength ? x.first.length() : maxLength;
        }

        for (auto const& x : *tvMap) {
            std::string space(maxLength - x.first.length(), ' ');
            ss << "  - " << x.first << space << ": " << x.second << "\n";
        }
    }

    ss << "------------------------------- Overall ------------------------------\n"
          " " << BaseBMFunc::typeToString(_type) << ": " << tTotal << 
          "s ("<< proxyOverallTime.getStart().sec() << ", " << proxyOverallTime.getEnd().sec() << ")  ";

    if (_size != 0) {
        ss << fileSizeMB / tTotal << " MB/s\n";
    } else {
        ss << tTotal << " s\n";
    }
    
    ss << "\n\n";

    LOG(INFO) << ss.str();
}

/********************************* BMStripeFunc *****************************************/
BMStripeFunc::BMStripeFunc() {
    Config &config = Config::getInstance();

    _numStripes = -1;
    _bmStripe = NULL;
    _stripeOn = config.getBenchmarkStripeEnabled();
}

BMStripeFunc::~BMStripeFunc() {
    if (_bmStripe && (_numStripes > 0)) {
        for (int i = 0; i < _numStripes; i++) {
            if (_bmStripe->at(i))
                delete _bmStripe->at(i);
        }
        delete _bmStripe;
    }
}

bool BMStripeFunc::setStripes(int numStripes) {
    if (numStripes <= 0) {
        LOG(ERROR) << "invalid numStripes: " << numStripes;
        return false;
    }

    if (numStripes > 0) {
        std::vector<BMStripe *> *newStripes = new std::vector<BMStripe *>(numStripes);
        if (_bmStripe) { 
            // copy existing results if any, discard if overflows
            int i = 0, copy = std::min(numStripes, _numStripes); 
            for (i = 0; i < copy; i++)
                newStripes->at(i) = _bmStripe->at(i);
            for (; i < _numStripes; i++) {
                delete _bmStripe->at(i);
            }
            // release the old vector
            delete _bmStripe;
        }
        _bmStripe = newStripes;
    }

    _numStripes = numStripes;

    if (!_bmStripe) {
        LOG(ERROR) << "no memory to allocate BMStripe vector";
        _stripeOn = false;
        return false;
    }


    bool failed = false;
    for (int i = 0; i < numStripes; i++) {
        BMStripe *instance;

        switch(_type) {
            case Type::WRITE:
            case Type::STAGED_WRITE:
                instance = static_cast<BMStripe *>(new BMWriteStripe());
                break;

            case Type::READ:
            case Type::STAGED_READ:
                instance = static_cast<BMStripe *>(new BMReadStripe());
                break;

            case Type::REPAIR:
                instance = static_cast<BMStripe *>(new BMRepairStripe());
                break;

            default:
                instance = static_cast<BMStripe *>(new BMStripe());
        }

        if (!instance) {
            LOG(ERROR) << "no memory to allocate BMStripe";
            failed = true;
            break;
        }

        _bmStripe->at(i) = instance;
    }

    // free all stripe before proceed
    if (failed) {
        for (int i = 0; i < numStripes; i++) {
            BMStripe *instance = _bmStripe->at(i);
            if (instance) {
                delete instance;
            }
        }
        delete _bmStripe;

        _stripeOn = false;
        return false;
    }

    return true;
}

BMStripe &BMStripeFunc::at(int idx) {
    return *_bmStripe->at(idx);
}

/********************************* BMStripe *****************************************/

BMStripe::BMStripe() {
    agentProcess = NULL;
    network = NULL;
}

BMStripe::~BMStripe() {
    if (_func && (_func->getNumAgents() > 0)) {
        delete agentProcess;
        delete network;
    }
}

bool BMStripe::setMeta(int id, unsigned long int size, BaseBMFunc *func) {
    BaseBMUnit::setMeta(id, size, func);

    int numAgents = _func->getNumAgents();

    if (numAgents < 0) {
        LOG(ERROR) << "invalid numAgents: " << numAgents;
        return false;
    }

    // skip allocating resources if no Agents are involved
    if (numAgents == 0)
        return true;

    // avoid memory leaks due to multiple calls
    if (agentProcess != 0) 
        delete agentProcess;
    if (network != 0)
        delete network;

    agentProcess = new std::vector<TagPt>(numAgents, TagPt());
    network = new std::vector<TagPt>(numAgents, TagPt());

    return true;
}

/********************************* BMWrite *****************************************/

BMWrite::BMWrite() {
    _type = WRITE;
}

BMWriteStripe& BMWrite::at(int idx) {
    BMStripe *stripe = &BMStripeFunc::at(idx);

    return *static_cast<BMWriteStripe *>(stripe);
}

std::map<std::string, double>* BMWrite::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    tvMap->insert(std::pair<std::string, double>("fileSize", getSizeMB()));
    tvMap->insert(std::pair<std::string, double>("(File)initBuffer", getSizeMB() / initBuffer.usedTime()));
    tvMap->insert(std::pair<std::string, double>("(File)updateMeta", getSizeMB() / updateMeta.usedTime()));

    // initialize file stats
    double networkRTOverall = 0.0;

    // initialize stripe stats
    if (isStripeOn()) {
        double preparation = 0.0;
        double encode = 0.0;
        double prepareChunks = 0.0;
        double networkRT = 0.0;
        double replyCheck = 0.0;
        // time
        double encodeToUpload = 0.0;
        double prepToEncode = 0.0;
        double uploadToFinalize = 0.0;
        double uploadTime = 0.0;
        double overallTime = 0.0;
        double networkTime = 0.0;
        double encodeTime = 0.0;
        double replyCheckTime = 0.0;
        double tmp = 0.0;
        std::vector<double> agentProcessVec(_numAgents, 0.0);
        std::vector<double> networkRTVec(_numAgents, 0.0);
        std::vector<double> networkVec(_numAgents, 0.0);
        std::vector<TagPt> networkStripes(_numStripes);

        std::vector<std::vector<TagPt> > networkRTAgents(_numAgents, std::vector<TagPt>(_numStripes));
        std::vector<std::vector<TagPt> > agentProcessAgents(_numAgents, std::vector<TagPt>(_numStripes));

        // Add up stripe values
        for (int i = 0; i < _numStripes; i++) {
            BMWriteStripe &bmStripe = this->at(i);
            double fileSizeMB = bmStripe.getSizeMB();
            preparation += (fileSizeMB / bmStripe.preparation.usedTime());
            encode += (fileSizeMB / bmStripe.encode.usedTime());
            prepareChunks += (fileSizeMB / bmStripe.prepareChunks.usedTime());
            networkRT += (fileSizeMB / bmStripe.networkRT.usedTime());
            replyCheck += (fileSizeMB / bmStripe.replyCheck.usedTime());

            encodeToUpload += bmStripe.encodeToUpload.usedTime();
            prepToEncode += bmStripe.prepToEncode.usedTime();
            uploadToFinalize += bmStripe.uploadToFinalize.usedTime();
            uploadTime += bmStripe.upload.usedTime();
            overallTime += bmStripe.overallTime.usedTime();
            networkTime += bmStripe.networkRT.usedTime();
            encodeTime += bmStripe.encode.usedTime();
            replyCheckTime += bmStripe.replyCheck.usedTime();
            tmp += bmStripe.temp.usedTime();

            for (int j = 0; j < _numAgents; j++) {
                networkRTAgents.at(j).at(i) = bmStripe.network->at(j);
                agentProcessAgents.at(j).at(i) = bmStripe.agentProcess->at(j);
            }
            
            std::vector<double> *speedVec = 0;
            Benchmark::vecTime2Speed(&speedVec, bmStripe.agentProcess, bmStripe.getSize());
            Benchmark::vecMatrixAdd(&agentProcessVec, speedVec);
            delete speedVec;

            Benchmark::vecTime2Speed(&speedVec, bmStripe.network, bmStripe.getSize());
            Benchmark::vecMatrixAdd(&networkRTVec, speedVec);
            delete speedVec;

            Benchmark::vecTime2SpeedWithOverlap(&speedVec, bmStripe.network, bmStripe.agentProcess, bmStripe.getSize());
            Benchmark::vecMatrixAdd(&networkVec, speedVec);
            delete speedVec;

            networkStripes.at(i) = Benchmark::findGap(bmStripe.network);
        }
        networkRTOverall = getSizeMB() / Benchmark::findUt(&networkStripes).sec();

        // Normalization
        preparation /= _numStripes;
        encode /= _numStripes;
        prepareChunks /= _numStripes;
        networkRT /= _numStripes;
        replyCheck /= _numStripes;

        Benchmark::vecMatrixMultiply(&agentProcessVec, 1.0 / _numStripes);
        Benchmark::vecMatrixMultiply(&networkRTVec, 1.0 / _numStripes);
        Benchmark::vecMatrixMultiply(&networkVec, 1.0 / _numStripes);

        // Insert to map
        std::stringstream ss;
        tvMap->insert(std::pair<std::string, double>("(StripeAvg)preparation", preparation));
        tvMap->insert(std::pair<std::string, double>("(StripeAvg)encode", encode));
        tvMap->insert(std::pair<std::string, double>("(StripeAvg)prepareChunks", prepareChunks));
        tvMap->insert(std::pair<std::string, double>("(StripeAvg)networkRT", networkRT));
        //tvMap->insert(std::pair<std::string, double>("replyCheck", replyCheck));
        tvMap->insert(std::pair<std::string, double>("(File)networkRTOverall", networkRTOverall));

        for (int i = 0; i < _numAgents; i++) {
            std::stringstream().swap(ss);
            ss << "(ChunkAvg)agentProcess_" << i;
            std::string key = ss.str();
            tvMap->insert(std::pair<std::string, double>(ss.str(), agentProcessVec.at(i)));
            
            std::stringstream().swap(ss);
            ss << "(ChunkAvg)networkRT_" << i;
            key = ss.str();
            tvMap->insert(std::pair<std::string, double>(ss.str(), networkRTVec.at(i)));

            std::stringstream().swap(ss);
            ss << "(ChunkAvg)network_" << i;
            key = ss.str();
            tvMap->insert(std::pair<std::string, double>(ss.str(), networkVec.at(i)));

            // std::stringstream().swap(ss);
            // ss << "network_round_trip_overall_" << i;
            // key = ss.str();
            // for (int j = 0; j < _numStripes; j++) {
            //     LOG(INFO) << networkRTAgents.at(i).at(j);
            // }
            // double timeNetworkRT = Benchmark::findUt(&networkRTAgents.at(i)).sec();
            // LOG(INFO) << "networkRT time(" << i << "): " << timeNetworkRT;
            // LOG(INFO) << "networkRT speed(" << i << "): " << getSizeMB() / _numAgents / timeNetworkRT;
            // tvMap->insert(std::pair<std::string, double>(ss.str(), getSizeMB() / _numAgents / timeNetworkRT));

            // std::stringstream().swap(ss);
            // ss << "agentProcess_overall_" << i;
            // key = ss.str();
            // for (int j = 0; j < _numStripes; j++) {
            //     LOG(INFO) << agentProcessAgents.at(i).at(j);
            // }
            // double timeAgentProcess = Benchmark::findUt(&agentProcessAgents.at(i)).sec();
            // LOG(INFO) << "agentProcess time(" << i << "): " << timeAgentProcess;
            // LOG(INFO) << "agentProcess speed(" << i << "): " << getSizeMB() / _numAgents / timeAgentProcess;
            // tvMap->insert(std::pair<std::string, double>(ss.str(), getSizeMB() / _numAgents / timeAgentProcess));

            // std::stringstream().swap(ss);
            // ss << "network_overall_" << i;
            // key = ss.str();
            // tvMap->insert(std::pair<std::string, double>(ss.str(), getSizeMB() / _numAgents / (timeNetworkRT - timeAgentProcess)));
        }

        
        tvMap->insert(std::pair<std::string, double>("Num. of strpies", _numStripes));
        tvMap->insert(std::pair<std::string, double>("Agg. time - temp tap point (s)", tmp));
        tvMap->insert(std::pair<std::string, double>("Agg. time - init buffer (s)", initBuffer.usedTime()));
        tvMap->insert(std::pair<std::string, double>("Agg. time - metadata (s)", updateMeta.usedTime()));
        tvMap->insert(std::pair<std::string, double>("Agg. time - upload (s)", uploadTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - prep-to-encode (s)", prepToEncode));
        tvMap->insert(std::pair<std::string, double>("Agg. time - encode-to-upload (s)", encodeToUpload));
        tvMap->insert(std::pair<std::string, double>("Agg. time - upload-to-collect (s)", uploadToFinalize));
        tvMap->insert(std::pair<std::string, double>("Agg. time - encode", encodeTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - network", networkTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - replyCheck", replyCheckTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - overall", overallTime));
        tvMap->insert(std::pair<std::string, double>("Total time - metadata", updateMeta.usedTime()));
    }
    
    return tvMap;
}

/********************************* BMRead *****************************************/

BMRead::BMRead() {
    _type = READ;
}

BMReadStripe& BMRead::at(int idx) {
    BMStripe *stripe = &BMStripeFunc::at(idx);

    return *static_cast<BMReadStripe *>(stripe);
}

std::map<std::string, double>* BMRead::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    tvMap->insert(std::pair<std::string, double>("fileSize", getSizeMB()));

    // initialize
    if (isStripeOn()) {
        std::vector<double> agentProcessVec(_numAgents, 0.0);
        std::vector<double> networkVec(_numAgents, 0.0);
        std::vector<TagPt> networkStripes(_numStripes);
        double replyCheck = 0.0;
        double preparation = 0.0;
        double preparationTime = 0.0;
        double decode = 0.0;
        double decodeTime = 0.0;
        double downloadTime = 0.0;
        double networkTime = 0.0;
        double replyCheckTime = 0.0;
        double stripeAggTime = 0.0;

        // Add up stripe values
        for (int i = 0; i < _numStripes; i++) {
            BMReadStripe &bmStripe = this->at(i);
            double fileSizeMB = bmStripe.getSizeMB();

            std::vector<double> *speedVec = 0;
            Benchmark::vecTime2Speed(&speedVec, bmStripe.agentProcess, bmStripe.getSize());
            Benchmark::vecMatrixAdd(&agentProcessVec, speedVec);
            delete speedVec;

            Benchmark::vecTime2Speed(&speedVec, bmStripe.network, bmStripe.getSize());
            Benchmark::vecMatrixAdd(&networkVec, speedVec);
            delete speedVec;

            replyCheck += (fileSizeMB / bmStripe.replyCheck.usedTime());
            preparation += (fileSizeMB / bmStripe.preparation.usedTime());
            decode += (fileSizeMB / bmStripe.decode.usedTime());

            networkStripes.at(i) = Benchmark::findGap(bmStripe.network);

            preparationTime += bmStripe.preparation.usedTime() * 1e3;
            downloadTime += bmStripe.download.usedTime();
            networkTime += networkStripes.at(i).usedTime();
            decodeTime += bmStripe.decode.usedTime();
            replyCheckTime += bmStripe.replyCheck.usedTime();

            stripeAggTime += bmStripe.overall.usedTime();
        }


        // Normalization
        Benchmark::vecMatrixMultiply(&agentProcessVec, 1.0 / _numStripes);
        Benchmark::vecMatrixMultiply(&networkVec, 1.0 / _numStripes);
        replyCheck /= _numStripes;
        preparation /= _numStripes;
        decode /= _numStripes;

        // Insert to map
        std::stringstream ss;
        for (int i = 0; i < _numAgents; i++) {
            std::stringstream().swap(ss);
            ss << "agentProcess_" << i;
            std::string key = ss.str();
            tvMap->insert(std::pair<std::string, double>(ss.str(), agentProcessVec.at(i)));
            
            std::stringstream().swap(ss);
            ss << "network_" << i;
            key = ss.str();
            tvMap->insert(std::pair<std::string, double>(ss.str(), networkVec.at(i)));
        }

        tvMap->insert(std::pair<std::string, double>("replyCheck", replyCheck));
        tvMap->insert(std::pair<std::string, double>("preparation", preparation));
        tvMap->insert(std::pair<std::string, double>("decode", decode));
        tvMap->insert(std::pair<std::string, double>("Agg. time - prep. (ms)", preparationTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - download (s)", downloadTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - network (s)", networkTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - reply (s)", replyCheckTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - decode (s)", decodeTime));
        tvMap->insert(std::pair<std::string, double>("Agg. time - overall (s)", stripeAggTime));
        tvMap->insert(std::pair<std::string, double>("Total time - metadata (ms)", metadata * 1e3));
    }

    return tvMap;
}

/********************************* BMRepair *****************************************/
BMRepair::BMRepair() {
    _type = REPAIR;
}

BMRepairStripe& BMRepair::at(int idx) {
    BMStripe *stripe = &BMStripeFunc::at(idx);

    return *static_cast<BMRepairStripe *>(stripe);
}

std::map<std::string, double>* BMRepair::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    unsigned long int totalRepairSize = 0;

    for (int i = 0; i < _numStripes; i++) {
        BMRepairStripe &bmStripe = this->at(i);
        totalRepairSize += bmStripe.getRepairSize();
    }

    _size = totalRepairSize;
    double totalRepairSizeMB = getSizeMB();

    tvMap->insert(std::pair<std::string, double>("fileSize", totalRepairSizeMB));

    tvMap->insert(std::pair<std::string, double>("getMeta", totalRepairSizeMB / getMeta.usedTime()));
    tvMap->insert(std::pair<std::string, double>("dataRepair", totalRepairSizeMB / dataRepair.usedTime()));
    tvMap->insert(std::pair<std::string, double>("updateMeta", totalRepairSizeMB / updateMeta.usedTime()));

    return tvMap;
}

void BMRepairStripe::setRepairSize(unsigned long int size) {
    _repairSize = size;
}

unsigned long int BMRepairStripe::getRepairSize() {
    return _repairSize;
}


/********************************* BMStaging *****************************************/



/********************************* BMStagingWrite *****************************************/

/*
BMStagingWrite::BMStagingWrite() {
    _type = STAGING_WRITE;
}

BMStagingWriteStripe& BMStagingWrite::at(int idx) {
    BMStripe *stripe = &BMStripeFunc::at(idx);

    return *static_cast<BMStagingWriteStripe *>(stripe);
}

std::map<std::string, double>* BMStagingWrite::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    tvMap->insert(std::pair<std::string, double>("fileSize", getSizeMB()));
    tvMap->insert(std::pair<std::string, double>("(File)initMeta", getSizeMB() / initMeta.usedTime()));
    tvMap->insert(std::pair<std::string, double>("(File)updateMeta", getSizeMB() / updateMeta.usedTime()));

    if (isStripeOn()) {
        double preparation = 0.0;
        double writeToStorage = 0.0;
        double finalize = 0.0;

        for (int i = 0; i < _numStripes; i++) {
            BMStagingWriteStripe &bmStripe = this->at(i);
            double stripeSizeMB = bmStripe.getSizeMB();

            preparation += (stripeSizeMB / bmStripe.preparation.usedTime());
            writeToStorage += (stripeSizeMB / bmStripe.writeToStorage.usedTime());
            finalize += (stripeSizeMB / bmStripe.finalize.usedTime());
        }
        
        preparation /= _numStripes;
        writeToStorage /= _numStripes;
        finalize /= _numStripes;

        tvMap->insert(std::pair<std::string, double>("(Stripe)preparation", preparation));
        tvMap->insert(std::pair<std::string, double>("(Stripe)writeToStorage", writeToStorage));
        tvMap->insert(std::pair<std::string, double>("(Stripe)finalize", finalize));
    }

    return tvMap;
}

*/
/********************************* BMStagingRead *****************************************/

/*
BMStagingRead::BMStagingRead() {
    _type = STAGING_READ;
}

BMStagingReadStripe& BMStagingRead::at(int idx) {
    BMStripe *stripe = &BMStripeFunc::at(idx);

    return *static_cast<BMStagingReadStripe *>(stripe);
}

std::map<std::string, double>* BMStagingRead::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    tvMap->insert(std::pair<std::string, double>("fileSize", getSizeMB()));
    tvMap->insert(std::pair<std::string, double>("updateMeta", getSizeMB() / updateMeta.usedTime()));

    if (isStripeOn()) {
        double preparation = 0.0;
        double readFromStorage = 0.0;
        double finalize = 0.0;

        for (int i = 0; i < _numStripes; i++) {
            BMStagingReadStripe &bmStripe = this->at(i);
            double stripeSizeMB = bmStripe.getSizeMB();

            preparation += (stripeSizeMB / bmStripe.preparation.usedTime());
            readFromStorage += (stripeSizeMB / bmStripe.readFromStorage.usedTime());
            finalize += (stripeSizeMB / bmStripe.finalize.usedTime());
        }
        
        preparation /= _numStripes;
        readFromStorage /= _numStripes;
        finalize /= _numStripes;

        tvMap->insert(std::pair<std::string, double>("preparation", preparation));
        tvMap->insert(std::pair<std::string, double>("readFromStorage", readFromStorage));
        tvMap->insert(std::pair<std::string, double>("finalize", finalize));
    }

    return tvMap;
}
*/

/********************************* BMStagingDelete *****************************************/

/*
BMStagingDelete::BMStagingDelete() {
    _type = STAGING_DELETE;
}

std::map<std::string, double>* BMStagingDelete::calcStats() {
    std::map<std::string, double>* tvMap = new std::map<std::string, double>();

    // NOTE: here we directly print out the time instead of the speed
    tvMap->insert(std::pair<std::string, double>("preparation", preparation.usedTime()));
    tvMap->insert(std::pair<std::string, double>("deleteFromStorage", deleteFromStorage.usedTime()));
    return tvMap;
}
*/



/********************************* Benchmark *****************************************/
void Benchmark::clear() {
    std::lock_guard<std::mutex> lk(_req2BMMapLock);
    _req2BMMap.clear();
}

bool Benchmark::add(int reqId, BaseBMFunc *baseBMFunc) {
    std::lock_guard<std::mutex> lk(_req2BMMapLock);

    std::map<int, BaseBMFunc *>::iterator it = _req2BMMap.find(reqId);
    if (it != _req2BMMap.end()) {
        DLOG(WARNING) << "Failed to find reqId: " << reqId;
        return false;
    }
    _req2BMMap.insert(std::pair<int, BaseBMFunc *>(reqId, baseBMFunc));
    
    return true;
}

bool Benchmark::remove(int reqId) {
    std::lock_guard<std::mutex> lk(_req2BMMapLock);

    std::map<int, BaseBMFunc *>::iterator it = _req2BMMap.find(reqId);
    if (it == _req2BMMap.end()) {
        DLOG(WARNING) << "Failed to find reqId: " << reqId;
        return false;
    }
    _req2BMMap.erase(it);

    return true;
}

BaseBMFunc *Benchmark::at(int reqId) {
    std::lock_guard<std::mutex> lk(_req2BMMapLock);

    std::map<int, BaseBMFunc *>::iterator it = _req2BMMap.find(reqId);
    if (it == _req2BMMap.end()) {
        DLOG(WARNING) << "Failed to find reqId: " << reqId;
        return 0;
    }
    return it->second;
}

bool Benchmark::replace(int reqId, BaseBMFunc *func) {
    std::map<int, BaseBMFunc *>::iterator it = _req2BMMap.find(reqId);
    if (it == _req2BMMap.end()) {
        DLOG(WARNING) << "Failed to find reqId: " << reqId;
        return false;
    }
    it->second = func;
    return true;
}
