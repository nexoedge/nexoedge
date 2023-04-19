// SPDX-License-Identifier: Apache-2.0

#ifndef __BENCHMARK_HH__
#define __BENCHMARK_HH__

#include <glog/logging.h>
#include <vector>
#include <map>
#include <mutex>
#include <string>
#include <sstream>
#include <stdarg.h>

#include "benchmark_time.hh"

#define INVALID_BM_ID -777
#define MAX_LOGMSG_FMT_LENGTH 1024
#define MAX_FILENAME_LENGTH 256
#define INVALID_FSIZE -1

class BaseBMInstance {
protected:
    unsigned long int _size;            /* instance size */

public:
    BaseBMInstance();

    TagPt overallTime;                  // overall time

    /**
     * get size (byte)
     * 
     * return unsigned long int: size
     */
    inline const unsigned long int getSize() {
        return _size;
    }

    /**
     * get size (MB)
     * 
     * return double: size
     */
    inline const double getSizeMB() {
        return byte2MB(_size);
    }

    /**
     * set meta
     * 
     * @param size: size
     * 
     * return bool: status
     * 
    */
    bool setMeta(unsigned long int size);

    /**
     * print log
     * 
     * @param startTime: start TimeVal
     * @param endTime: end TimeVal
     * @param format: additional log message
     * 
     * return string: log message
     * 
    */
    std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                                const char *eventName, const char *format, va_list &arglist, const std::string upperLevelStr);

    static double byte2MB(const unsigned long int size) {
        return (size * 1.0 / (1 << 20));
    }
};

class BaseBMFunc : public BaseBMInstance {

public:

    enum Type {
        WRITE,
        MODIFY,
        READ,
        DELETE,
        REPAIR,
        STAGED_WRITE,
        STAGED_READ,
        STAGED_DELETE,
        UNKNOWN
    };

    /**
     * convert BaseBMFunc from Type to char*
     * 
     * @param value: enum
     * 
     * return const char*: corresponding string
     */
    static const char* typeToString(Type value) {
        static const char *table[] = { "Write", "Modify", "Read", "Delete", "Repair", \
             "Staging Write", "Staging Read", "Staging Delete", "Unknown" };
        return table[value];
    }

    BaseBMFunc();
    virtual ~BaseBMFunc() = default;

    /* TagPts  */
    /* overall time */ 
    TagPt clientOverallTime;                                  /* client -> proxy -> agent -> proxy -> client */
    TagPt proxyOverallTime;                                   /* proxy -> agent -> proxy */
    TagPt agentOverallTime;                                   /* agent time */

    inline int getReqId() {
        return _reqId;
    }

    inline const char *getName() {
        return _name;
    }

    bool hasStripeFunc();

    /**
     * set meta
     * 
     * @param reqId: request id
     * @param size: size
     * @param name: name
     * @param nameLength: file name length
     * @param numAgents: number of agents
     * 
     * return bool: status
     * 
    */
    bool setMeta(int reqId, unsigned long int size, const char *name, int nameLength, int numAgents = 0);

    /**
     * print log
     * 
     * @param startTime: start TimeVal
     * @param endTime: end TimeVal
     * @param format: additional log message
     * 
     * return string: log message
     * 
    */
    std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                    const char *eventName="", const char *format="", ...);

    /**
     * Update the number of Agents for benchmarking
     *
     * @param numAgents number of agents
     *
     * return if update is successful
     */
    bool updateNumAgents(int numAgents);

    inline int getNumAgents() {
        return _numAgents;
    }

    /**
     * print out benchmark value map
     * 
     * @param tvMap: value map
     */
    void printStats(std::map<std::string, double>* tvMap);

protected:
    /* bind to file meta */
    int _reqId;                         /* request id */

    char _name[MAX_FILENAME_LENGTH];
    int _nameLength;
    int _numAgents;

    BaseBMFunc::Type _type = UNKNOWN;
};

/**
 * BaseBMUnit
 */
class BaseBMUnit : public BaseBMInstance {
// private:
protected:
    int _id;                            /* instance id */
    BaseBMFunc *_func;                  /* BaseBMFunc that this instance belongs to */

public:

    BaseBMUnit(); 
    /**
     * set meta
     * 
     * @param id: instance id
     * @param size: size
     * @param func: BaseBMFunc that this instance belongs to
     * 
     * return bool: status
     * 
    */
    bool setMeta(int id, unsigned long int size, BaseBMFunc *func);

    /**
     * check whether the instance is valid
     * 
     * return bool: status
     * 
    */
    inline bool isValid();

    /**
     * print log
     * 
     * @param startTime: start TimeVal
     * @param endTime: end TimeVal
     * @param format: additional log message
     * 
     * return string: log message
     * 
    */
    std::string log(const TimeVal &startTime, const TimeVal &endTime, 
                    const char *eventName="", const char *format="", ...);
};

/**
 * Benchmark BMUnit
 */

class BMStripe : public BaseBMUnit {
public:
    std::vector<TagPt> *agentProcess;
    std::vector<TagPt> *network;
    TagPt replyCheck;

    BMStripe();
    ~BMStripe();

    /**
     * set meta
     * 
     * @param id: instance id
     * @param size: size
     * @param func: BaseBMFunc that this instance belongs to
     * 
     * return bool: status
     * 
    */
    bool setMeta(int id, unsigned long int size, BaseBMFunc *func);
};

class BMReadStripe : public BMStripe {
public:
    TagPt overall;              // overall time
    TagPt preparation;
    TagPt decode;
    TagPt download;             // staged read from storage
};

class BMWriteStripe : public BMStripe {
public:
    TagPt overall;          // overall time
    TagPt preparation;
    TagPt prepareChunks;
    TagPt encode;
    TagPt networkRT;        // overall network roundtrip time for the stripes
    TagPt upload;           // staged write to storage

    TagPt encodeToUpload;
    TagPt prepToEncode;
    TagPt uploadToFinalize;

    TagPt temp;
};

class BMRepairStripe : public BMStripe {
private:
    unsigned long int _repairSize;

public:
    void setRepairSize(unsigned long int size);
    unsigned long int getRepairSize();
};

/**
 * BMStripeFunc
 */
class BMStripeFunc : public BaseBMFunc {
protected:
    int _numStripes;
    std::vector<BMStripe *> *_bmStripe;
    bool _stripeOn;

public:
    BMStripeFunc();
    ~BMStripeFunc();
    bool setStripes(int numStripes);
    BMStripe &at(int idx);

    inline bool isStripeOn() {
        return _stripeOn;
    };
};

/**
 * Benchmark Write
 */
class BMWrite : public BMStripeFunc {
public:
    TagPt initBuffer;
    TagPt updateMeta;

    BMWrite();
    BMWriteStripe &at(int idx);
    std::map<std::string, double>* calcStats();
};

/**
 * Benchmark Read
 */
class BMRead : public BMStripeFunc {
public:
    double metadata;

    BMRead();
    BMReadStripe &at(int idx);
    std::map<std::string, double>* calcStats();
};

/**
 * Benchmark Repair
 */
class BMRepair : public BMStripeFunc {
public:
    TagPt getMeta;
    TagPt dataRepair;
    TagPt updateMeta;

    BMRepair();
    BMRepairStripe &at(int idx);
    std::map<std::string, double>* calcStats();
};

/**
 * Benchmark main class
 */
class Benchmark {

private:
    // a request to benchmark instance map is required for multi-client benchmarking
    std::map<int, BaseBMFunc *> _req2BMMap;   /* request to benchmark instance map */
    std::mutex _req2BMMapLock;                /* lock for request to benchmark instance map */

    Benchmark() {}
    Benchmark(Benchmark const&); // Don't Implement
    void operator=(Benchmark const&); // Don't implement

public:
    /**
     * Singleton: Instantiated on first use
    */
    static Benchmark& getInstance() {
        static Benchmark instance; // Guaranteed to be destroyed
        return instance; 
    }

    void clear();

    /**
     * add benchmark function instance to map
     * 
     * @param reqId: client request id
     * @param baseBMFunc *: benchmark instance
     * 
     * @return: status
    */
    bool add(int reqId, BaseBMFunc *baseBMFunc);

    /**
     * remove benchmark function instance
     * 
     * @param reqId: client request id
     * @return bool: status
    */
    bool remove(int reqId);

    /**
     * get benchmark function instance
     * 
     * @param reqId: client request id
     * @return BaseBMFunc *: benchmark instance
     */
    BaseBMFunc *at(int reqId);

    /**
     * replace benchmark function instance with func
     * 
     * @param reqId: client request id
     * @param func: benchmark instance
     * @return bool: status
     */
    bool replace(int reqId, BaseBMFunc *func);

    /**
     * get the time range between minimum start time and maximum end time
     * 
     * @param vec: vector that store the TagPts to be serached
     * @param earliest
     * @param start
     * @return TimeVal: the time range
     */
    static TimeVal findTv(std::vector<TagPt> *vec, bool earliest, bool start) {

        TimeVal _tv = earliest ? TimeVal(LONG_MAX, LONG_MAX) : TimeVal(INVALID_TV, INVALID_TV);

        if (vec == 0) 
            return _tv;

        int num = static_cast<int>(vec->size());

        // loop all events
        for (int i = 0; i < num; i++) {
            TimeVal _tv_s = start ? vec->at(i).getStart() : vec->at(i).getEnd();
            
            if (earliest) {
                _tv = (_tv_s < _tv) ? _tv_s : _tv;
            } else {
                _tv = (_tv_s > _tv) ? _tv_s : _tv;
            }
        }

        return _tv;
    }

    static inline TagPt findGap(std::vector<TagPt> *vec) {
        return TagPt(findTv(vec, true, true), findTv(vec, false, false));
    }
    
    static inline TimeVal findUt(std::vector<TagPt> *vec) {
        TagPt gap = findGap(vec);
        return gap.getEnd() - gap.getStart();
    }

    static int vecTime2SpeedWithOverlap(std::vector<double> **dst, std::vector<TagPt> *src1, std::vector<TagPt> *src2, unsigned long int size) {
        if (src1->size() != src2->size()) {
            LOG(ERROR) << "vector size mismatch";
            return -1;
        }
        int srcSize = static_cast<int>(src1->size());
        double fileSizeMB = BaseBMInstance::byte2MB(size);

        *dst = new std::vector<double>(srcSize, 0.0);
        for (int i = 0; i < srcSize; i++) {
            double interval = src1->at(i).usedTime() - src2->at(i).usedTime();
            (*dst)->at(i) = fileSizeMB / interval;
        }

        return 0;
    }

    /**
     * Vector util functions
     */
    static int vecTime2Speed(std::vector<double> **dst, std::vector<TagPt> *src, unsigned long int size) {
        if (src == 0 || dst == 0)
            return 0;

        int srcSize = static_cast<int>(src->size());
        double fileSizeMB = BaseBMInstance::byte2MB(size);

        *dst = new std::vector<double>(srcSize, 0.0);
        for (int i = 0; i < srcSize; i++) {
            (*dst)->at(i) = fileSizeMB / src->at(i).usedTime();
        }

        return 0;
    }

    static int vecMatrixAdd(std::vector<double> *dst, std::vector<double> *src) {
        if (dst == 0 || src == 0)
            return 0;

        int dstSize = static_cast<int>(dst->size());
        int srcSize = static_cast<int>(src->size());

        if (dstSize != srcSize) {
            return srcSize - dstSize;
        }

        for (int i = 0; i < srcSize; i++) {
            dst->at(i) += src->at(i);
        }

        return 0;
    }

    static int vecMatrixMultiply(std::vector<double> *src, double N) {
        int srcSize = static_cast<int>(src->size());

        for (int i = 0; i < srcSize; i++) {
            src->at(i) = src->at(i) * N;
        }

        return 0;
    }

};

#endif // define __BENCHMARK_HH__
