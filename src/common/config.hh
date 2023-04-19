// SPDX-License-Identifier: Apache-2.0

#ifndef __CONFIG_HH__
#define __CONFIG_HH__

#include <stdint.h>
#include <limits.h>
#include <string>
#include <vector>
#include <set>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "define.hh"

class Config {
public:

    static Config& getInstance() {
        static Config instance; // Guaranteed to be destroyed
        // Instantiated on first use
        return instance;
    }

    void setConfigPath (const char *generalPath = "general.ini", const char *proxyPath = "proxy.ini", const char *agentPath = "agent.ini", const char *dirPath = ".");
    void setConfigPath (std::string dir);

    // general
    bool glogToConsole() const;
    std::string getGlogDir() const;
    int getLogLevel() const;
    // general.retry
    int getRetryInterval() const;
    int getNumRetry() const;
    // general.network
    bool listenToAllInterfaces() const;
    bool manualTcpKeepAlive() const;
    int getTcpKeepAliveIdle() const;
    int getTcpKeepAliveIntv() const;
    int getTcpKeepAliveCnt() const;
    int getTcpBufferSize() const;
    // general.failureDetection
    int getFailureTimeout() const;
    // general.event
    int getEventProbeTimeout() const;
    // general.dataIntegrity
    bool verifyChunkChecksum() const;
    // general.benchmark
    bool getBenchmarkStripeEnabled() const;

    // agent
    std::string getAgentIP() const;
    unsigned short getAgentPort() const;
    unsigned short getAgentCPort() const;
    int getNumContainers() const;
    int getContainerId(int i) const;
    std::string getContainerPath(int i) const;
    unsigned long int getContainerCapacity(int i) const;
    unsigned short getContainerType(int i) const;
    std::string getContainerRegion(int i) const;
    std::string getContainerKeyId(int i) const;
    std::string getContainerKey(int i) const;
    std::string getContainerHttpProxyIP(int i) const;
    unsigned short getContainerHttpProxyPort(int i) const;
    // agent.misc
    int getAgentNumWorkers() const;
    int getAgentNumZmqThread() const;
    unsigned long int getCopyBlockSize() const;
    bool getAgentFlushOnClose() const;
    bool getAgentRegisterToProxy() const;

    // proxy
    int getNumProxy() const;
    unsigned char getProxyNamespaceId() const;
    std::string getProxyIP(int i) const;
    unsigned short getProxyCPort(int i) const;
    int getMyProxyNum() const;
    std::string getProxyInterface() const;
    // proxy.storage_class
    std::string getStorageClassesFilePath() const;
    std::string getDefaultStorageClass() const;
    bool hasStorageClass(std::string storageClass) const;
    int getNumStorageClasses() const;
    std::set<std::string> getStorageClasses() const;
    int getCodingScheme(std::string storageClass = "") const;
    int getN(std::string storageClass = "") const;
    int getK(std::string storageClass = "") const;
    int getF(std::string storageClass = "") const;
    int getMaxChunkSize(std::string storageClass = "") const;
    // proxy.metastore
    int getProxyMetaStoreType() const;
    std::string getProxyMetaStoreIP() const;
    unsigned short getProxyMetaStorePort() const;
    // proxy.misc
    int getProxyNumZmqThread() const;
    bool isRepairAtProxy() const;
    bool isRepairUsingCAR() const;
    bool overwriteFiles() const;
    bool reuseDataConn() const;
    int getLivenessCacheTime() const;
    std::vector<std::pair<std::string, unsigned short> > getAgentList();
    int getJournalCheckInterval() const;
    // proxy.data_distribution
    int getProxyDistributePolicy() const;
    bool isAgentNear(const char *ipStr) const;
    // proxy.background_write
    bool ackRedundancyInBackground() const;
    bool writeRedundancyInBackground() const;
    int getBgTaskCheckInterval() const;
    bool getProxyNumBgChunkWorker() const;
    int* getProxyNearIpRanges(int &numRanges) const;
    // proxy.zmqITF
    int getProxyZmqNumWorkers() const;
    unsigned short getProxyZmqPort() const;
    // proxy.recovery
    bool autoFileRecovery() const;
    int getFileRecoverInterval() const;
    int getFileScanInterval() const;
    time_t getChunkScanInterval() const;
    int getChunkScanBatchSize() const;
    int getFileRecoverBatchSize() const;
    int getChunkScanSamplingPolicy() const;
    double getChunkScanSamplingRate() const;
    // proxy.reporter
    std::string getProxyReporterDBIP() const;
    unsigned short getProxyReporterDBPort() const;
    int getProxyReporterDBRecordBufferSize() const;
    bool sendStatsToReporterDB() const;

    // proxy.staging
    bool proxyStagingEnabled() const;
    // proxy.staging.storage
    std::string getProxyStagingStorageURL() const;
    // proxy.staging.autoClean
    std::string getProxyStagingAutoCleanPolicy() const;
    int getProxyStagingAutoCleanNumDaysExpire() const;
    int getProxyStagingAutoCleanScanIntv() const;
    // proxy.staging.bgwrite
    std::string getProxyStagingBackgroundWritePolicy() const;
    int getProxyStagingBackgroundWriteScanInterval() const;
    std::string getProxyStagingBackgroundWriteTimestamp() const;

    void printConfig() const;

private:
    Config() {}
    Config(Config const&); // Don't Implement
    void operator=(Config const&); // Don't implement
    
    bool readBool (const boost::property_tree::ptree &pt, const char *key) const;
    int readInt (const boost::property_tree::ptree &pt, const char *key) const;
    unsigned int readUInt (const boost::property_tree::ptree &pt, const char *key) const;
    long long readLL (const boost::property_tree::ptree &pt, const char *key) const;
    unsigned long long readULL (const boost::property_tree::ptree &pt, const char *key) const;
    double readFloat (const boost::property_tree::ptree &pt, const char *key) const;
    std::string readString (const boost::property_tree::ptree &pt, const char *key) const;

    int readIntWithBounds (const boost::property_tree::ptree &pt, const char *key, int min = 0, int max = INT32_MAX) const;
    int readIntWithBoundsAndDefault (const boost::property_tree::ptree &pt, const char *key, int dv = 0, int min = 0, int max = INT32_MAX) const;

    unsigned short parseContainerType(std::string typeName) const;
    int parseLogLevel(std::string levelName) const;
    int parseDistributionPolicy(std::string policyName) const;
    int parseCodingScheme(std::string schemeName) const;
    int parseChunkScanSamplingPolicy(std::string policyName) const;
    int parseMetaStoreType(std::string storeName) const;

    int getStorageClassConfig(std::string storageClass, std::string config, int dv = 0, int min = 0, int max = INT32_MAX) const;

    static const char *ContainerTypeName[];
    static const char *LogLevelName[];
    static const char *DistributionPolicyName[];
    static const char *ChunkScanSamplingPolicyName[];
    static const char *MetaStoreName[];

    boost::property_tree::ptree _agentPt;
    boost::property_tree::ptree _proxyPt;
    boost::property_tree::ptree _generalPt;
    boost::property_tree::ptree _storageClassPt;

    typedef struct {
        int id;
        std::string url;
        unsigned long int capacity;
        unsigned short type;
        std::string region;
        std::string key;
        std::string keyId;
        struct {
            std::string ip;
            unsigned short port;
        } httpProxy;
    } ContainerInfo;

    typedef struct {
        std::string ip;
        unsigned short cport;
    } ProxyInfo;
        
    struct {
        int level;
        bool glogToConsole;
        std::string glogdir;
        struct {
            int num;
            int intv;
        } retry;
        struct {
            bool listenToAllInterfaces;
            struct {
                bool enabled;
                int idle;
                int intv;
                int cnt;
            } tcpKeepAlive;
            int tcpBuffer;
        } network;
        struct {
            bool verifyChunkChecksum;
        } dataIntegrity;
        struct {
            int eventProbeTimeout;
        } event;
        struct {
            int timeout;
        } failureDetection;
        struct {
            bool stripeEnabled;
        } benchmark;
    } _general;

    struct {
        std::string ip;
        unsigned short port;
        unsigned short cport;
        int numContainers;
        ContainerInfo containers[MAX_NUM_CONTAINERS];
        struct {
            int numWorkers;
            int numZmqThread;
            unsigned long int copyBlockSize;
            bool flushOnClose;
            bool registerToProxy;
        } misc;
    } _agent;

    struct {
        int numProxy;
        unsigned char  namespaceId;
        ProxyInfo addrs[MAX_NUM_PROXY];
        int myProxyNum;
        std::string interface;
        struct {
            std::string filePath;
            std::set<std::string> classes;
            std::string defaultClass;
        } storageClass;
        struct {
            int type;
            struct {
                std::string ip;
                unsigned short port;
            } redis;
        } metastore;
        struct {
            int numZmqThread;
            bool repairAtProxy;
            bool repairUsingCAR;
            bool overwriteFiles;
            bool reuseDataConn;
            int livenessCacheTime;
            std::vector<std::pair<std::string, unsigned short> > agentList; // IP, port
            int scanJournalIntv;
        } misc;
        struct {
            int policy;
            int numNearIpRanges;
            unsigned long int nearIpRanges[MAX_NUM_NEAR_IP_RANGES];
            unsigned long int nearIpRangeMasks[MAX_NUM_NEAR_IP_RANGES];
        } dataDistribution;
        struct {
            bool ackRedundancy;
            bool writeRedundancy;
            int numWorker;
            int taskCheckIntv;
        } backgroundWrite;
        struct {
            int numWorkers;
            unsigned short port;
        } zmqITF;
        struct {
            bool enabled;
            int recoverIntv;
            int scanIntv;
            int scanChunkIntv;
            int chunkBatchSize;
            int batchSize;
            struct {
                int policy;
                double rate;
            } chunkScanSampling;
        } recovery;
        struct {
            std::string ip;
            unsigned short port; 
            int recordBufSize;
        } reporterDB;
        struct {
            bool enabled;
            std::string url;
            struct {
                std::string policy;
                int numDaysExpire;
                int scanIntv;
            } autoClean;
            struct {
                std::string policy;
                int scanIntv;
                std::string scheduledTime;
            } bgwrite;
        } staging;
    } _proxy;
};

#endif // define __CONFIG_HH__
