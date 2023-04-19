// SPDX-License-Identifier: Apache-2.0

#ifndef __REQUEST_HH__
#define __REQUEST_REPLY_HH__

#include <string>
#include <time.h>

#include "../common/define.hh"
#include "../proxy/coordinator.hh"
#include "file.hh"

struct Request {
    int opcode;
    struct {
        std::string name;
        unsigned char namespaceId;
        unsigned long int offset;
        union {
            unsigned long int size;
            unsigned long int length;
        };
        time_t ctime;
        bool isCached;
        std::string cachePath;
        unsigned char *data;
        std::string storageClass;
    } file;

    struct {
        unsigned long int usage;
        unsigned long int capacity;
        unsigned long int fileCount;
        unsigned long int fileLimit;
        unsigned long int repairCount;
    } stats;

    struct {
        FileInfo *fileInfo;
        unsigned int numFiles;
        ProxyCoordinator::AgentInfo *agentInfo;
        unsigned int numAgents;
        struct {
            std::string *name;
            int *progress;
            int num;
        } bgTasks;
    } list;

    SysInfo proxyStatus;

    Request() {
        opcode = ClientOpcode::UNKNOWN_CLIENT_OP;
        file.namespaceId = INVALID_NAMESPACE_ID;
        file.data = 0;
        file.offset = INVALID_FILE_OFFSET;
        file.size = INVALID_FILE_LENGTH;
        file.isCached = false;
        stats.usage = 0;
        stats.capacity = 0;
        stats.fileCount = 0;
        stats.fileLimit = 0;
        list.fileInfo = 0;
        list.numFiles = 0;
        list.agentInfo = 0;
        list.numAgents = 0;
        list.bgTasks.name = 0;
        list.bgTasks.progress = 0;
        list.bgTasks.num = 0;
    }

    ~Request() {
        delete[] list.fileInfo;
        delete[] list.agentInfo;
        delete[] list.bgTasks.name;
        delete[] list.bgTasks.progress;
    }
};

typedef Request        Reply;

#endif // define __REQUEST_REPLY_HH__
