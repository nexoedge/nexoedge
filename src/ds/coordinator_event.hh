// SPDX-License-Identifier: Apache-2.0

#ifndef __COORDINATOR_EVENT_HH__
#define __COORDINATOR_EVENT_HH__

#include <string>

#include "../common/define.hh"

struct SysInfo {
    struct {
        char num; 
        float usage[256]; 
    } cpu;
    struct {
        unsigned int total;
        unsigned int free;
    } mem;
    struct {
        double in;
        double out;
    } net;

    unsigned char hostType;

    SysInfo() {
        cpu.num = 0;
        memset(cpu.usage, 0, 256 * sizeof(float));
        mem = {0, 0};
        net = {0.0, 0.0};
        hostType = HostType::HOST_TYPE_UNKNOWN;
    }
};

struct CoordinatorEvent {
    unsigned short opcode;

    int agentId;
    unsigned char agentHostType;
    std::string agentAddr;
    unsigned short cport;
    int numContainers;
    int *containerIds;
    unsigned long int *containerUsage;
    unsigned long int *containerCapacity;
    unsigned char *containerType;

    SysInfo sysinfo;

    CoordinatorEvent() {
        opcode = 0;
        agentId = 0;
        agentHostType = HostType::HOST_TYPE_UNKNOWN;
        cport = 0;
        numContainers = 0;
        containerIds = 0;
        containerUsage = 0;
        containerCapacity = 0;
        containerType = 0;
    }

    ~CoordinatorEvent() {
        delete [] containerIds;
        delete [] containerUsage;
        delete [] containerCapacity;
        delete [] containerType;
    }
};

#endif // define __COORDINATOR_EVENT_HH__
