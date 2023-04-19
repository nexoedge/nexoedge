// SPDX-License-Identifier: Apache-2.0

#include <sys/sysinfo.h>

#include <boost/timer/timer.hpp>

#include "coordinator.hh"
#include "../common/define.hh"

#include <glog/logging.h>
#include <curl/curl.h>

Coordinator::Coordinator() {
    _latestInfoIdx = 0;
    _running = true;
    _hostType = HostType::HOST_TYPE_UNKNOWN;
    pthread_create(&_sysInfoT, NULL, Coordinator::updateSysInfoBg, (void*) this);
}

Coordinator::~Coordinator() {
    _running = false;
    pthread_join(_sysInfoT, 0);
}

bool Coordinator::hasData(unsigned short opcode) {
    return (
        opcode == REG_AGENT_REQ ||
        opcode == UPD_AGENT_REP ||
        opcode == GET_SYSINFO_REP ||
        false
    );
}

unsigned long int Coordinator::sendEventMessage(zmq::socket_t &socket, const CoordinatorEvent &event) {
    unsigned long int bytes = 0; 

    bytes += socket.send(&event.opcode, sizeof(event.opcode), hasData(event.opcode)? ZMQ_SNDMORE : 0);
    
    if (!hasData(event.opcode))
        return bytes;

    int addrLength = event.agentAddr.length();
    switch (event.opcode) {
    case Opcode::REG_AGENT_REQ:
    case Opcode::UPD_AGENT_REP:
        // agent id
        bytes += socket.send(&event.agentId, sizeof(event.agentId), ZMQ_SNDMORE);
        // agent host type
        bytes += socket.send(&event.agentHostType, sizeof(event.agentHostType), ZMQ_SNDMORE);
        // agent address
        bytes += socket.send(&addrLength, sizeof(addrLength), ZMQ_SNDMORE); 
        if (addrLength)
            bytes += socket.send(event.agentAddr.c_str(), addrLength, ZMQ_SNDMORE);
        bytes += socket.send(&event.cport, sizeof(event.cport), ZMQ_SNDMORE);
        // number of containers held by the agent
        bytes += socket.send(&event.numContainers, sizeof(int), event.numContainers > 0? ZMQ_SNDMORE : 0);
        // list of container ids
        if (event.numContainers > 0) {
            bytes += socket.send(event.containerIds, sizeof(int) * event.numContainers, ZMQ_SNDMORE);
            bytes += socket.send(event.containerType, sizeof(unsigned char) * event.numContainers, ZMQ_SNDMORE);
            bytes += socket.send(event.containerUsage, sizeof(unsigned long int) * event.numContainers, ZMQ_SNDMORE);
            bytes += socket.send(event.containerCapacity, sizeof(unsigned long int) * event.numContainers, 0);
        }
        break;

    case Opcode::GET_SYSINFO_REP:
        // number of cpus
        bytes += socket.send(&event.sysinfo.cpu.num, sizeof(event.sysinfo.cpu.num), ZMQ_SNDMORE);
        // cpu usage
        bytes += socket.send(event.sysinfo.cpu.usage, sizeof(float) * event.sysinfo.cpu.num, ZMQ_SNDMORE);
        // total memory
        bytes += socket.send(&event.sysinfo.mem.total, sizeof(event.sysinfo.mem.total), ZMQ_SNDMORE);
        // free memory 
        bytes += socket.send(&event.sysinfo.mem.free, sizeof(event.sysinfo.mem.free), ZMQ_SNDMORE);
        // ingress traffic rate
        bytes += socket.send(&event.sysinfo.net.in, sizeof(event.sysinfo.net.in), ZMQ_SNDMORE);
        // outgress traffic rate
        bytes += socket.send(&event.sysinfo.net.out, sizeof(event.sysinfo.net.out), 0);
        break;

    default:
        break;
    }

    DLOG(INFO) << "Coordinator message sent (" << bytes << "B)";

    return bytes;
}

unsigned long int Coordinator::getEventMessage(zmq::socket_t &socket, CoordinatorEvent &event) {
    unsigned long int bytes = 0; 
    zmq::message_t msg;

#define getNextMsg( ) do { \
    msg.rebuild(); \
    if (socket.recv(&msg) == false) \
        return 0; \
    bytes += msg.size(); \
} while(0);

#define getField(_FIELD_, _CAST_TYPE_) do { \
    getNextMsg(); \
    event._FIELD_= *((_CAST_TYPE_ *) msg.data()); \
} while(0)

    getField(opcode, unsigned short);

    if (!hasData(event.opcode))
        return bytes;

    int addrLength = 0;
    switch (event.opcode) {
    case Opcode::REG_AGENT_REQ:
    case Opcode::UPD_AGENT_REP:
        // agent id
        if (!msg.more()) return 0;
        getField(agentId, int);

        // agent host type
        if (!msg.more()) return 0;
        getField(agentHostType, unsigned char);

        // agent address
        if (!msg.more()) return 0;
        getNextMsg();
        addrLength = *((int*) msg.data());
        
        if (addrLength > 0) {
            if (!msg.more()) return 0;
            getNextMsg();
            event.agentAddr = std::string((char*) msg.data(), addrLength);
        }

        // coordinator port
        if (!msg.more()) return 0;
        getField(cport, unsigned short);

        // number of containers
        if (!msg.more()) return 0;
        getField(numContainers, int);

        // list of container ids
        if (event.numContainers > 0) {
            if (!msg.more()) return 0;
            getNextMsg();
            event.containerIds = new int[event.numContainers];
            memcpy(event.containerIds, msg.data(), sizeof(int) * event.numContainers);
            if (!msg.more()) return 0;
            getNextMsg();
            event.containerType = new unsigned char[event.numContainers];
            memcpy(event.containerType, msg.data(), sizeof(unsigned char) * event.numContainers);
            if (!msg.more()) return 0;
            getNextMsg();
            event.containerUsage = new unsigned long int[event.numContainers];
            memcpy(event.containerUsage, msg.data(), sizeof(unsigned long int) * event.numContainers);
            getNextMsg();
            event.containerCapacity = new unsigned long int[event.numContainers];
            memcpy(event.containerCapacity, msg.data(), sizeof(unsigned long int) * event.numContainers);
        }
        break;

    case GET_SYSINFO_REP:
        // number of cpu
        if (!msg.more()) return 0;
        getField(sysinfo.cpu.num, char);
        // cpu usage
        if (!msg.more()) return 0;
        getNextMsg();
        memcpy(event.sysinfo.cpu.usage, msg.data(), sizeof(float) * event.sysinfo.cpu.num);
        // total memory
        if (!msg.more()) return 0;
        getField(sysinfo.mem.total, unsigned int);
        // free memory
        if (!msg.more()) return 0;
        getField(sysinfo.mem.free, unsigned int);
        // ingress traffic rate
        if (!msg.more()) return 0;
        getField(sysinfo.net.in, double);
        // outgress traffic rate
        if (!msg.more()) return 0;
        getField(sysinfo.net.out, double);
        break;

    default:
        break;
    }

#undef getNextMsg
#undef getField
    DLOG(INFO) << "Coordinator message received (" << bytes << "B)";

    return bytes;
}

void Coordinator::updateSysInfo() {
    int nextIdx = (_latestInfoIdx + NUM_SYSINFO_HIST) % NUM_SYSINFO_HIST;

    // cpu and net
    struct sysinfo info;
    sysinfo(&info);
    int numCpu = get_nprocs();
    _sysinfo[nextIdx].cpu.num = numCpu; 

    unsigned int time = 0, total[numCpu * 2] = {0}, idle[numCpu * 2] = {0};
    unsigned long rxBytes[2] = {0}, txBytes[2] = {0};
    int ret = 0;
    boost::timer::cpu_timer mytimer;
    for (int r = 0; r < 2; r++) {
        // cpu
        FILE *f = fopen("/proc/stat", "r");
        // ignore the first line
        if (f) {
            ret = fscanf(f, "%*s %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u\n");
            // read times for each cpu
            for (int i = 0; i < _sysinfo[nextIdx].cpu.num && f != NULL; i++) {
                ret = fscanf(f, "%*s");
                for (int j = 0; j < 10; j++) {
                    ret = fscanf(f, "%u", &time);
                    if (!ret)
                        time = 0;
                    if (j == 3)
                        idle[i * 2 + r] += time;
                    total[i * 2 + r] += time;
                }
                if (r == 1 && total[i * 2 + 1] > total[i * 2]) {
                    _sysinfo[nextIdx].cpu.usage[i] = (1 - ((idle[i * 2 + 1]- idle[i * 2]) * 1.0 / (total[i * 2 + 1] - total[i * 2]))) * 100;
                    //DLOG(INFO) << "CPU " << i << " Usage = " << _sysinfo[nextIdx].cpu.usage[i] << "%";
                }
            }
            fclose(f);
        }

        // net
        f = fopen("/proc/net/dev", "r");
        if (f) {
            // skip first two lines (labels)
            ret = fscanf(f, "%*s %*s %*s %*s\n");
            ret = fscanf(f, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s\n");
            // read rx_bytes and tx_bytes
            ret = fscanf(f, "%*s %lu %*u %*u %*u %*u %*u %*u %*u %lu %*u %*u %*u %*u %*u %*u\n", &rxBytes[r], &txBytes[r]);
            fclose(f);
            if (r == 1) {
                boost::timer::cpu_times duration = mytimer.elapsed();
                if (duration.wall > 0) {
                    _sysinfo[nextIdx].net.in = (rxBytes[1] - rxBytes[0]) * 1.0 / (duration.wall / 1e9);
                    _sysinfo[nextIdx].net.out = (txBytes[1] - txBytes[0]) * 1.0 / (duration.wall / 1e9);
                } else {
                    _sysinfo[nextIdx].net.in = 0.0;
                    _sysinfo[nextIdx].net.out = 0.0;
                }
            }
        }
            
        if (r == 0)
            sleep(1);
    }

    // memory (in MB)
    _sysinfo[nextIdx].mem.total = info.totalram / (1 << 20);
    _sysinfo[nextIdx].mem.free = info.freeram / (1 << 20);
    //DLOG(INFO) << "Memory (free/total) " << _sysinfo[nextIdx].mem.free << "MB /" << _sysinfo[nextIdx].mem.total << "MB";

    // mark this as the latest
    _latestInfoIdx = nextIdx;

    // host type
    if (_hostType == HostType::HOST_TYPE_UNKNOWN) {
        _hostType = checkHostType();
        LOG(INFO) << "Host type = " << (int) _hostType;
    }
}

void *Coordinator::updateSysInfoBg(void *arg) {

    Coordinator *c = static_cast<Coordinator*>(arg);

    while (c->_running) {
        c->updateSysInfo();
        // periodical updates (every second)
        sleep(1);
    }

    return 0;
}

unsigned char Coordinator::checkHostType() {
    unsigned char type = HostType::HOST_TYPE_UNKNOWN;

    if (checkHostTypeAction("http://100.100.100.200", &type, HOST_TYPE_ALI)) {
        // alibaba
    } else if (checkHostTypeAction("http://metadata.google.internal", &type, HOST_TYPE_GCP)) {
        // gcp
    } else if (checkHostTypeAction("http://metadata.tencentyun.com", &type, HOST_TYPE_TENCENT)) {
        // tencent
    } else if (checkHostTypeAction("http://169.254.169.254", &type)) {
        // aws, azure
    } else if (checkHostTypeAction("http://169.254.169.254", &type, HOST_TYPE_HUAWEI)) {
        // huawei
    } else {
        type = HostType::HOST_TYPE_ON_PREM;
    }

    return type;
}

size_t Coordinator::checkHostTypeByHeader(char *buffer, size_t unitSize, size_t length, void *userdata) {
    if (length <= 0)
        return length;

    unsigned char *type = (unsigned char *) userdata;
    std::string header(buffer, length);
    size_t delimiter = header.find(":");
    if (delimiter == std::string::npos)
        return length;

    std::string key = header.substr(0, delimiter), value = header.substr(delimiter + 1) ;
    if (strstr(key.c_str(), "Server") != 0) {
        if (strstr(value.c_str(), "EC2") != 0) {
            *type = HostType::HOST_TYPE_AWS;
        } else if (strstr(value.c_str(), "Microsoft") != 0) {
            *type = HostType::HOST_TYPE_AZURE;
        }
    }

    return unitSize * length;
}

size_t Coordinator::checkHostTypeOutput(char *buffer, size_t unitSize, size_t length, void *userdata) {
    // do nothing (ignore output)
    return unitSize * length;
}

bool Coordinator::checkHostTypeAction(const char *address, unsigned char *type, unsigned char expectedType) {
    if (type == 0 || address == 0)
        return false;

    CURL *curl = 0;
    CURLcode res;

    long response = 0;
    bool found = false;

    curl = curl_easy_init();
    if (!curl) {
        goto check_agent_host_type_action_exit;
    }

    // timeout after 20ms
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 20);
    // ignore response body
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, 0);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, checkHostTypeOutput);
    // scan header fields
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, type);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, checkHostTypeByHeader);

    // probe using curl
    curl_easy_setopt(curl, CURLOPT_URL, address);
    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response);

    DLOG(INFO) << "probe " << address  << " res = " << res << " http response code = " << response;

    // check for a successful response
    if (res == CURLE_OK && (response == 200 || response == 400)) {
        // avoid overriding results from header field scan
        if (expectedType != HostType::HOST_TYPE_UNKNOWN)
            *type = expectedType;
        if (*type != HostType::HOST_TYPE_UNKNOWN)
            found = true;
    }

check_agent_host_type_action_exit:
    // clean up resources
    curl_easy_cleanup(curl);
    return found;
}

