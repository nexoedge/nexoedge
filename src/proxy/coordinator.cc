// SPDX-License-Identifier: Apache-2.0

#include <glog/logging.h>
#include <iomanip>

#include "coordinator.hh"
#include "../common/config.hh"
#include "../common/io.hh"
#include "../common/util.hh"
#include "../ds/coordinator_event.hh"

#define AGENT_MONITOR_CONN_POINT "inproc://monitor-agent"

ProxyCoordinator::ProxyCoordinator(std::map<int, std::string> *containerToAgentMap) : Coordinator() {
    _cxt = zmq::context_t(1);

    _containerToAgentMap = containerToAgentMap;
    
    // socket to listen incoming requests
    _socket = new zmq::socket_t (_cxt, ZMQ_REP);
    Util::setSocketOptions(_socket);
    // avoid infinite wait
    _socket->setsockopt(ZMQ_RCVTIMEO, Config::getInstance().getEventProbeTimeout());
    // rotating placement (if enabled)
    _startingAgentIdx = 0;
    // mark barrier for termination
    pthread_barrier_init(&_stopRunning, NULL, 2);
    _isRunning = false;
    _monitor = new MonitorAgentWorker(this);
    // mark the last checked time (Agent liveness) as never (since epoch 0)
    _lastCheckedTime = 0;

    // register pre-assigned agents
    registerPresetAgents();
}

ProxyCoordinator::~ProxyCoordinator() {
    LOG(WARNING) << "Terminating Proxy Coordinator";
    // let the main running thread stop itself first
    _isRunning = false;
    pthread_barrier_wait(&_stopRunning);
    _socket->close();
    // close socket for monitoring Agents
    delete _monitor;
    // close zmq context
    _cxt.close();
    delete _socket;
    LOG(WARNING) << "Terminated Proxy Coordinator";
}

void *ProxyCoordinator::run(void *arg)  {
    ProxyCoordinator *self = (ProxyCoordinator *) arg;
    Config &config = Config::getInstance();

    int myProxyNum = config.getMyProxyNum();

    std::string proxyIP = config.listenToAllInterfaces()? "0.0.0.0" : config.getProxyIP(myProxyNum);

    std::string address = IO::genAddr(proxyIP, config.getProxyCPort(myProxyNum));
    self->_socket->bind(address);

    // run monitoring service to update agent liveness 
    pthread_t monitorThread;
    pthread_create(&monitorThread, NULL, monitorAgents, self);

    self->_isRunning = true;

    try {
        while(self->_isRunning) {
            // get a request
            CoordinatorEvent event;
            if (Coordinator::getEventMessage(*(self->_socket), event) == 0)
                continue;

            AgentInfo agentInfo;

            bool success = true;
            switch (event.opcode) {
            case Opcode::REG_AGENT_REQ: // agent registeration
                // avoid one agents from having too many containers
                if (event.numContainers > NUM_MAX_CONTAINER_PER_AGENT) {
                    LOG(WARNING) << "Too many containers (" << event.numContainers << ") from agent at IP = " << event.agentAddr;
                    success = false;
                }
                agentInfo.numContainers = 0;
                success = self->registerAgent(event);
                self->printAgents();
                event.opcode = success? Opcode::REG_AGENT_REP_SUCCESS : Opcode::REG_AGENT_REP_FAIL;
                break;
            default:
                LOG(WARNING) << "Unknow opcode " << event.opcode << ", drop event message";
                break;
            }
            // send the reply
            Coordinator::sendEventMessage(*(self->_socket), event);
        }
    } catch (zmq::error_t &e) {
        LOG(WARNING) << "Proxy coordinator stops listening, " << e.what();
    }

    pthread_cancel(monitorThread);
    pthread_join(monitorThread, NULL);

    for (auto &a : self->_agents) {
        if (a.second.socket)
            a.second.socket->close();
        delete a.second.socket;
        a.second.socket = NULL;
    }
    pthread_barrier_wait(&self->_stopRunning);
    LOG(WARNING) << "Proxy coordinator stops running";

    return NULL;
}

int ProxyCoordinator::checkContainerLiveness(const int containerIds[], int numContainers, bool status[], bool updateStatusFirst, bool checkAllFailures, bool treatUnusedAsOffline) {
    std::string addr;

    // update agent status
    if (updateStatusFirst && _lastCheckedTime + Config::getInstance().getLivenessCacheTime() < time(NULL)) {
        updateAgentStatus();
        _lastCheckedTime = time(NULL);
    }

    _agentsLock.lock();
    int numFailed = 0;
    // Container is alive only if (1) there is a mapping to an Agent, and (2) the Agent is alive
    for (int i = 0; i < numContainers; i++) {
        // skip contacting an agent if no container is used for this chunk
        if (containerIds[i] == UNUSED_CONTAINER_ID) {
            if (treatUnusedAsOffline) {
                status[i] = false;
                numFailed++;
                if (!checkAllFailures) { break; }
            }
            continue;
        }
        // check if the container is mapped to an Agent
        try {
            addr = _containerToAgentMap->at(containerIds[i]);
        } catch (std::out_of_range &e) {
            if (containerIds[i] != INVALID_CONTAINER_ID)
                LOG(WARNING) << "Cannot find an agent mapped to container id = " << containerIds[i];
            status[i] = false;
            numFailed++;
            if (!checkAllFailures)
                break;
            continue;
        }
        // check if the associated Agent is alive
        try {
            status[i] = _agentStatus.at(IO::getAddrIP(addr));
            numFailed += (status[i]? 0 : 1);
        } catch (std::out_of_range &e) {
            LOG(WARNING) << "Cannot agent status ip = " << IO::getAddrIP(addr) << " for container id = " << containerIds[i];
            status[i] = false;
            numFailed++;
            if (!checkAllFailures)
                break;
        }
    }
    _agentsLock.unlock();
    return numFailed;
}

int ProxyCoordinator::getNumAliveContainers(bool skipFull, const std::string storageClass) {
    int k = Config::getInstance().getK(storageClass);
    int numAliveContainers = 0;

    // update agent status
    updateAgentStatus();
    _agentsLock.lock();
    for (auto a : _aliveAgents) {
        AgentInfo info = _agents.at(a);
        for (int i = 0; i < info.numContainers; i++) {
            if (skipFull && info.containerUsage[i] + 1 + 2 * k >= info.containerCapacity[i])
                continue;
            numAliveContainers++;
        }
    }
    _agentsLock.unlock();

    return numAliveContainers;
}

int ProxyCoordinator::findSpareContainers(const int *containerIds, int numContainers, const bool *status, int spareContainers[], int numSpare, unsigned long int fsize, const CodingMeta &codingMeta) {
    int selected = 0;
    std::set<std::string> agentsAlive;
    std::set<int> containers;
    std::map<std::string, int> agentContainerCount;

    if (numSpare <= 0) {
        LOG(INFO) << "Invalid argument for finding spare container, numSpare = " << numSpare;
        return 0;
    }

    // update agent status
    if (_lastCheckedTime + Config::getInstance().getLivenessCacheTime() < time(NULL)) {
        updateAgentStatus();
        _lastCheckedTime = time(NULL);
    }

    std::pair<std::set<int>::iterator, bool> ret;
    for (int i = 0; i < numContainers && status && containerIds; i++) {
        if (status[i]) {
            // add alive containers into a pool to avoid repeated selection
            ret = containers.insert(containerIds[i]);
            // skip the counting for duplicated container ids
            if (!ret.second) {
                continue;
            }
            // check if the container belongs to any agent (and skip the counting if not)
            std::map<int, std::string>::iterator it = _containerToAgentMap->find(containerIds[i]);
            if (it == _containerToAgentMap->end()) {
                continue;
            }
            // count the number of alive containers chosen from an agent to avoid violating the limit
            std::string agentIP = IO::getAddrIP(it->second);
            if (agentContainerCount.count(agentIP) > 0) {
                agentContainerCount.at(agentIP) += 1;
            } else {
                agentContainerCount.insert(std::pair<std::string, int> (agentIP, 1));
            }
        }
    }

    int n = codingMeta.n;
    int k = codingMeta.k;
    int f = codingMeta.f;
    int l = f > 0 ? (n - k) / f : n; // max. number of containers chosen per agent, floor of (n-k)/f 
    int r = 0; // min. number of containers chosen per agent

    //DLOG(INFO) << "Check for " << numSpare << " with " << numContainers << " at hand, n = " << n << ", k = " << k << ", f = " << f << " l = " << l;

    _agentsLock.lock();

    std::vector<int> skippedContainers;
    //int skippedContainers[_containerToAgentMap->size()];
    int agentCount = 0;
    int policy = Config::getInstance().getProxyDistributePolicy();
    for (auto a : _aliveAgents) {
        AgentInfo &info = _agents.at(a);

        int localSelected = 0;
        int numScreened = 0;

        int numToScreen = info.numContainers;
        std::multimap<float, int>::iterator uit = info.utilizationMap.begin();
        int cidx = info.startingContainerIndex;

        std::map<std::string, int>::iterator rec = agentContainerCount.find(a);

        // skip this agent if the number of container previously chosen is >= l
        if (rec != agentContainerCount.end() && rec->second >= l) {
            //DLOG(INFO) << "Skip agent " << a << " which has " << rec->second << " vs limit " << l;
            continue;
        }

        if (policy == DistributionPolicy::LU) {
            // skip this agent (TODO fall back instead)
            if (uit == info.utilizationMap.end())
                continue;
            numToScreen = info.utilizationMap.size(); 
            cidx = uit->second;
        }

        while (numScreened < numToScreen && selected < numSpare && localSelected < l) {
            numScreened++;
            // skip containers that are already selected prior to function call
            if (containers.count(info.containerIds[cidx]) > 0) {
                cidx = (cidx + 1) % numToScreen;
                // but count towards the number of selected containers of this agent
                //DLOG(INFO) << "Skip container " << info.containerIds[cidx] << " which is already in container set";
                localSelected++;
                continue;
            }
            // skip containers that do not have sufficient space for new data
            if (info.containerUsage[cidx] + (fsize + 2 * k) / k > info.containerCapacity[cidx]) {
                DLOG(INFO) << "Container id = " << info.containerIds[cidx] << " too full (" << info.containerUsage[cidx] << ") for chuck (" << (fsize + 2 * k) / k << ")";
                continue;
            } else {
                DLOG(INFO) << "Container id = " << info.containerIds[cidx] << " okay (" << info.containerUsage[cidx] << ") for chunk (" << (fsize + 2 * k) / k << ")";
            }
            // save containers that are not near for later (lower priority)
            if (agentCount < _startingAgentIdx || (policy == DistributionPolicy::STATIC && !info.isNear)) {
                skippedContainers.push_back(info.containerIds[cidx]);
                //DLOG(INFO) << "Push container " << info.containerIds[cidx] << " into lower priority container set";
            } else {
                spareContainers[selected++] = info.containerIds[cidx];
                // mark next starting container expected to use for round-robin
                if (policy == DistributionPolicy::RR && localSelected == 0 && info.numContainers > 0) {
                    info.startingContainerIndex = (cidx + 1) % info.numContainers;
                }
                DLOG(INFO) << "Select container " << selected - 1 << " id = " << spareContainers[selected - 1];
            }
            // count number of containers selected for this agent
            localSelected++;

            // find next container index to screen
            if (policy == DistributionPolicy::LU) {
                uit++;
                if (uit == info.utilizationMap.end())
                    break;
                cidx = uit->second;
            } else if (numToScreen > 0) {
                cidx = (cidx + 1) % numToScreen;
            }
        }
        if (localSelected < r) {
            LOG(WARNING) << "Failed to select at least " << r << " containers for agent " << info.addr << ", only " << localSelected << " selected.";
            return 0;
        }
        if (selected >= numSpare)
            break;
        agentCount++;
    }

    size_t numSkippedContainers = skippedContainers.size();
    for (size_t i = 0; i < numSkippedContainers && selected < numSpare; i++) {
        spareContainers[selected++] = skippedContainers.at(i);
        DLOG(INFO) << "Select container " << selected - 1 << " id = " << spareContainers[selected - 1];
    }
    
    // update the starting agent index for round-robin policy
    if (policy == DistributionPolicy::RR && _aliveAgents.size() > 0) {
        _startingAgentIdx = (_startingAgentIdx + 1) % _aliveAgents.size();
    }

    _agentsLock.unlock();

    return selected;
}

int ProxyCoordinator::findChunkGroups(const int *containerIds, int numContainers, const bool *status, int chunkGroups[]) {
    int numGroups = 0;
    std::map<std::string, int> agentToGroupMap;
    int groupSize[numContainers];
    memset(groupSize, 0, sizeof(int) * numContainers);
    std::string agent;

    _agentsLock.lock();
    for (int i = 0; i < numContainers && status && containerIds; i++) {
        if (status[i] == false)
            continue;
        // find the associated agent
        try {
            agent = _containerToAgentMap->at(containerIds[i]);
        } catch (std::out_of_range &e) {
            LOG(ERROR) << "Invalid container id = " << containerIds[i] << " is not mapped to any agent";
            continue;
        }
        // find the associated group of the agent
        if (agentToGroupMap.count(agent) == 0)
            agentToGroupMap.insert(std::pair<std::string, int>(agent, numGroups++));
        // mark the connection between chunks
        int groupId = agentToGroupMap.at(agent);
        chunkGroups[groupId * (numContainers + 1) + groupSize[groupId] + 1] = i;
        groupSize[groupId]++;
    }
    // mark the size of each chunk group
    for (int i = 0; i < numGroups; i++)
        chunkGroups[i * (numContainers + 1)] = groupSize[i];
    _agentsLock.unlock();

    return numGroups;
}

void *ProxyCoordinator::monitorAgents(void *arg) {
    ProxyCoordinator *self = (ProxyCoordinator *) arg;
    try {
        // start a monitor socket to handle events
        self->_monitor->monitor(*(self->_socket), AGENT_MONITOR_CONN_POINT);
    } catch (zmq::error_t &e) {
        LOG(WARNING) << "Proxy coordinator encounters an error when monitoring its socket, " << e.what();
    }
    LOG(INFO) << "Proxy coordinator stops monitoring its socket";
    return NULL;
}

void ProxyCoordinator::MonitorAgentWorker::on_event_accepted(const zmq_event_t &event, const char *addr) {
    LOG(INFO) << "Coordinator socket event (accept) addr = " << addr << " ip = " << IO::getAddrIP(addr);
}

void ProxyCoordinator::MonitorAgentWorker::on_event_closed(const zmq_event_t &event, const char *addr) {
    DLOG(INFO) << "Coordinator socket event (close)";
}

void ProxyCoordinator::MonitorAgentWorker::on_event_disconnected(const zmq_event_t &event, const char *addr) {
    LOG(INFO) << "Coordinator socket event (disconnect)";
    _coordinator->pingAgents();
}

bool ProxyCoordinator::setAgentAlive(std::string ip) {
    _aliveAgents.insert(ip);
    if (_agentStatus.insert(std::pair<std::string, bool>(ip, true)).second == false) {
        _agentStatus.at(ip) = true;
    }
    return true;
}

bool ProxyCoordinator::setAgentDown(std::string ip) {
    _aliveAgents.erase(ip);
    try {
        _agentStatus.at(ip) = false;
    } catch (std::exception &e) {
        return false;
    }
    return true;
}

void ProxyCoordinator::pingAgents() {
    DLOG(INFO) << "Start Agent PING";
    CoordinatorEvent event;
    _agentsLock.lock();
    for (auto &a : _agents) {
        if (a.second.socket == NULL) continue;
        DLOG(INFO) << "Ping agent at IP = " << a.first;
        try {
            // PING agent, expect a PONG
            event.opcode = Opcode::SYN_PING;
            if (Coordinator::sendEventMessage(*a.second.socket, event) == 0) {
                DLOG(INFO) << "Failed to send PING to agent at IP = " << a.first;
                throw zmq::error_t();
            }
            if (Coordinator::getEventMessage(*a.second.socket, event) == 0 || event.opcode != Opcode::ACK_PING) {
                DLOG(INFO) << "Failed to get PONG from agent at IP = " << a.first;
                throw zmq::error_t();
            }
        } catch (zmq::error_t &e) {
            LOG(WARNING) << "Cannot reach agent at IP = " << a.first;
            // assume agent fails if message cannot be sent, or the response is not PONG
            setAgentDown(a.first);
            // clean up the socket
            a.second.socket->close();
            delete a.second.socket;
            a.second.socket = NULL;
        }
    }
    _agentsLock.unlock();
    DLOG(INFO) << "End of Agent PING";
}

bool ProxyCoordinator::registerAgent(CoordinatorEvent &event) {
    std::lock_guard<std::mutex> lk(_agentsLock);

    bool success = true;
    AgentInfo agentInfo;
    agentInfo.numContainers = 0;

    // check if the container ids are unique
    for (int i = 0; i < event.numContainers && success; i++) {
        if (!_containerToAgentMap->insert(std::pair<int, std::string>(event.containerIds[i], event.agentAddr)).second) { // id already registered
            std::string origAddr = _containerToAgentMap->at(event.containerIds[i]);
            if (origAddr != event.agentAddr && _agentStatus.at(IO::getAddrIP(origAddr))) { // registered, and the original agent not failed
                for (int j = 0; j < i; j++) {
                    _containerToAgentMap->erase(event.containerIds[j]);
                    LOG(WARNING) << "Remove container "<< event.containerIds[j] << " due to duplicated container detected";
                }
                LOG(WARNING) << "Failed to add duplicated container " << event.containerIds[i] << " for agent at " << event.agentAddr;
                success = false;
                break;
            } else { // otherwise, original agent comes back or it is being replaced; accept the change
                try {
                    // TODO check content first before accepting the container?
                    // remove the container from the original agent info
                    AgentInfo &info = _agents.at(IO::getAddrIP(origAddr));
                    for (int j = 0; j < info.numContainers; j++) {
                        // container to remove is found
                        if (info.containerIds[j] == event.containerIds[i] && j + 1 < info.numContainers) {
                            // remove from utilization map
                            // move container ids following forward
                            memmove(info.containerIds + j, info.containerIds + j + 1, sizeof(int) * info.numContainers - j - 1);
                            memmove(info.containerUsage + j, info.containerUsage + j + 1, sizeof(unsigned long int) * info.numContainers - j - 1);
                            memmove(info.containerCapacity + j, info.containerCapacity + j + 1, sizeof(unsigned long int) * info.numContainers - j - 1);
                            memmove(info.containerType + j, info.containerType + j + 1, sizeof(unsigned char) * info.numContainers - j - 1);
                            break;
                        }
                    }
                    info.numContainers--;
                    // change the mapping from the original agent to the new agent
                    _containerToAgentMap->at(event.containerIds[i]) = event.agentAddr;
                    LOG(WARNING) << "Accept change of container status, from Agent at " << origAddr << " to " << event.agentAddr;
                } catch (std::exception &e) {
                    for (int j = 0; j < i; j++) {
                        _containerToAgentMap->erase(event.containerIds[j]);
                        LOG(WARNING) << "Remove container " << event.containerIds[j] << " due to duplicated container detected";
                    }
                    success = false;
                    break;
                }
            }
        }
        agentInfo.containerIds[agentInfo.numContainers] = event.containerIds[i];
        agentInfo.containerUsage[agentInfo.numContainers] = event.containerUsage[i];
        agentInfo.containerCapacity[agentInfo.numContainers] = event.containerCapacity[i];
        agentInfo.containerType[agentInfo.numContainers] = event.containerType[i];
        float usage = event.containerUsage[i] * 1.0 / event.containerCapacity[i];
        agentInfo.utilizationMap.insert(std::make_pair(usage, agentInfo.numContainers));
        agentInfo.numContainers++;
        LOG(INFO) << "Add container " << event.containerIds[i] << " for agent at " << event.agentAddr;
    }
    // mark the agent as alive if it can register successfully
    if (success) {
        std::string agentIP = IO::getAddrIP(event.agentAddr); 
        // TODO check for duplicated Agent address?
        setAgentAlive(agentIP);
        // start a connection for sending requests to Agents
        zmq::socket_t *as = new zmq::socket_t(_cxt, ZMQ_REQ);
        int timeout = Config::getInstance().getFailureTimeout();
        Util::setSocketOptions(as);
        as->setsockopt(ZMQ_RCVTIMEO, timeout);
        as->setsockopt(ZMQ_SNDTIMEO, timeout);
        as->setsockopt(ZMQ_LINGER, timeout);
        try {
            as->connect(IO::genAddr(agentIP, event.cport));
        } catch (zmq::error_t &e) {
            LOG(WARNING) << "Cannot connect to agent ip = " << agentIP << " for detecting disconnection";
            delete as;
            as = NULL;
        }
        agentInfo.socket = as;
        // check if the Agent is near to Proxy
        agentInfo.isNear = Config::getInstance().isAgentNear(agentIP.c_str());
        // mark host type
        agentInfo.hostType = event.agentHostType;
        // map the connection to Agent's IP
        auto result = _agents.insert(std::pair<std::string, AgentInfo>(IO::getAddrIP(event.agentAddr), agentInfo));
        if (result.second == false) {
            // unmap the previous containers
            for (int i = 0; i < result.first->second.numContainers; i++) {
                _containerToAgentMap->erase(result.first->second.containerIds[i]);
            }
            // clean up allocated resources
            delete result.first->second.socket;
            free(result.first->second.addr);
            // replace agent info
            result.first->second = agentInfo;
        }
    }
    return success;
}

void ProxyCoordinator::registerPresetAgents() {
    std::vector<std::pair<std::string, unsigned short> > list = Config::getInstance().getAgentList();
    int numAgents = list.size();

    // best effort to register preset agents
    for (int i = 0; i < numAgents; i++) {
        std::string agentIP = list.at(i).first;
        unsigned short port = list.at(i).second;
        // start a connection for sending requests to Agents
        zmq::socket_t as = zmq::socket_t(_cxt, ZMQ_REQ);
        int timeout = Config::getInstance().getFailureTimeout();
        Util::setSocketOptions(&as);
        as.setsockopt(ZMQ_RCVTIMEO, timeout);
        as.setsockopt(ZMQ_SNDTIMEO, timeout);
        CoordinatorEvent event;
        try {
            // establish connection
            as.connect(IO::genAddr(agentIP, port));
            // request for a status update
            if (!requestStatusUpdateFromAgent(event, as, agentIP))
                continue;
            // register the agent
            if (!registerAgent(event)) {
                LOG(WARNING) << "Failed to register agent at IP = " << agentIP;
            }
            as.close();
        } catch (zmq::error_t &e) {
            as.close();
            LOG(WARNING) << "Cannot register agent at IP = " << agentIP;
        }
    }
    printAgents();
}

void ProxyCoordinator::printAgents() {
    _agentsLock.lock();
    for (auto &a : _agents) {
        LOG(INFO) << "Agent " << IO::getAddrIP(a.first)
            << ", " << (a.second.socket? "UP" : "DOWN") 
            << ", " << a.second.numContainers;
        ;
        for (int i = 0; i < a.second.numContainers; i++) {
            LOG(INFO) << std::setprecision(4) << "Container " << a.second.containerIds[i] << ", " << 
                    a.second.containerUsage[i] << "/" << a.second.containerCapacity[i] << "(" << a.second.containerUsage[i] * (double) 1.0 / a.second.containerCapacity[i] * 100 << "%)"; 
        }
    }
    _agentsLock.unlock();
}

bool ProxyCoordinator::requestStatusUpdateFromAgent(CoordinatorEvent &event, zmq::socket_t &socket, const std::string &ip) {
    event.opcode = Opcode::UPD_AGENT_REQ;
    if (Coordinator::sendEventMessage(socket, event) == 0) {
        DLOG(INFO) << "Failed to send status update request to agent at IP = " << ip;
        throw zmq::error_t();
    }
    if (Coordinator::getEventMessage(socket, event) == 0 || event.opcode != Opcode::UPD_AGENT_REP) {
        DLOG(INFO) << "Failed to get status update from agent at IP = " << ip;
        return false;
    }
    return true;
}

void ProxyCoordinator::updateAgentStatus() {
    _agentsLock.lock();
    for (auto &a : _agents) {
        CoordinatorEvent event;
        if (a.second.socket == NULL)
            continue;
        try {
            if (!requestStatusUpdateFromAgent(event, *a.second.socket, a.first))
                continue;
            // update the status
            a.second.hostType = event.agentHostType;
            a.second.utilizationMap.clear();
            for (int i = 0; i < event.numContainers; i++) {
                // find the matching container id in the array (and cater any change in container order)
                for (int j = i, scanned = 0; scanned < event.numContainers; j = (j + 1) % event.numContainers, scanned++) { 
                    if (a.second.containerIds[j] != event.containerIds[i])
                        continue;
                    a.second.containerUsage[j] = event.containerUsage[j];
                    a.second.containerCapacity[j] = event.containerCapacity[j];
                    a.second.containerType[j] = event.containerType[j];
                    break;
                }
                // sort containers by usage
                float usage = event.containerUsage[i] * 1.0 / event.containerCapacity[i];
                a.second.utilizationMap.insert(std::make_pair(usage, i));
                DLOG(INFO) << "Agent add container " << event.containerIds[i] << " current usage = " << usage;
            }
            DLOG(INFO) << "Agent has " << a.second.utilizationMap.size() << " containers in utilization map";
            // warn if the update contains fewer containers than that in the existing record
            if (a.second.utilizationMap.size() != (size_t) event.numContainers) {
                LOG(WARNING) << "Agent only sent updates on " << event.numContainers << " containers, expecting " << a.second.numContainers;
            }
            // request agent system info
            event.opcode = Opcode::GET_SYSINFO_REQ;
            if (Coordinator::sendEventMessage(*a.second.socket, event) == 0) {
                DLOG(INFO) << "Failed to send system info request to agent at IP = " << a.first;
                throw zmq::error_t();
            }
            if (Coordinator::getEventMessage(*a.second.socket, event) == 0 || event.opcode != Opcode::GET_SYSINFO_REP) {
                DLOG(INFO) << "Failed to get system info from agent at IP = " << a.first;
                continue;
            }
            for (int i = 0; i < event.numContainers; i++)
                a.second.sysinfo = event.sysinfo;
        } catch (zmq::error_t &e) {
            LOG(WARNING) << "Cannot reach agent at IP = " << a.first;
            // assume agent fails if message cannot be sent, or the response is not PONG
            setAgentDown(a.first);
            // clean up the socket
            a.second.socket->close();
            delete a.second.socket;
            a.second.socket = NULL;
        }

    }
    _agentsLock.unlock();
}

int ProxyCoordinator::getAgentStatus(AgentInfo **info) {
    updateAgentStatus();
    _agentsLock.lock();
    AgentInfo *temp = new AgentInfo[_agents.size()];
    int numAgents = 0;
    for (auto &a : _agents) {
        temp[numAgents] = a.second;
        const char *agentAddr = IO::getAddrIP(a.first).c_str();
        temp[numAgents].addr = (char *) malloc (strlen(agentAddr) + 1);
        temp[numAgents].hostType = a.second.hostType;
        strcpy(temp[numAgents].addr, agentAddr);
        temp[numAgents].alive = (a.second.socket != NULL);
        numAgents++;
    }
    _agentsLock.unlock();
    *info = temp;
    return numAgents;
}

bool ProxyCoordinator::getProxyStatus(SysInfo &info) {
    info = _sysinfo[_latestInfoIdx];
    info.hostType = _hostType;
    return true;
}

void ProxyCoordinator::getStorageUsage(unsigned long int &usage, unsigned long int &capacity, const std::string storageClass) {
    updateAgentStatus();
    // TODO fix the usage estimation
    int k = Config::getInstance().getK(storageClass), n = Config::getInstance().getN(storageClass);
    usage = 0;
    capacity = 0;

    unsigned long int minCapacity = ULONG_MAX, maxUsage = 0;
    int numAliveContainers = 0, numContainers = 0;

    _agentsLock.lock();
    // find the min capacity and max usage among containers
    for (auto &a : _agents) {
        for (int i = 0; i < a.second.numContainers; i++) {
            numAliveContainers += a.second.socket != NULL;
            numContainers++;
            minCapacity = std::min(minCapacity, a.second.containerCapacity[i]);
            maxUsage = std::max(maxUsage, a.second.containerUsage[i]);
        }
    }
    _agentsLock.unlock();

    // adjust the logical usage
    if (numAliveContainers <= n) {
        capacity = minCapacity * k;
        usage = maxUsage * k;
    } else {
        capacity = minCapacity * numContainers / n * k;
        usage = maxUsage * numContainers / n * k;
    }

    // mark the volume as full if not even okay for storing single copy
    if (numAliveContainers < k)
        capacity = usage;
}
