// SPDX-License-Identifier: Apache-2.0

#include <string>

#include <glog/logging.h>

#include "coordinator.hh"
#include "../common/config.hh"
#include "../common/io.hh"
#include "../common/util.hh"

#define PROXY_MONITOR_CONN_POINT "inproc://monitor-proxy"

AgentCoordinator::AgentCoordinator(ContainerManager *cm) : _cm(cm), Coordinator()  {
    Config &config = Config::getInstance();
    _cxt = zmq::context_t(1);
    _numProxy = config.getNumProxy();
    int timeout = Config::getInstance().getEventProbeTimeout();

    for (int i = 0; i < _numProxy; i++) {
        _proxy[i] = new zmq::socket_t(_cxt, ZMQ_REQ);
        Util::setSocketOptions(_proxy[i]);
        _proxy[i]->setsockopt(ZMQ_RCVTIMEO, timeout);
        _proxy[i]->setsockopt(ZMQ_LINGER, timeout);
        _mworkers[i] = new MonitorProxyWorker(this);
    }

    _socket = new zmq::socket_t(_cxt, ZMQ_REP);
    Util::setSocketOptions(_socket);
    _socket->setsockopt(ZMQ_RCVTIMEO, timeout);
    _isListening = false;
    _isMonitoring = false;

    // start background listening thread for proxy requests
    pthread_create(&_runt, NULL, AgentCoordinator::listenToProxy, this);

    // check host type before registration
    checkHostType();
}

AgentCoordinator::~AgentCoordinator() {
    LOG(WARNING) << "Terminating Coordinator ...";

    // stop listening to incoming events
    _isListening = false;
    pthread_join(_runt, NULL);

    // stop the monitoring threads
    if (_isMonitoring) {
        _isMonitoring = false;
        // threads for monitoring proxy dis-/re-connection
        for (int i = 0; i < _numProxy; i++) {
            pthread_cancel(_monitort[i]);
            pthread_join(_monitort[i], NULL);
            delete _mworkers[i];
            _mworkers[i] = 0;
        }
    }

    // close the sockets connected to Proxies
    for (int i = 0; i < _numProxy; i++) {
        delete _mworkers[i];
        _proxy[i]->close();
        delete _proxy[i];
    }

    _socket->close();
    _cxt.close();
    delete _socket;

    LOG(WARNING) << "Terminated Coordinator ...";
}

bool AgentCoordinator::registerToProxy(bool listenToProxy) {
    Config &config = Config::getInstance();

    for (int i = 0; i < config.getNumProxy(); i++) {
        ProxyMonitorInfo *info = new ProxyMonitorInfo();
        info->coordinator = this;
        info->i = i;
        info->listenToProxy = listenToProxy;
        pthread_create(&_monitort[i], NULL, _registerToProxy, info);
    }
    _isMonitoring = true;

    return true;
}

bool AgentCoordinator::sendRegisterMessageToProxy(int i, std::string proxyAddr, bool needsConnect) {
    // register myself and my containers to proxy
    CoordinatorEvent event, revent;
    event.opcode = Opcode::REG_AGENT_REQ;
    prepareStatus(event);

    if (needsConnect) {
        // connect to proxy
        try {
            _proxy[i]->connect(proxyAddr);
        } catch (zmq::error_t &e) {
            LOG(ERROR) << "Failed to connect to proxy coordinator, " << e.what();
            return false;
        }
    }

    try {
        // send out the registration request 
        if (Coordinator::sendEventMessage(*(_proxy[i]), event) == 0) {
            LOG(ERROR) << "Failed to send message to proxy coordinator for registeration";
            return false;
        }
        // get the registration reply 
        while (Coordinator::getEventMessage(*(_proxy[i]), revent) == 0) {
            if (!_isMonitoring) {
                LOG(ERROR) << "Failed to get reply from proxy coordinator for registeration";
                return false;
            }
        }
    } catch (zmq::error_t &e) {
        LOG(ERROR) << "Failed to connect to proxy coordinator, " << e.what();
        return false;
    }

    if (revent.opcode != Opcode::REG_AGENT_REP_SUCCESS) {
        LOG(ERROR) << "Failed to register to Proxy at " << proxyAddr;
        return false;
    }

    return true;
}

void *AgentCoordinator::listenToProxy(void *arg) {
    Config &config = Config::getInstance();
    AgentCoordinator *self = (AgentCoordinator *) arg;

    // start listening for requests
    std::string agentIP = config.listenToAllInterfaces()? "0.0.0.0" : config.getAgentIP();
    std::string agentAddr = IO::genAddr(agentIP, config.getAgentCPort());
    self->_socket->bind(agentAddr);

    self->_isListening = true;
    try {
        while (self->_isListening) {
            CoordinatorEvent event;
            if (Coordinator::getEventMessage(*(self->_socket), event) == 0)
                continue;
            switch (event.opcode) {
            case Opcode::SYN_PING: // send PONG for PING
                DLOG(INFO) << "Get PING";
                event.opcode = Opcode::ACK_PING;
                Coordinator::sendEventMessage(*(self->_socket), event);
                DLOG(INFO) << "Sent PONG";
                break;
            case Opcode::UPD_AGENT_REQ: // update Agent info
                event.opcode = Opcode::UPD_AGENT_REP;
                self->prepareStatus(event);
                Coordinator::sendEventMessage(*(self->_socket), event);
                break;
            case Opcode::GET_SYSINFO_REQ:
                event.opcode = Opcode::GET_SYSINFO_REP;
                self->prepareSysInfo(event);
                Coordinator::sendEventMessage(*(self->_socket), event);
                break;
            default:
                LOG(WARNING) << "Unknown opcode = " << event.opcode;
                break;
            }
        }
    } catch (zmq::error_t &e) {
        LOG(WARNING) << "Agent coordinator stops listening, " << e.what();
    }


    return NULL;
}

void AgentCoordinator::prepareStatus(CoordinatorEvent &event) {
    Config &config = Config::getInstance();

    event.agentAddr = IO::genAddr(config.getAgentIP(), config.getAgentPort());
    event.agentHostType = _hostType;
    event.cport = config.getAgentCPort();

    if (_cm == NULL) {
        event.numContainers = 0;
        LOG(ERROR) << "No container manager assigned!";
        return;
    }

    event.numContainers = _cm->getNumContainers();
    event.containerIds = new int[event.numContainers];
    _cm->getContainerIds(event.containerIds);
    event.containerType = new unsigned char[event.numContainers];
    _cm->getContainerType(event.containerType);
    event.containerUsage = new unsigned long int[event.numContainers];
    event.containerCapacity = new unsigned long int[event.numContainers];
    _cm->getContainerUsage(event.containerUsage, event.containerCapacity);
}

void AgentCoordinator::prepareSysInfo(CoordinatorEvent &event) {
    event.sysinfo = _sysinfo[_latestInfoIdx];
    event.sysinfo.hostType = _hostType;
}

void AgentCoordinator::MonitorProxyWorker::on_event_connected(const zmq_event_t &event, const char *addr) {
    DLOG(INFO) << "(Re-)Connected event on " << addr;
    try {
        _coordinator->sendRegisterMessageToProxy(_coordinator->_proxyMap.at(addr), addr, /* needsConnect */ false);
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to find record for registering again after re-connection?";
    }
}

void *AgentCoordinator::monitorProxy(void *arg) {
    ProxyMonitorInfo *info = (ProxyMonitorInfo *) arg;
    int i = info->i; 
    AgentCoordinator *coordinator = info->coordinator;
    try {
        // start a monitor socket to handle re-connecting events
        delete info;
        char connectPoint[64];
        sprintf(connectPoint, "%s-%02d", PROXY_MONITOR_CONN_POINT, i);
        coordinator->_mworkers[i]->monitor(*(coordinator->_proxy[i]), connectPoint);
        LOG(WARNING) << "Agent coordinator stops monitoring its socket (" << i << ")";
    } catch (zmq::error_t &e) {
        LOG(WARNING) << "Agent coordinator encounters an error when monitoring its socket (" << i << "), " << e.what();
    }
    return NULL;
}

void *AgentCoordinator::_registerToProxy(void *arg) {
    Config &config = Config::getInstance();

    ProxyMonitorInfo *info = (ProxyMonitorInfo *) arg;

    std::string proxyAddr = IO::genAddr(config.getProxyIP(info->i), config.getProxyCPort(info->i));

    // register to proxy
    if (info->coordinator->sendRegisterMessageToProxy(info->i, proxyAddr) == false)
        return (void*) -1;

    LOG(INFO) << "Registered to Proxy " << info->i << " at " << proxyAddr;

    // setup monitor to register on re-connection
    if (info->listenToProxy) {
        info->coordinator->_proxyMap.insert(std::pair<std::string, int>(proxyAddr, info->i));
        return monitorProxy(info);
    } else {
        free(info);
    }
    return 0;
}
