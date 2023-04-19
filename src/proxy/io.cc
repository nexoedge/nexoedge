// SPDX-License-Identifier: Apache-2.0

#include <glog/logging.h>

#include "io.hh"
#include "../common/config.hh"
#include "../common/util.hh"

ProxyIO::ProxyIO(std::map<int, std::string> *containerToAgentMap) {
    _cxt = zmq::context_t(Config::getInstance().getProxyNumZmqThread());
    _containerToAgentMap = containerToAgentMap;
}

ProxyIO::~ProxyIO() {
    LOG(WARNING) << "Terminating Proxy IO";
    for (auto it : _containerToSocketMap) {
        it.second->close();
        delete it.second;
    }
    _cxt.close();
    LOG(WARNING) << "Terminated Proxy IO";
}

void *ProxyIO::sendChunkRequestToAgent(void *arg) {
    RequestMeta &meta = *((RequestMeta*) arg);

    // convert the request metadata from ProxyIO::RequestMeta to IO::RequestMeta
    IO::RequestMeta ioMeta;
    ioMeta.isFromProxy = true;
    ioMeta.reply = meta.reply;
    ioMeta.request = meta.request;
    ioMeta.containerId = meta.containerId;
    try {
        ioMeta.address = meta.io->_containerToAgentMap->at(meta.containerId);
    } catch (std::exception &e) {
        LOG(ERROR) << "Failed to find agent addresss, container id = " << meta.containerId;
        return (void *) -1;
    }

    // TAGPT (start): network
    if (meta.network != NULL) {
        meta.network->markStart();
    }

    if (Config::getInstance().reuseDataConn()) {
        meta.io->_lock.lock();
        try {
            ioMeta.socket = meta.io->_containerToSocketMap.at(meta.containerId);
        } catch (std::exception &e) {
            ioMeta.socket = new zmq::socket_t(meta.io->_cxt, ZMQ_REQ);
            meta.io->_containerToSocketMap.insert(std::pair<int, zmq::socket_t*>(meta.containerId, ioMeta.socket));
            Util::setSocketOptions(ioMeta.socket);
            int timeout = Config::getInstance().getFailureTimeout();
            ioMeta.socket->setsockopt(ZMQ_SNDTIMEO, timeout);
            ioMeta.socket->setsockopt(ZMQ_RCVTIMEO, timeout);
            ioMeta.socket->setsockopt(ZMQ_LINGER, timeout);
            ioMeta.socket->connect(ioMeta.address);
        }
        meta.io->_lock.unlock();
    } else {
        ioMeta.cxt = &meta.io->_cxt;
    }

    void *retVal = IO::sendChunkRequestToAgent((void*) &ioMeta);
    // return IO::sendChunkRequestToAgent((void*) &ioMeta);

    // TAGPT (end): network
    if (meta.network != NULL) {
        meta.network->markEnd();
    }
    
    pthread_exit(retVal);
}

