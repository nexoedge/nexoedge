// SPDX-License-Identifier: Apache-2.0

#include <string>
#include <exception>

#include <zmq.hpp>

#include "io.hh"
#include "../common/config.hh"
#include "../common/io.hh"
#include "../common/util.hh"

AgentIO::~AgentIO() {
    if (_frontend)
       _frontend->close();
    if (_backend)
       _backend->close();
    delete _frontend;
    delete _backend;
}

void AgentIO::run(const char *workerAddr) {
    // setup a zmq proxy that listen to the chunk events from Proxy, and distribute them to chunk workers
    Config &config = Config::getInstance();
    std::string ip = config.listenToAllInterfaces()? "0.0.0.0" : config.getAgentIP();
    unsigned short listenPort = config.getAgentPort();
    std::string agentAddr = IO::genAddr(ip, listenPort);

    // frontend, bind to interface(s) and listen to Proxy requests
    _frontend = new zmq::socket_t(*_cxt, ZMQ_ROUTER);
    Util::setSocketOptions(_frontend);
    _frontend->bind(agentAddr);

    // backend, bind to internal address for distributing events to workers
    _backend = new zmq::socket_t(*_cxt, ZMQ_DEALER);
    _backend->bind(workerAddr);

    // start running the proxy (blocking)
    try {
        zmq::proxy(*_frontend, *_backend, NULL);
    } catch (std::exception &e) {
    }
}

