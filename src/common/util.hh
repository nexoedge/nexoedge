// SPDX-License-Identifier: Apache-2.0

#ifndef __UTIL_HH__
#define __UTIL_HH__

#include "config.hh"
#include <zmq.hpp>

class Util {
public:
    /**
     * Set socket options according to configuration
     *
     * @param[in, out] socket socket to set the options
     **/
    static void setSocketOptions(zmq::socket_t *socket) {
        Config &config = Config::getInstance();
        
        int bufferSize = config.getTcpBufferSize();
        socket->setsockopt(ZMQ_SNDBUF, bufferSize);
        socket->setsockopt(ZMQ_RCVBUF, bufferSize);

        if (socket == 0 || !config.manualTcpKeepAlive())
            return;

        socket->setsockopt(ZMQ_TCP_KEEPALIVE, 1);
        socket->setsockopt(ZMQ_TCP_KEEPALIVE_IDLE, config.getTcpKeepAliveIdle());
        socket->setsockopt(ZMQ_TCP_KEEPALIVE_INTVL, config.getTcpKeepAliveIntv());
        socket->setsockopt(ZMQ_TCP_KEEPALIVE_CNT, config.getTcpKeepAliveCnt());
    }

    static bool includeSample(int population, double samplingRate) {
        return rand() % (int)(population * 1e3) <= samplingRate * (population * 1e3);
    } 
};

#endif //define __UTIL_HH__
