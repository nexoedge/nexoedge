// SPDX-License-Identifier: Apache-2.0

#ifndef __Agent_IO_HH__
#define __Agent_IO_HH__

#include <string>

#include "../common/define.hh"

class AgentIO {
public:
    /**
     * Constructor
     *
     * @param[in] cxt               pointer to an instance of zero-mq context 
     **/
    AgentIO(zmq::context_t *cxt) {
        _cxt = cxt;
        _frontend = 0;
        _backend = 0;
    }
    ~AgentIO();

    /**
     * Start receiving the chunk events from external network (Proxy) and queue them up for workers to process
     *
     * @param[in] workerAddr        address of the queue for chunk evnets
     **/
    void run(const char *workerAddr);

private:
    zmq::context_t *_cxt;                /**< zero-mq context */
    zmq::socket_t *_frontend, *_backend;   /**< socket holder of frontend and backend sockets */
};
#endif // define __Agent_IO_HH__
