// SPDX-License-Identifier: Apache-2.0

#ifndef __PROXY_IO_HH__
#define __PROXY_IO_HH__

#include <string>
#include <map>
#include <mutex>

#include <zmq.hpp>

#include "../common/io.hh"
#include "../ds/chunk_event.hh"

class ProxyIO {
public:
    ProxyIO(std::map<int, std::string> *containerToAgentMap);
    ~ProxyIO();

    struct RequestMeta {
        int containerId;
        ProxyIO *io;
        ChunkEvent *request;
        ChunkEvent *reply;
        TagPt *network;

        RequestMeta() {
            reset();
        }

        ~RequestMeta() {
            reset();
        }

        void reset() {
            containerId = -999;
            io = 0;
            request = 0;
            reply = 0;
            network = 0;
        }
    } ;

    /**
     * Send a chunk event request to agent (and get the reply)
     *
     * @param arg    pointer to a ProxyIO::RequestMeta structure
     * @return whether the operation is successful, NULL if sucessful, non-NULL otherwise
     **/
    static void *sendChunkRequestToAgent(void *arg);

private:
    std::map<int, std::string> *_containerToAgentMap;           /**< container id to agent address mapping */
    std::map<int, zmq::socket_t*> _containerToSocketMap;        /**< container id to socket mapping */
    std::mutex _lock;

    zmq::context_t _cxt;                                        /**< zeromq context */

};

#endif // define __PROXY_IO_HH__
