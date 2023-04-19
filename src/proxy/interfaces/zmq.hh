// SPDX-License-Identifier: Apache-2.0

#ifndef __PROXY_INT_ZMQ_HH__
#define __PROXY_INT_ZMQ_HH__

#include <pthread.h>

#include <zmq.hpp>

#include "../../common/zmq_int_define.hh"
#include "../proxy.hh"
#include "../../ds/request_reply.hh"

class ProxyZMQIntegration {
public:
    ProxyZMQIntegration();
    ProxyZMQIntegration(Proxy *proxy);
    ProxyZMQIntegration(ProxyCoordinator *coordinator, std::map<int, std::string> *map, BgChunkHandler::TaskQueue *queue = 0);
    ~ProxyZMQIntegration();

    static void *run(void *arg);


private:
    Proxy *_proxy;                                         /**< proxy */
    ProxyCoordinator *_coordinator;
    std::map<int, std::string> *_containerToAgentMap;
    BgChunkHandler::TaskQueue *_queue;

    bool _releaseProxy;                                    /**< whether to release proxy upon destruction */

    zmq::context_t _cxt;                                   /**< zmq context */
    zmq::socket_t *_frontend;
    zmq::socket_t *_backend;

    static const char *_workerAddr;                        /**< worker address for zmq socket */

    int _numWorkers;                                       /**< number of worker threads */
    pthread_t _workers[MAX_NUM_WORKERS];                   /**< pthread structure for worker threads */

    pthread_barrier_t _stopRunning;                        /**< barrier when the interface stops running */
    bool _isRunning;                                       /**< whether the interface is running */

    bool stop();

    /**
     * Worker procedure for handling requests
     *
     * @param[in] arg       an instance of ProxyZMQIntegration
     * @return always NULL
     **/
    static void *handleRequests(void *arg);

    /**
     * Parse a request from client
     *
     * @param[in] socket    socket connected to client
     * @param[in,out] req   file request
     * @return 0 if a request is successfully received, 1 on protocol error, EAGAIN on probe timeout
     **/
    static int getRequest(zmq::socket_t &socket, Request &req); 

    /**
     * Parse a request from client
     *
     * @param[in] socket    socket connected to client
     * @param[in,out] rep   reply to file request
     * @return whether a reply is successfully sent 
     **/
    static bool sendReply(zmq::socket_t &socket, Reply &rep);

    /**
     * Tell whether file data is expected in the request
     *
     * @param[in] op        client operation code
     * @return whether file data is expected
     **/
    static bool hasFileData(int op) {
        return (
            op == ClientOpcode::WRITE_FILE_REQ ||
            op == ClientOpcode::APPEND_FILE_REQ ||
            op == ClientOpcode::OVERWRITE_FILE_REQ ||
            false
        );
    }

    /**
     * Tell whether only the operation code is expected in the request
     *
     * @param[in] op        client operation code
     * @return whether only the operation code is expected
     **/
    static bool hasOpcodeOnly(int op) {
        return (
            op == ClientOpcode::GET_CAPACITY_REQ ||
            op == ClientOpcode::GET_AGENT_STATUS_REQ ||
            op == ClientOpcode::GET_PROXY_STATUS_REQ ||
            op == ClientOpcode::GET_BG_TASK_PRG_REQ ||
            false
        );
    }

    /**
     * Tell whether only the namespace id is expected after the operation code in the request
     *
     * @param[in] op        client operation code
     * @return whether only the namespace id is expected after the operation code
     **/
    static bool hasNamespaceIdOnly(int op) {
        return false;
    }

    /**
     * Tell whether the file size is expected in the request
     *
     * @param[in] op         client operation code
     * @return whether the file size is expected
     **/
    static bool hasFileSize(int op) {
        return (
            op == ClientOpcode::WRITE_FILE_REQ ||
            op == ClientOpcode::APPEND_FILE_REQ ||
            op == ClientOpcode::OVERWRITE_FILE_REQ ||
            op == ClientOpcode::READ_FILE_RANGE_REQ ||
            op == ClientOpcode::COPY_FILE_REQ ||
            false
        );
    }

    /**
     * Tell whether the file offset is expected in the request
     *
     * @param[in] op         client operation code
     * @return whether the file offset is expected
     **/
    static bool hasFileOffset(int op) {
        return (
            op == ClientOpcode::APPEND_FILE_REQ ||
            op == ClientOpcode::OVERWRITE_FILE_REQ ||
            op == ClientOpcode::READ_FILE_RANGE_REQ ||
            op == ClientOpcode::COPY_FILE_REQ ||
            false
        );
    }

    /**
     * Tell whether only the opcode needs be sent in the reply
     *
     * @param[in] op        client operation code
     * @return whether only the opcode needs to be sent
     **/
    static bool replyOpcodeOnly(int op) {
        return 
            !replyFileData(op) &&
            !replyStats(op) &&
            !replyFileList(op) &&
            op != GET_APPEND_SIZE_REP_SUCCESS &&
            op != GET_READ_SIZE_REP_SUCCESS &&
            op != APPEND_FILE_REP_SUCCESS &&
            op != OVERWRITE_FILE_REP_SUCCESS &&
            op != GET_AGENT_STATUS_REP_SUCCESS &&
            op != GET_PROXY_STATUS_REP_SUCCESS &&
            op != GET_BG_TASK_PRG_REP_SUCCESS &&
            op != GET_REPAIR_STATS_REP_SUCCESS &&
            true
        ;
    }

    /**
     * Tell whether the file data needs be sent in the reply
     *
     * @param[in] op        client operation code
     * @return whether the file data needs to be sent
     **/
    static bool replyFileData(int op) {
        return (
            op == READ_FILE_REP_SUCCESS ||
            op == READ_FILE_RANGE_REP_SUCCESS ||
            false
        );
    }

    /**
     * Tell whether the storage statistics needs be sent in the reply
     *
     * @param[in] op        client operation code
     * @return whether the storage statistics needs to be sent
     **/
    static bool replyStats(int op) {
        return (op == GET_CAPACITY_REP_SUCCESS);
    }

    /**
     * Tell whether the file list needs to be sent in the reply
     *
     * @param[in] op        client operation code
     * @return whether the file list needs to be sent
     **/
    static bool replyFileList(int op) {
        return (op == GET_FILE_LIST_REP_SUCCESS);
    }
};

#endif //define __PROXY_INT_ZMQ_HH__
