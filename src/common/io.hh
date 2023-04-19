// SPDX-License-Identifier: Apache-2.0

#ifndef __IO_HH__
#define __IO_HH__

#include <zmq.hpp>

#include "../ds/chunk_event.hh"

class IO {
public:
    /**
     * Parse an incoming chunk event from socket
     *
     * @param[in]  socket socket to receive the event
     * @param[out] event chunk event parsed from the socket
     *
     * @return number of bytes received from the socket
     **/
    static unsigned long int getChunkEventMessage(zmq::socket_t &socket, ChunkEvent &event);

    /**
     * Send an chunk event over a socket
     *
     * @param[in] socket socket to send the event
     * @param[in] event chunk event to send over the socket
     *
     * @return number of bytes sent
     **/
    static unsigned long int sendChunkEventMessage(zmq::socket_t &socket, const ChunkEvent &event);

    /**
     * Generate an address string with given IP and port ("tcp://IP:port")
     *
     * @param ip IP of the address
     * @param port port of the address
     *
     * @return the address string
     **/
    static std::string genAddr(std::string ip, unsigned port);

    /**
     * Extract IP from an address string ("tcp://IP:port")
     *
     * @param addr the address
     *
     * @return IP in the address string
     **/
    static std::string getAddrIP(std::string addr);

    /**
     * Issue chunk request to agent
     *
     * @param arg pointer to a IO::RequestMeta structure
     * @return whether the operation is successful, NULL if sucessful, non-NULL otherwise
     **/
    static void *sendChunkRequestToAgent(void *arg);

    typedef struct {
        int containerId;                  /**< id of the first container of the agent */
        bool isFromProxy;                 /**< whether this request is sent from Proxy */
        struct {
            zmq::context_t *cxt;          /**< zeromq context */
            zmq::socket_t *socket;        /**< zeromq socket */
        };
        std::string address;              /**< agent address (tcp://[ip]:[port]) */
        ChunkEvent *request;              /**< requested chunk event */
        ChunkEvent *reply;                /**< replied chunk event */
    } RequestMeta;

private:
    /** *
     * Tell whether the chunk event message should be from proxy
     * 
     * @param opcode operation code of the chunk event
     * 
     * @return whether the message is from proxy
    **/
    static bool isFromProxy(unsigned short opcode);
    /** *
     * Tell whether the chunk event message should be from agent
     * 
     * @param opcode operation code of the chunk event
     * 
     * @return whether the message is from agent
    **/
    static bool isFromAgent(unsigned short opcode);
    /**
     * Tell whether the chunk event message should contain data
     *
     * @param opcode operation code of the chunk event
     *
     * @return whether the message should contain data
     **/
    static bool hasData(unsigned short opcode);

    /**
     * Tell whether the chunk event message should contain container Ids
     *
     * @param opcode operation code of the chunk event
     *
     * @return whether the message should contain container Ids
     **/
    static bool hasContainerIds(unsigned short opcode);

    /**
     * Tell whether the chunk event message should contain chunks
     *
     * @param opcode operation code of the chunk event
     *
     * @return whether the message should contain chunks
     **/
    static bool hasChunkData(unsigned short opcode);

    /**
     * Tell whether the chunk event message should contain coding information
     *
     * @param opcode operation code of the chunk event
     *
     * @return whether the message should contain coding information
     **/
    static bool needsCoding(unsigned short opcode);

    /**
     * Tell whether the chunk event message should contain repair chunk information
     *
     * @param opcode operation code of the chunk event
     *
     * @return whether the message should contain repair chunk information
     **/
    static bool hasRepairChunkInfo(unsigned short opcode);

    /**
     * Tell the actual factor of incoming chunks
     *
     * @param opcode operation code of the chunk event
     *
     * @return the factor on the number of incoming chunks w.r.t. that specified in event.numChunks
     **/
    static int getNumChunkFactor(unsigned short opcode);
};

#endif // define __IO_HH__
