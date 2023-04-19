// SPDX-License-Identifier: Apache-2.0

#ifndef __AGENT_HH__
#define __AGENT_HH__

#include <atomic>
#include <pthread.h>

#include <zmq.hpp>

#include "container_manager.hh"
#include "coordinator.hh"
#include "io.hh"
#include "../common/define.hh"
#include "../ds/chunk_event.hh"
#include "../common/benchmark/benchmark.hh"

class Agent {
public:
    Agent();
    ~Agent();

    /**
     * Main loop for handling incoming events
     *
     * @param[in] reg        whether it should register itself to Proxy
     **/
    void run(bool reg = true);

    /**
     * Print statistics
     **/
    void printStats();

    zmq::context_t _cxt; /**< socket context for zeromq */
private:

    /**
     * Internal function for (multi-threaded) event handling
     *
     * @param[in] arg        pointer to an instance of Agent
     *
     * @return always NULL
     **/
    static void *handleChunkEvent(void *arg);

    /**
     * Increment the total ingress traffic (chunk and header)
     *
     * @param[in] traffic    amount of traffic to add in bytes
     **/
    void addIngressTraffic(unsigned long int traffic);

    /**
     * Increment the total egress traffic (chunk and header)
     *
     * @param[in] traffic    amount of traffic to add in bytes
     **/
    void addEgressTraffic(unsigned long int traffic);

    /**
     * Increment the ingress traffic for chunks
     *
     * @param[in] traffic    amount of traffic to add in bytes
     **/
    void addIngressChunkTraffic(unsigned long int traffic);

    /**
     * Increment the egress traffic for chunks
     *
     * @param[in] traffic    amount of traffic to add in bytes
     **/
    void addEgressChunkTraffic(unsigned long int traffic);

    /**
     * Increment the operation count
     *
     * @param[in] success    whether the operation is successful
     **/
    void incrementOp(bool success = true);

    // modules
    AgentIO *_io;                                     /**< IO module */
    ContainerManager *_containerManager;              /**< container manager module */
    AgentCoordinator *_coordinator;                   /**< coordinator */

    // workers
    int _numWorkers;                                  /**< number of workers for event handling */
    pthread_t _workers[MAX_NUM_WORKERS];              /**< pthread structure for worker threads */

    // settings for zmq
    const char *_workerAddr = "inproc://agentworker"; /**< internal address of event queue for workers */

    // event count
    std::atomic<int> _eventCount;                     /**< evnet id counter */

    // stats
    struct {
        struct {
            unsigned long int in;
            unsigned long int out;
        } traffic;                                    /**< total ingress/egress traffic */
        struct {
            unsigned long int in;
            unsigned long int out;
        } chunk;                                      /**< ingress/egress chunk traffic */
        struct {
            unsigned long int success;
            unsigned long int fail;
        } ops;                                        /**< operation count */
        pthread_mutex_t lock;                         /**< lock for concurrent access */
    } _stats;                                         /**< statistics for agent */
};

#endif // define __AGENT_HH__
