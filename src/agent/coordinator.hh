// SPDX-License-Identifier: Apache-2.0

#ifndef __AGENT_COORDINATOR_HH__
#define __AGENT_COORDINATOR_HH__

#include <pthread.h>
#include <map>

#include <zmq.hpp>

#include "container_manager.hh"
#include "../common/coordinator.hh"

class AgentCoordinator : Coordinator {
public:
    AgentCoordinator(ContainerManager *cm);
    ~AgentCoordinator();

    /**
     * Register the Agent and its containers to Proxy
     *
     * @param[in] listenToProxy   whether to also start a background thread to listen proxy requests
     * @return whether the registration is successful
     **/
    bool registerToProxy(bool listenToProxy = true);

    /**
     * Listen to Proxy requests
     *
     * @param[in] arg             pointer to an instance of Agent coordinator
     **/
    static void *listenToProxy(void *arg);

private:

    /**
     * Implementation of monitoring socket to handle events on connections
     **/
    class MonitorProxyWorker : public zmq::monitor_t {
    public:
        MonitorProxyWorker(AgentCoordinator *coordinator) {
            _coordinator = coordinator;
        }

        void on_event_connected(const zmq_event_t &event, const char *addr);
        //void on_event_connect_delayed(const zmq_event_t &event, const char *addr);
        //void on_event_accepted(const zmq_event_t &event, const char *addr);
        //void on_event_disconnected(const zmq_event_t &event, const char *addr);

        AgentCoordinator *_coordinator;   /**< an instance of Agent coordinator */
    };

    typedef struct {
        AgentCoordinator *coordinator;    /**< an instance of Agent coodinator */
        int i;                            /**< Proxy number */
        bool listenToProxy;               /**< whether to start monitoring the connection to Proxy */
    } ProxyMonitorInfo;                   /**< info for registering to Proxy and init the Proxy connection monitor */

    /**
     * Prepare the event with Agent current status 
     *
     * @param[out] event          prepared coordinator event 
     * @remark this function sets CoordinatorEvent::agentAddr, CoordinatorEvent::numContainers, CoordinatorEvent::containerIds, CoordinatorEvent::containerUsage
     */
    void prepareStatus(CoordinatorEvent &event);

    /**
     * Prepare the event with Agent system status
     *
     * @param[out] event          prepared coordinator event 
     * @remark this function sets all fields in CoordinatorEvent::sysinfo
     */
    void prepareSysInfo(CoordinatorEvent &event);

    /**
     * Send register message to designated Proxy
     *
     * @param[in] i               array index of Proxy sockets
     * @param[in] proxyAddr       address of Proxy
     * @return whether the registration is sucessful
     **/
    bool sendRegisterMessageToProxy(int i, std::string proxyAddr, bool needsConnect = true);

    /**
     * Monitor Proxy connection for re-connection
     *
     * @param[in] arg             pointer to an instance of ProxyMonitorInfo
     * @return always NULL
     **/
    static void *monitorProxy(void *arg);

    /**
     * Register Agent to Proxy
     *
     * @param[in] listenToProxy   whether to also start a background thread to listen Proxy requests
     * @return (void *) -1 if failed to send a registeration message, NULL otherwise
     **/
    static void *_registerToProxy(void *listenToProxy);

    zmq::context_t _cxt;                            /**< zero-mq context */
    zmq::socket_t *_proxy[MAX_NUM_PROXY];           /**< socket connect to Proxy for registration and alive detection */
    zmq::socket_t *_socket;                         /**< socket listening to incoming Proxy requests */
    pthread_t _runt;                                /**< background thread for listening Proxy requests */
    bool _isListening;                              /**< whether there is a background thread running */
    int _numProxy;                                  /**< record the number of Proxy connected to when init */
    pthread_t _monitort[MAX_NUM_PROXY];             /**< threads for monitoring Proxy sockets */
    bool _isMonitoring;                             /**< whether the monitoring on Proxy has started */
    MonitorProxyWorker *_mworkers[MAX_NUM_PROXY];   /**< proxy monitoring sockets */
    std::map<std::string, int> _proxyMap;           /**< mapping of Proxy address to array index */

    ContainerManager *_cm;                          /**< container manager */
};

#endif // define __AGENT_COORDINATOR_HH__
