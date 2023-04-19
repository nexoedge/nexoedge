// SPDX-License-Identifier: Apache-2.0

#ifndef __PROXY_COORDINATOR_HH__
#define __PROXY_COORDINATOR_HH__

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include <pthread.h>

#include <zmq.hpp>

#include "../common/define.hh"
#include "../common/coordinator.hh"
#include "../ds/coding_meta.hh"

#define BITS_FOR_CONTAINERS_PER_AGENT (4)
#define NUM_MAX_CONTAINER_PER_AGENT (1 << BITS_FOR_CONTAINERS_PER_AGENT)

class ProxyCoordinator : Coordinator {
public:
    /**
     * Constructor
     *
     * @param[in] containerToAgentMap pointer to the map that store the mapping from containers to agents
     **/
    ProxyCoordinator(std::map<int, std::string> *containerToAgentMap);
    ~ProxyCoordinator();

    struct AgentInfo {
        union{
            zmq::socket_t *socket;                                        /**< zero-mq socket for sending request to agent */
            bool alive;                                                   /**< liveness of agent, if not using socket */
        };
        unsigned char hostType;                                           /**< type of host agent is running on */
        bool isNear;                                                      /**< whether the agent is near to Proxy */
        char *addr;                                                       /**< agent address */
        int numContainers;                                                /**< number of containers managed by agent */
        int startingContainerIndex;                                       /**< container index to start with */
        int containerIds[NUM_MAX_CONTAINER_PER_AGENT] = { INVALID_CONTAINER_ID }; /**< ids of containers managed by agent */
        unsigned long int containerUsage[NUM_MAX_CONTAINER_PER_AGENT];    /**< storage usage of containers managed by agent */
        unsigned long int containerCapacity[NUM_MAX_CONTAINER_PER_AGENT]; /**< storage capacity of containers managed by agent */
        unsigned char containerType[NUM_MAX_CONTAINER_PER_AGENT];         /**< type of containers managed by agent */
        std::multimap<float, int> utilizationMap;                         /**< container index sorted by utilization */
        SysInfo sysinfo;

        AgentInfo() {
            hostType = HostType::HOST_TYPE_UNKNOWN;
            addr = 0;
            isNear = false;
            numContainers = 0;
            startingContainerIndex = 0;
        }

        ~AgentInfo() {
            free(addr);
        }
    };

    /**
     * Core of coordinator for listening and handling incoming events
     * (Expect to be run using pthead_create())
     *
     * @param[in] arg pointer to the instance of proxy coordinator
     * @return NULL
     **/
    static void *run(void *arg);

    /**
     * Check the liveness of containers
     * 
     * @param[in] containerIds      list of ids of the container to check
     * @param[in] numContainers     number of the containers to check
     * @param[out] status           pre-allocated list of status of the containers to check, its size should be equal to numContainers
     * @param[in] updateStatusFirst whether to update agent status before failure check
     * @param[in] checkAllFailures  whether to check for all failures
     * @param[in] treatUnusedAsOffline  whether to treat unused container as offline
     * @return if checkAllFailures is true, return number of failed chunks; otherwise, return whether there are failed chunks
     **/
    int checkContainerLiveness(const int containerIds[], int numContainers, bool status[], bool updateStatusFirst = true, bool checkAllFailures = true, bool treatUnusedAsOffline = false);

    /**
     * Get number the alive containers
     * 
     * @param[in] skipFull whether treat full containers as not alive/available
     * @param[in] storageClass storage class for estimating container fullness
     * @return number of alive containers
     **/
    int getNumAliveContainers(bool skipFull = false, const std::string storageClass = "");

    /**
     * Find spare containers excluding the existing containers
     * 
     * @param[in] containerIds      list of ids of the container to check
     * @param[in] numContainers     number of the containers to check
     * @param[in] status            pre-allocated list of status of the containers to check, its size should be equal to numContainers
     * @param[out] spareContainers  list of ids of spare containers, its size should be equal to numSpare
     * @param[in] numSpare          number of spare containers required
     * @param[in] fsize             length of data to write
     * @param[in] codingMeta        coding metadata for container selection
     * @return number of spare containers found
     **/
    int findSpareContainers(const int *containerIds, int numContainers, const bool *status, int spareContainers[], int numSpare, unsigned long int fsize, const CodingMeta &codingMeta);

    /**
     * Find spare containers excluding the existing containers
     * 
     * @param[in] containerIds      list of ids of the container to check
     * @param[in] numContainers     number of the containers to check
     * @param[in] status            list of status of the containers to check, its size should be equal to numContainers
     * @param[out] chunkGroups      list of ids of chunks in the same group, its size should be equal to (numContainers + 1) * numContainers, each group occupies a row of (numContainers + 1) elements, and the first element is the group size 
     * @return number of chunk groups found
     **/
    int findChunkGroups(const int *containerIds, int numContainers, const bool *status, int chunkGroups[]);

    /**
     * Update agent status
     **/
    void updateAgentStatus();

    /**
     * Obtain agent status
     * 
     * @param[out] info pointer to the agent info
     * @return number of agents
     **/
    int getAgentStatus(AgentInfo **info);

    /**
     * Obtain proxy status
     * 
     * @param[out] info pointer to the proxy info
     * @return always true
     **/
    bool getProxyStatus(SysInfo &info);
    /**
     * Obtain storage usage
     *
     * @param[in,out] usage      current usage
     * @param[in,out] capacity   total capacity
     * @param[in] storageClass   storage class for estimating the remaining storage
     **/
    void getStorageUsage(unsigned long int &usage, unsigned long int &capacity, const std::string storageClass = "");

    /**
     * Pre-register the agents listed in the configuration file
     **/
    void registerPresetAgents();

private:
    /**
     * Implementation of monitoring socket to handle events on connections
     **/
    class MonitorAgentWorker : public zmq::monitor_t {
    public:
        MonitorAgentWorker(ProxyCoordinator *coordinator) {
            _coordinator = coordinator;
        }

        //void on_event_connected(const zmq_event_t &event, const char *addr);
        //void on_event_connect_delayed(const zmq_event_t &event, const char *addr);
        //void on_event_connect_retried(const zmq_event_t &event, const char *addr);
        //void on_event_listening(const zmq_event_t &event, const char *addr);
        void on_event_accepted(const zmq_event_t &event, const char *addr);
        //void on_event_accept_failed(const zmq_event_t &event, const char *addr);
        void on_event_closed(const zmq_event_t &event, const char *addr);
        //void on_event_close_failed(const zmq_event_t &event, const char *addr);
        void on_event_disconnected(const zmq_event_t &event, const char *addr);
        //void on_event_unknown(const zmq_event_t &event, const char *addr);

    private:
        ProxyCoordinator *_coordinator;   /**< an instance of proxy coordinator */
    };

    /**
     * Start monitoring socket events (for network connections with agents)
     * (Expect to be run using pthead_create())
     * 
     * @param arg pointer to the instance of proxy coordinator
     **/
    static void *monitorAgents(void *arg);

    /**
     * Mark an agent as alive
     *
     * @param ip IP address of the agent
     * @return whether the agent status is updated is successful
     **/
    bool setAgentAlive(std::string ip);

    /**
     * Mark an agent as down
     *
     * @param ip IP address of the agent
     * @return whether the agent status is updated is successful
     **/
    bool setAgentDown(std::string ip);

    /**
     * Ping agents to update their liveness 
     **/
    void pingAgents();

    /**
     * Register an agent
     **/ 
    bool registerAgent(CoordinatorEvent &event);

    /**
     * Request status update from agent through a socket and save the status update to the event
     **/
    bool requestStatusUpdateFromAgent(CoordinatorEvent &event, zmq::socket_t &socket, const std::string &ip);
    /**
     * Print agnets
     **/
    void printAgents();

    zmq::context_t _cxt;                                         /**< socket context for zeromq */
    std::map<int, std::string> *_containerToAgentMap;            /**< mapping from container to agent address */
    std::map<std::string, bool> _agentStatus;                    /**< status of agents */
    zmq::socket_t *_socket;                                      /**< coordinator socket for listening incoming events */
    std::mutex _agentsLock;                                      /**< lock on the mapping from address to socket */
    std::map<std::string, AgentInfo> _agents;                    /**< address to socket mapping of agents */
    std::set<std::string> _aliveAgents;                          /**< set of alive agents */
    MonitorAgentWorker *_monitor;                                /**< Agent monitor */

    pthread_barrier_t _stopRunning;                              /**< barrier to sync stop progress */
    bool _isRunning;                                             /**< whether the coordinator is running */
    time_t _lastCheckedTime;                                     /**< the last checking time of Agent liveness */

    int _startingAgentIdx;                                       /**< index of the starting container */

};

#endif // define __PROXY_COORDINATOR_HH__
