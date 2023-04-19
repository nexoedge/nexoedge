// SPDX-License-Identifier: Apache-2.0

#include <pthread.h>
#include <string>
#include <map>

#include <zmq.hpp>
#include <glog/logging.h>

#include "../../agent/coordinator.hh"
#include "../../common/config.hh"
#include "../../common/coordinator.hh"
#include "../../common/define.hh"
#include "../../common/io.hh"
#include "../../ds/coordinator_event.hh"
#include "../../proxy/coordinator.hh"

/**
 * Coordinator Test
 *
 * Test flow
 * 1. Run proxy coordinator Register
 * 2. Send correct crafted registration message to proxy coordinator
 *    - Expect sucessful registeration
 * 3. Use Agent coordinator to send message to proxy coordinator
 *
 **/

#define CID_OFFSET (100)

int main (int argc, char **argv) {
    Config &config = Config::getInstance();
    config.setConfigPath();

    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    google::InitGoogleLogging(argv[0]);

    printf("Start Coordinator Test\n");
    printf("======================\n");

    std::map<int, std::string> containers;
    zmq::context_t cxt(1);

    // run the proxy coordinator
    ProxyCoordinator *proxyc = new ProxyCoordinator(&containers);
    pthread_t pct;
    pthread_create(&pct, NULL, ProxyCoordinator::run, proxyc);

    // simulate the agent to send some messages
    CoordinatorEvent ce;
    ce.agentAddr = IO::genAddr(config.getAgentIP(), config.getAgentPort());
    ce.opcode = Opcode::REG_AGENT_REQ;
    ce.numContainers = 2;
    ce.containerIds = new int[ce.numContainers];
    ce.containerUsage = (unsigned long int *) malloc (sizeof(unsigned long int) * ce.numContainers);
    ce.containerCapacity = (unsigned long int *) malloc (sizeof(unsigned long int) * ce.numContainers);
    ce.containerType = new unsigned char[ce.numContainers];
    for (int i = 0; i < ce.numContainers; i++) {
        ce.containerIds[i] = i + CID_OFFSET;
        ce.containerUsage[i] = i;
        ce.containerCapacity[i] = 20 + i;
        ce.containerType[i] = (i % ContainerType::UNKNOWN_CONTAINER);
    }
    
    // normal register
    int myProxyNum = config.getMyProxyNum();
    std::string proxyAddr = IO::genAddr(config.getProxyIP(myProxyNum), config.getProxyCPort(myProxyNum));
    zmq::socket_t acs (cxt, ZMQ_REQ);
    acs.connect(proxyAddr);

    if (Coordinator::sendEventMessage(acs, ce) == 0) {
        printf("> Failed to send event!!\n");
        return 1;
    }
    if (Coordinator::getEventMessage(acs, ce) ==  0) {
        printf("> Failed to get event!!\n");
        return 1;
    }

    if (ce.opcode != Opcode::REG_AGENT_REP_SUCCESS) {
        printf("Failed to register agnet!!\n");
        return 1;
    } else {
        printf("Register agent successfully.\n");
    }

    // clean up socket and container list
    acs.close();
    cxt.close();
    containers.clear();
    
    // run the agent coordinator
    ContainerManager *cm = new ContainerManager();
    AgentCoordinator *agentc = new AgentCoordinator(cm);

    if (!agentc->registerToProxy(/* run background listening thread */ false)) {
        printf("Failed to use agent coordinator to register!!\n");
        return 1;
    } else {
        printf("Register agent successfully using agent coordinator.\n");
    }

    delete agentc;
    delete cm;

    delete proxyc;
    // stop and release the proxy coordinator
    pthread_join(pct, NULL);

    printf("End of Coordinator Test\n");
    printf("=======================\n");

    return 0;
}
