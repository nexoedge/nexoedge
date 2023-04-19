// SPDX-License-Identifier: Apache-2.0

#include <stdio.h> // printf()
#include <signal.h> // signal()

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include "../common/config.hh"
#include "interfaces/zmq.hh"
#include "dedup/impl/dedup_all.hh"

Proxy *proxy = 0;
ProxyZMQIntegration *proxy_zmq = 0;
ProxyCoordinator *coordinator = 0;
pthread_t ct;

bool terminating = false;

void handleManualAgentReconnect(int signal) {
    if (signal != SIGUSR1)
        return;

    if (coordinator) {
        printf("Signal receviced, recoonecting agents\n");
        coordinator->registerPresetAgents();
    }
}

void handleTerm(int signal) {
    if (signal != SIGTERM)
        return;

    if (terminating) { return; }
    terminating = true;

    delete proxy_zmq;
    delete proxy;
    delete coordinator;

    proxy_zmq = 0;
    proxy = 0;
    coordinator = 0;

    pthread_join(ct, 0);

    printf("Terminated Proxy\n");
}

int main(int argc, char **argv) {

    // unmask the main thread from SIGTERM
    signal(SIGTERM, handleTerm);
    signal(SIGUSR1, handleManualAgentReconnect);

    // init stack printing on signal
    //google::InstallFailureSignalHandler();

    // system env path
    const char *evnPath = std::getenv("NCLOUD_CONFIG_PATH");
    
    Config &config = Config::getInstance();
    // custom config file path
    if (argc > 1) {
        config.setConfigPath(std::string(argv[1]));
        printf("Search config files under input path  = %s\n", argv[1]);
    } else if (evnPath != 0) {
        config.setConfigPath(std::string(evnPath));
        printf("Search config files under env path  = %s\n", argv[1]);
    } else {
        config.setConfigPath();
    }

    // setup logging
    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
        printf("Output log to %s\n", config.getGlogDir().c_str());
    } else {
        FLAGS_logtostderr = true;
        printf("Output log to console\n");
    }
    FLAGS_minloglevel = config.getLogLevel();
    FLAGS_logbuflevel = -1;
    google::InitGoogleLogging(argv[0]);

    config.printConfig();

    // create proxy and interface
    std::map<int, std::string> map; // container to agent map
    coordinator = new ProxyCoordinator(&map); // proxy coordinator
    BgChunkHandler::TaskQueue queue; // background chunk task queue
    pthread_create(&ct, NULL, ProxyCoordinator::run, coordinator); // proxy coordinator thread

    DeduplicationModule *dedup = new DedupNone();

    // always open the zmq interface (for monitoring), and optional interfaces for request processing
    std::string interfaces = config.getProxyInterface();
    proxy = new Proxy(coordinator, &map, &queue, dedup);
    proxy_zmq = new ProxyZMQIntegration(proxy);
    
    // wait for enough agents and containers...
    //printf(">>> Please start at least %d containers now, and press enter when it is ready for requests... <<<\n", Config::getInstance().getN());
    //getchar();

    //printf(">>> Start polling for request <<<\n");
    // start polling for requests
    void *ptr = 0;
    pthread_t zmq_itf;
    bool zmq_running = false;
    if (proxy_zmq != 0) {
        zmq_running = pthread_create(&zmq_itf, 0, ProxyZMQIntegration::run, proxy_zmq) == 0;
    }
        
    // clean up for termination
    if (zmq_running != 0) {
        pthread_join(zmq_itf, 0);
        printf("Terminated Proxy (zmq)\n");
    }

    return ptr == 0;
}
