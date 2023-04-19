// SPDX-License-Identifier: Apache-2.0

#include <signal.h> // signal()

#include <glog/logging.h>

extern "C" {
#include <oss_c_sdk/aos_http_io.h>
}

#include <aws/core/Aws.h>
#include <glog/logging.h>

#include "../common/config.hh"
#include "agent.hh"

Agent *agent = 0;
Aws::SDKOptions options;

void handleTerm(int signal) {
    if (signal != SIGTERM)
        return;

    delete agent;
    agent = 0;

    // release the resources used by cloud sdks
    aos_http_io_deinitialize();
    Aws::ShutdownAPI(options);
}

int main(int argc, char **argv) {
    signal(SIGTERM, handleTerm);

    google::InstallFailureSignalHandler();
    Config &config = Config::getInstance();
    const char *envPath = std::getenv("NCLOUD_CONFIG_PATH");
    if (argc > 1)
        config.setConfigPath(std::string(argv[1]));
    else if (envPath != 0)
        config.setConfigPath(std::string(envPath));
    else
        config.setConfigPath();

    // init aws sdk
    Aws::InitAPI(options);
    // init aliyun sdk
    if (aos_http_io_initialize(NULL, 0) != AOSE_OK) {
        LOG(ERROR) << "Failed to init Aliyun OSS interface";
        return 1;
    }

    // init log output
    google::InitGoogleLogging(argv[0]);
    if (!config.glogToConsole()) {
        FLAGS_log_dir = config.getGlogDir().c_str();
    } else {
        FLAGS_logtostderr = true;
    }
    FLAGS_minloglevel = config.getLogLevel();
    FLAGS_logbuflevel = -1;

    // new an agent
    agent = new Agent();

    // run the agent
    agent->run(config.getAgentRegisterToProxy());

    return 0;
}
