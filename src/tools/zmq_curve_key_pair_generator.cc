#include <zmq.h>
#include <iostream>
#include <fstream>

#define KEY_SIZE 41

const char *agentPublicKeyFilePath = "agent_pkey";
const char *agentSecretKeyFilePath = "agent_skey";
const char *proxyPublicKeyFilePath = "proxy_pkey";
const char *proxySecretKeyFilePath = "proxy_skey";
const char *outputDir = ".";

void usage(const char *name) {
    std::cout << "Usage: " << name << " [output directory path]\n";
}

bool writeKey(const char *path, char *key) {
    std::ofstream keyfile(path, std::ios::binary | std::ios::out);
    if (keyfile.fail()) {
        std::cerr << "Failed to open file [" << path << "] for write!\n";
        return false;
    }
    keyfile.write(key, KEY_SIZE);
    if (keyfile.fail()) {
        keyfile.close();
        std::cerr << "Failed to write to file [" << path << "]!\n";
        return false;
    }
    keyfile.close();
    return true;
}

bool genKeys(char *pkey, char *skey) {
    if (zmq_curve_keypair(pkey, skey) != 0) {
        std::cerr << "Failed to generate a key pair!\n";
        return false;
    };
    return true;
}

bool genAndWriteKeys(bool forAgent = true) {
    char pkey[KEY_SIZE], skey[KEY_SIZE];
    char pkeyPath[4096], skeyPath[4096];
    sprintf(pkeyPath, "%s/%s", outputDir, forAgent? agentPublicKeyFilePath: proxyPublicKeyFilePath);
    sprintf(skeyPath, "%s/%s", outputDir, forAgent? agentSecretKeyFilePath: proxySecretKeyFilePath);
    if (!genKeys(pkey, skey) || !writeKey(pkeyPath, pkey) || !writeKey(skeyPath, skey)) {
        return false;
    }
    return true;
}

int main(int argc, const char ** argv) {
    // update output directory if specified
    if (argc == 2) { outputDir = argv[1]; }
    if (argc > 2) { usage(argv[0]); return 1; }
    // generate the agent key pair
    if (!genAndWriteKeys()) {
        std::cerr << "Failed to generate a key pair for agents!\n";
    }
    if (!genAndWriteKeys(false)) {
        std::cerr << "Failed to generate a key pair for proxies!\n";
    }
    return 0;
}
