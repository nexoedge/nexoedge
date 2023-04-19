// SPDX-License-Identifier: Apache-2.0

#include <stdexcept>
#include <unistd.h>
#include <linux/limits.h>

#include <glog/logging.h>

#include "container_manager.hh"
#include "container/all.hh"
#include "../common/config.hh"
#include "../common/coding/coding_util.hh"

ContainerManager::ContainerManager() {
    Config &config = Config::getInstance();
    _numContainers = config.getNumContainers();
    // init containers
    // support multiple types of containers
    for (int i = 0; i < _numContainers; i++) {
        unsigned short ctype = config.getContainerType(i);
        std::string cstr = config.getContainerPath(i);
        int cid = config.getContainerId(i);
        unsigned long int capacity = config.getContainerCapacity(i);
        std::string key = config.getContainerKey(i);
        std::string keyId = config.getContainerKeyId(i);
        std::string region = config.getContainerRegion(i);
        std::string proxyIP = config.getContainerHttpProxyIP(i);
        unsigned short  proxyPort = config.getContainerHttpProxyPort(i);
        switch (ctype) {
        case ContainerType::FS_CONTAINER:
            _containerPtrs[i] = new FsContainer(cid, cstr.c_str(), capacity);
            DLOG(INFO) << "FS container with id = " << cid << " folder name = " << cstr << " capacity = " << capacity;
            break;
        case ContainerType::AWS_CONTAINER:
            _containerPtrs[i] = new AwsContainer(cid, cstr, region, keyId, key, capacity, "", proxyIP, proxyPort);
            DLOG(INFO) << "AWS container with id = " << cid << " bucket name = " << cstr << " capacity = " << capacity;
            break;
        case ContainerType::ALI_CONTAINER:
            _containerPtrs[i] = new AliContainer(cid, cstr, region, keyId, key, capacity);
            DLOG(INFO) << "Aliyun container with id = " << cid << " bucket name = " << cstr << " capacity = " << capacity;
            break;
        case ContainerType::AZURE_CONTAINER:
            _containerPtrs[i] = new AzureContainer(cid, cstr, key, capacity, proxyIP, proxyPort);
            DLOG(INFO) << "Azure container with id = " << cid << " capacity = " << capacity;
            break;
        default:
            LOG(ERROR) << "Container type " << ctype << " not supported! (container no. = " << i << ")";
            exit(1);
            break;
        }
        if (_containers.insert(std::pair<int, Container*>(cid, _containerPtrs[i])).second == false) {
            LOG(ERROR) << "Found container with duplicated id = " << cid;
            exit(1);
        }
    }
}

ContainerManager::~ContainerManager() {
    LOG(WARNING) << "Terminating Container Manager ...";
    // release the containers
    for (int i = 0; i < _numContainers; i++)
        delete _containerPtrs[i];
    LOG(WARNING) << "Terminated Container Manager ...";
}

bool ContainerManager::putChunks(int containerId[], Chunk chunks[], int numChunks) {
    bool ret = true;
    int i = 0;

    bool verifyChecksum = Config::getInstance().verifyChunkChecksum();

    // store chunks to containers
    for (i = 0; i < numChunks && ret; i++) {
        try {
            // verify checksum before write
            if (verifyChecksum && !chunks[i].verifyMD5()) {
                ret = false;
                break;
            }
            // write chunk
            if ((ret = _containers.at(containerId[i])->putChunk(chunks[i])) == false) {
                ret = false;
                break;
            }
            _containers.at(containerId[i])->bgUpdateUsage();
        } catch (std::exception &e) {
            LOG(ERROR) << "Cannot find container " << containerId[i] << " to write chunk";
            ret = false;
            break;
        }
    }
    // remove stored chunks once failed
    for (int j = 0; j < i && !ret; j++) { 
        try {
            _containers.at(containerId[j])->deleteChunk(chunks[i]);
            _containers.at(containerId[i])->bgUpdateUsage();
        } catch (std::exception &e) {
            LOG(ERROR) << "Cannot find container " << containerId[j] << " to remove chunk after write failure";
        }
    }
    return ret;
}

bool ContainerManager::getChunks(int containerId[], Chunk chunks[], int numChunks) {
    bool ret = true;
    // get chunks from containers
    for (int i = 0; i < numChunks; i++ ) {
        try {
            if ((ret = _containers.at(containerId[i])->getChunk(chunks[i])) == false) {
                throw std::invalid_argument("");
            }
        } catch (std::exception &e) {
            ret = false;
            break;
        }
    }
    return ret;
}

bool ContainerManager::deleteChunks(int containerId[], Chunk chunks[], int numChunks) {
    // delete chunks from containers
    for (int i = 0; i < numChunks; i++ ) {
        try {
            _containers.at(containerId[i])->deleteChunk(chunks[i]);
            _containers.at(containerId[i])->bgUpdateUsage();
        } catch (std::exception &e) {
            LOG(ERROR) << "Cannot find container " << containerId[i] << " to remove chunk";
        }
    }
    return true;
}

bool ContainerManager::copyChunks(int containerId[], Chunk srcChunks[], Chunk dstChunks[], int numChunks) {
    // copy chunks within containers
    bool ret = true;
    for (int i = 0; i < numChunks; i++) {
        try {
            ret = _containers.at(containerId[i])->copyChunk(srcChunks[i], dstChunks[i]) && ret;
            _containers.at(containerId[i])->bgUpdateUsage();
        } catch (std::exception &e) {
            ret = false;
            // remove already copied chunks upon error
            for (int j = 0; j < i; j++)
                try {
                    _containers.at(containerId[j])->deleteChunk(dstChunks[j]);
                } catch (std::exception &e) {
                    LOG(ERROR) << "Cannot find container " << containerId[j] << " to remove chunk after copy failure";
                }
            break;
        } 
    }
    return ret;
}

bool ContainerManager::moveChunks(int containerId[], Chunk srcChunks[], Chunk dstChunks[], int numChunks) {
    bool ret = true;
    for (int i = 0; i < numChunks; i++) {
        try {
            ret = _containers.at(containerId[i])->moveChunk(srcChunks[i], dstChunks[i]) && ret;
        } catch (std::exception &e) {
            ret = false;
            // revert already moved chunks upon error
            for (int j = 0; j < i; j++)
                try {
                    _containers.at(containerId[j])->moveChunk(dstChunks[j], srcChunks[j]);
                } catch (std::exception &e) {
                    LOG(ERROR) << "Cannot find container " << containerId[j] << " to reverse chunk moving after move failure";
                }
            break;
        } 
    }
    return ret;
}

bool ContainerManager::hasChunks(int containerId[], Chunk chunks[], int numChunks) {
    bool ret = true;
    for (int i = 0; ret && i < numChunks; i++) {
        try {
            ret = _containers.at(containerId[i])->hasChunk(chunks[i]) && ret;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to find container " << containerId[i] << " to check chunk";
        }
    }
    return ret;
}

int ContainerManager::verifyChunks(int containerId[], Chunk chunks[], int numChunks) {
    int numCorruptedChunks = 0;
    for (int i = 0; i < numChunks; i++) {
        try {
            if (!_containers.at(containerId[i])->verifyChunk(chunks[i])) {
                // replace the info of corrupted chunk in place (so the chunk list is reused)
                if (i != numCorruptedChunks) {
                    chunks[numCorruptedChunks].release();
                    chunks[numCorruptedChunks] = chunks[i];
                    // avoid potential double free
                    chunks[i].release();
                }
                numCorruptedChunks += 1;
            }
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to find container " << containerId[i] << " to check chunk";
            return -1;
        }
    }
    return numCorruptedChunks;
}

bool ContainerManager::revertChunks(int containerId[], Chunk chunks[], int numChunks) {
    bool ret = true;
    for (int i = 0; ret && i < numChunks; i++) {
        try {
            ret = _containers.at(containerId[i])->revertChunk(chunks[i]) && ret;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to find container " << containerId[i] << " to revert chunk";
        }
    }
    return ret;
}

Chunk ContainerManager::getEncodedChunks(int containerId[], Chunk chunks[], int numChunks, unsigned char matrix[]) {
    Chunk codedChunk, rawChunks[numChunks];
    unsigned char *rawData[numChunks];
    bool ret = false;
    for (int i = 0; i < numChunks; i++) {
        rawChunks[i].setId(chunks[i].getNamespaceId(), chunks[i].getFileUUID(), chunks[i].getChunkId());
        rawChunks[i].fileVersion = chunks[i].fileVersion;
        // get the chunk
        try {
            if ((ret = _containers.at(containerId[i])->getChunk(rawChunks[i], true)) == false) {
                LOG(ERROR) << "Failed to get chunk id = " << chunks[i].getChunkName() << " from container " << containerId[i];
                throw std::invalid_argument("");
            }
            rawData[i] = rawChunks[i].data;
        } catch (std::exception &e) {
            LOG(ERROR) << "Failed to find container / chunk for chunk id = (" << (unsigned int) chunks[i].getNamespaceId() << "," << chunks[i].getFileUUID() << "," << (int) chunks[i].getChunkId() << ") from container " << containerId[i];
            ret = false;
            break;
        }
        if (codedChunk.data == NULL) {
            codedChunk.data = (unsigned char *) malloc (rawChunks[i].size);
            if (codedChunk.data == NULL) {
                LOG(ERROR) << "Failed to allocate memory for data of the encoded chunk";
                return codedChunk;
            }
            codedChunk.size = rawChunks[i].size;
        }
    }
    if (ret) {
        // encode the chunk
        CodingUtils::encode(rawData, numChunks, &codedChunk.data, 1, codedChunk.size, matrix);
        codedChunk.freeData = false;
    } else {
        codedChunk.size = 0;
        free(codedChunk.data);
    }
    return codedChunk;
}

int ContainerManager::getNumContainers() {
    return _numContainers;
}

void ContainerManager::getContainerIds(int containerIds[]) {
    for (int i = 0; i < _numContainers; i++)
        containerIds[i] = _containerPtrs[i]->getId();
}

void ContainerManager::getContainerType(unsigned char containerType[]) {
    Config &config = Config::getInstance();
    for (int i = 0; i < _numContainers; i++)
        containerType[i] = config.getContainerType(i);
}

void ContainerManager::getContainerUsage(unsigned long int containerUsage[], unsigned long int containerCapacity[]) {
    for (int i = 0; i < _numContainers; i++) {
        containerUsage[i] = _containerPtrs[i]->getUsage();
        containerCapacity[i] = _containerPtrs[i]->getCapacity();
        _containerPtrs[i]->bgUpdateUsage();
    }
}
