// SPDX-License-Identifier: Apache-2.0

#ifndef __DEFINE_HH__
#define __DEFINE_HH__

#include <stdint.h>

// internal variable types
typedef uint32_t                   length_t;
typedef uint64_t                   offset_t;
typedef unsigned char              data_t;

typedef uint8_t                    namespace_id_t;
typedef uint16_t                   chunk_id_t;
typedef uint32_t                   version_id_t;

typedef uint8_t                    coding_param_t;

typedef uint32_t                   num_t;

// constants
/// chunk
#define INVALID_CHUNK_ID           (int)(-1)
/// container
#define INVALID_CONTAINER_ID       (int)(-1)
#define UNUSED_CONTAINER_ID        (int)(-2)
/// file
#define INVALID_NAMESPACE_ID       (namespace_id_t)(-1)
#define INVALID_FILE_OFFSET        (offset_t)(-1)
#define INVALID_FILE_LENGTH        INVALID_FILE_OFFSET
#define DEFAULT_NAMESPACE_ID       Config::getInstance().getProxyNamespaceId()
#define CHUNK_VERSION_MAX_LEN      (unsigned char)(128)
/// TCP/IP address
#define INVALID_IP                 "0.0.0.0"
#define INVALID_PORT               (1 << 16) // 65536
// limits
#define MAX_NUM_CONTAINERS         (int)(100)
#define MAX_NUM_AGENTS             (int)(100)
#define MAX_NUM_PROXY              (int)(100)
#define MAX_NUM_WORKERS            (int)(256)
#define MAX_NUM_NEAR_IP_RANGES     (16)

#define HOUR_IN_SECONDS            (3600)
//#define HOUR_IN_SECONDS            (30) // for code testing

// see also CodingSchemeName in common/config.cc
enum CodingScheme {
    RS,
    UNKNOWN_CODE
};

enum Opcode {
    // chunk request
    PUT_CHUNK_REQ,          // 0
    GET_CHUNK_REQ,
    DEL_CHUNK_REQ,
    CPY_CHUNK_REQ,
    ENC_CHUNK_REQ,

    // chunk reply
    PUT_CHUNK_REP_SUCCESS,  // 5
    GET_CHUNK_REP_SUCCESS,
    DEL_CHUNK_REP_SUCCESS,
    CPY_CHUNK_REP_SUCCESS,
    ENC_CHUNK_REP_SUCCESS,
    PUT_CHUNK_REP_FAIL,     // 10
    GET_CHUNK_REP_FAIL,
    DEL_CHUNK_REP_FAIL,
    CPY_CHUNK_REP_FAIL,
    ENC_CHUNK_REP_FAIL,

    // agent register
    REG_AGENT_REQ,          // 15
    REG_AGENT_REP_SUCCESS,
    REG_AGENT_REP_FAIL,
    UPD_AGENT_REQ,
    UPD_AGENT_REP,

    // coordinator events
    SYN_PING,               // 20
    ACK_PING,

    // proxy instructs agent to send peer request
    RPR_CHUNK_REQ,
    RPR_CHUNK_REP_SUCCESS,
    RPR_CHUNK_REP_FAIL,

    // chunk existance check
    CHK_CHUNK_REQ,          // 25
    CHK_CHUNK_REP_SUCCESS,
    CHK_CHUNK_REP_FAIL,

    // move chunks
    MOV_CHUNK_REQ,
    MOV_CHUNK_REP_SUCCESS,
    MOV_CHUNK_REP_FAIL,    // 30

    // revert chunks
    RVT_CHUNK_REQ,
    RVT_CHUNK_REP_SUCCESS,
    RVT_CHUNK_REP_FAIL,

    // get agent system info
    GET_SYSINFO_REQ,
    GET_SYSINFO_REP,      // 35

    // Verify chunk checksum
    VRF_CHUNK_REQ,
    VRF_CHUNK_REP_SUCCESS,
    VRF_CHUNK_REP_FAIL,

    UNKNOWN_OP,
};

// see also ContainerTypeName in common/config.cc
enum ContainerType {
    FS_CONTAINER,          // 0
    ALI_CONTAINER,
    AWS_CONTAINER,
    AZURE_CONTAINER,

    UNKNOWN_CONTAINER,
};

// see also DistributionPolicyName in common/config.cc
enum DistributionPolicy {
    STATIC,
    RR, // round-robin
    LU, // least-used

    UNKNOWN_DIST_POLICY,
};

enum FileStatus {
    NONE,
    BG_TASK_PENDING,
    PART_BG_TASK_COMPLETED,
    ALL_BG_TASKS_COMPLETED
};

enum ChunkScanSamplingPolicy {
    NONE_SAMPLING_POLICY,
    CHUNK_LEVEL,
    STRIPE_LEVEL,
    FILE_LEVEL,
    CONTAINER_LEVEL,

    UNKNOWN_SAMPLING_POLICY
};

// note this has the same order as ContainerType
enum HostType {
    HOST_TYPE_ON_PREM,   // 0
    HOST_TYPE_ALI,
    HOST_TYPE_AWS,
    HOST_TYPE_AZURE,
    HOST_TYPE_TENCENT,
    HOST_TYPE_GCP,      // 5
    HOST_TYPE_HUAWEI,

    HOST_TYPE_UNKNOWN
};

enum MetaStoreType {
    REDIS,

    UNKNOWN_METASTORE
};

extern const char *CodingSchemeName[];
extern const char EmptyStringMD5[];

#endif // define __DEFINE_HH__
