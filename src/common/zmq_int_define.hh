// SPDX-License-Identifier: Apache-2.0

#ifndef __ZMQ_INT_DEFINE_H__
#define __ZMQ_INT_DEFINE_H__

//#define ZMQ_IPC_ENDPOINT "ipc:///tmp/ncloud_zmq_local"
//#define ZMQ_IPC_ENDPOINT "tcp://127.0.0.1:59001"

enum ClientOpcode {
    // file write
    WRITE_FILE_REQ,
    WRITE_FILE_REP_SUCCESS,
    WRITE_FILE_REP_FAIL,

    // file read
    READ_FILE_REQ,
    READ_FILE_REP_SUCCESS,
    READ_FILE_REP_FAIL,

    // file append
    APPEND_FILE_REQ,
    APPEND_FILE_REP_SUCCESS,
    APPEND_FILE_REP_FAIL,
    GET_APPEND_SIZE_REQ,
    GET_APPEND_SIZE_REP_SUCCESS,
    GET_APPEND_SIZE_REP_FAIL,

    // file delete
    DEL_FILE_REQ,
    DEL_FILE_REP_SUCCESS,
    DEL_FILE_REP_FAIL,

    // file partial read
    READ_FILE_RANGE_REQ,
    READ_FILE_RANGE_REP_SUCCESS,
    READ_FILE_RANGE_REP_FAIL,
    GET_READ_SIZE_REQ,
    GET_READ_SIZE_REP_SUCCESS,
    GET_READ_SIZE_REP_FAIL,

    // file rename
    RENAME_FILE_REQ,
    RENAME_FILE_REP_SUCCESS,
    RENAME_FILE_REP_FAIL,

    // normalized storage capacity
    GET_CAPACITY_REQ,
    GET_CAPACITY_REP_SUCCESS,
    GET_CAPACITY_REP_FAIL,

    // list files
    GET_FILE_LIST_REQ,
    GET_FILE_LIST_REP_SUCCESS,
    GET_FILE_LIST_REP_FAIL,

    // agent status
    GET_AGENT_STATUS_REQ,
    GET_AGENT_STATUS_REP_SUCCESS,
    GET_AGENT_STATUS_REP_FAIL,

    // file overwrite
    OVERWRITE_FILE_REQ,
    OVERWRITE_FILE_REP_SUCCESS,
    OVERWRITE_FILE_REP_FAIL,

    // background task progress
    GET_BG_TASK_PRG_REQ,
    GET_BG_TASK_PRG_REP_SUCCESS,
    GET_BG_TASK_PRG_REP_FAIL,

    // server-side file copying
    COPY_FILE_REQ,
    COPY_FILE_REP_SUCCESS,
    COPY_FILE_REP_FAIL,

    // repair stats
    GET_REPAIR_STATS_REQ,
    GET_REPAIR_STATS_REP_SUCCESS,
    GET_REPAIR_STATS_REP_FAIL,

    // proxy status
    GET_PROXY_STATUS_REQ,
    GET_PROXY_STATUS_REP_SUCCESS,
    GET_PROXY_STATUS_REP_FAIL,

    UNKNOWN_CLIENT_OP,
};

#endif //define __ZMQ_INT_DEFINE_H__
